package raft

import (
	"../labgob"
	"../labrpc"
	"bytes"
	"sync"
	"sync/atomic"
	"time"
)

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

type State int

const (
	Init State = iota
	Follower
	Candidate
	Leader
)

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
	Snapshot     []byte
}

//
// log entry
//
type LogEntry struct {
	Term    int
	Command interface{}
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	state   State
	applyCh chan ApplyMsg

	// persistent state, all servers
	currentTerm int
	votedFor    int
	log         []LogEntry

	// volatile state, all servers
	commitIndex int
	lastApplied int

	// volatile state, leaders
	// re-init after election
	nextIndex  []int
	matchIndex []int

	// temp state for election timer
	lastHeartbeat time.Time

	// for log compaction
	lastSnapshotIndex int
	lastSnapshotTerm  int

	// apply signal
	applySignal *sync.Cond
}

//
// returns the term of the last item in log
//
func (rf *Raft) lastLogTerm() int {
	// expects lock to be held
	return rf.log[len(rf.log)-1].Term
}

//
// returns the index of the last item in log
//
func (rf *Raft) lastLogIndex() int {
	// expects lock to be held
	return rf.lastSnapshotIndex + len(rf.log) - 1
}

//
// returns the relative index of absoluteIndex in log,
// relativeIndex = absoluteIndex - lastSnapshotIndex
//
func (rf *Raft) relativeIndex(absoluteIndex int) int {
	relative := absoluteIndex - rf.lastSnapshotIndex
	//rf.logDebug("absolute %v = relative %v", absoluteIndex, relative)
	return relative
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.state == Leader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)

	// expects lock to be held
	data := rf.encodePersistentStates()
	rf.persister.SaveRaftState(*data)
}

func (rf *Raft) encodePersistentStates() *[]byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	_ = e.Encode(rf.currentTerm)
	_ = e.Encode(rf.votedFor)
	_ = e.Encode(rf.log)
	_ = e.Encode(rf.lastSnapshotIndex)
	_ = e.Encode(rf.lastSnapshotTerm)
	data := w.Bytes()
	return &data
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var votedFor int
	var log []LogEntry
	var lastSnapshotIndex int
	var lastSnapshotTerm int
	rf.mu.Lock()
	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&log) != nil ||
		d.Decode(&lastSnapshotIndex) != nil ||
		d.Decode(&lastSnapshotTerm) != nil {
		rf.logFatal("failed to read persisted state")
	} else {
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.log = log
		rf.lastSnapshotIndex = lastSnapshotIndex
		rf.lastSnapshotTerm = lastSnapshotTerm
	}
	rf.logInfo("reloaded state: currentTerm=%v, votedFor=%v, log=%v",
		rf.currentTerm, rf.votedFor, rf.logString())
	rf.mu.Unlock()
}

func (rf *Raft) changeState(newState State) {
	// expects lock to be held
	rf.logDebug("%v -> %v", stateName(rf.state), stateName(newState))

	switch newState {
	case Follower:
		if rf.state != newState {
			go rf.electionTimer()
		}
		rf.state = newState
	case Candidate:
		// set state in beginElection instead
		// begin election
		go rf.beginElection()
	case Leader:
		// set up heartbeat loop
		if rf.state != newState {
			rf.initLeaderStates()
			go rf.sendAppendEntriesTimer()
		}
		rf.state = newState
	default:
		rf.logFatal("unexpected new state type: %v", stateName(newState))
	}
}

func (rf *Raft) applyLoop() {
	for !rf.killed() {
		rf.mu.Lock()
		rf.applySignal.Wait()
		rf.mu.Unlock()
		rf.applyEntries()
	}
}

func (rf *Raft) applyEntries() {
	rf.mu.Lock()
	if rf.lastSnapshotIndex > rf.lastApplied {
		rf.logDebug("sending snapshot to app")
		applyMsg := ApplyMsg{
			CommandValid: false,
			Command:      "InstallSnapshot",
			CommandIndex: rf.lastSnapshotIndex,
			Snapshot:     rf.persister.ReadSnapshot(),
		}
		rf.lastApplied = rf.lastSnapshotIndex
		rf.logDebug("advanced lastApplied to %v", rf.lastApplied)
		rf.mu.Unlock()
		rf.applyCh <- applyMsg
		return
	}

	var applyQueue []ApplyMsg
	oldLastApplied := rf.lastApplied
	for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
		applyMsg := ApplyMsg{
			CommandValid: true,
			Command:      rf.log[rf.relativeIndex(i)].Command,
			CommandIndex: i,
		}
		applyQueue = append(applyQueue, applyMsg)
	}
	if len(applyQueue) > 0 {
		rf.logDebug("ready to apply index %v to %v", rf.lastApplied+1, rf.lastApplied+len(applyQueue))
	}
	rf.mu.Unlock()
	for _, msg := range applyQueue {
		rf.mu.Lock()
		rf.logDebug("about to apply index %v", msg.CommandIndex)
		rf.mu.Unlock()
		rf.applyCh <- msg
		rf.mu.Lock()
		rf.logDebug("applied index %v", msg.CommandIndex)
		rf.lastApplied = msg.CommandIndex
		rf.mu.Unlock()
	}
	rf.mu.Lock()
	if rf.lastApplied > oldLastApplied {
		rf.logDebug("applied entries %v to %v", oldLastApplied+1, rf.lastApplied)
	}
	rf.mu.Unlock()
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	// Your code here (2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	index := rf.lastLogIndex() + 1
	term := rf.currentTerm
	isLeader := rf.state == Leader

	if isLeader {
		newEntry := LogEntry{
			Term:    term,
			Command: command,
		}
		rf.logDebug("adding new entry to log at index %v: %+v", index, newEntry)
		rf.log = append(rf.log, newEntry)
		rf.logDebug("updated log: %v", rf.logString())

		rf.persist()
		rf.broadcastAppendEntries()
		oldMatchIndex := rf.matchIndex[rf.me]
		oldNextIndex := rf.nextIndex[rf.me]
		rf.matchIndex[rf.me] = index
		rf.nextIndex[rf.me]++
		rf.logVerbose("matchIndex[%v] %v -> %v", rf.me, oldMatchIndex, rf.matchIndex[rf.me])
		rf.logVerbose("nextIndex[%v] %v -> %v", rf.me, oldNextIndex, rf.nextIndex[rf.me])
		rf.advanceLeaderCommitIndex()
	}
	return index, term, isLeader
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.state = Init
	rf.applyCh = applyCh
	rf.applySignal = sync.NewCond(&rf.mu)

	// persistent
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.log = make([]LogEntry, 1)
	rf.lastSnapshotIndex = 0

	// volatile
	rf.commitIndex = 0
	rf.lastApplied = 0

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// init
	go rf.applyLoop()
	rf.mu.Lock()
	rf.changeState(Follower)
	rf.mu.Unlock()

	return rf
}
