package raft

import (
	"../labrpc"
	"math/rand"
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

// import "bytes"
// import "../labgob"

//region Constants

const (
	HeartbeatTimeout     = 150 * time.Millisecond
	ElectionTimeoutMsMin = 300
	ElectionTimeoutMsMax = 600
)

type State int

const (
	Init State = iota
	Follower
	Candidate
	Leader
)

//endregion

//region Raft structs

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
	state State

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
}

//endregion

//region State management

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
}

//endregion

//region RPC-related

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.logDebug("receives RequestVote for term %v from s%v", args.Term, args.CandidateId)
	if args.Term < rf.currentTerm {
		// term passed, vote no
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		rf.logDebug("votes no to s%v", args.CandidateId)
		return
	}

	// update term if we receive a higher RequestVote
	if args.Term > rf.currentTerm {
		rf.logDebug("updates term to %v, converting to follower", args.Term)
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.changeState(Follower)
	}

	// if we haven't voted yet, or have voted for the candidate,
	// then vote yes
	if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
		// FIXME: check lastLogIndex before voting
		reply.VoteGranted = true
		reply.Term = rf.currentTerm
		rf.votedFor = args.CandidateId
		rf.logDebug("votes YES to s%v", args.CandidateId)
	} else {
		rf.logDebug("votes NO to s%v, already voted to s%v", args.CandidateId, rf.votedFor)
	}
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

type AppendEntriesArgs struct {
	Term     int
	LeaderId int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.currentTerm
	rf.logDebug("receives AppendEntries with term %v from s%v", args.Term, args.LeaderId)
	if args.Term >= rf.currentTerm {
		rf.lastHeartbeat = time.Now()
		if rf.state == Candidate {
			// recognize the leader as legitimate, return to follower state
			rf.logDebug("receives AppendEntries from s%v, converting to follower for term %v",
				args.LeaderId, args.Term)
			rf.currentTerm = args.Term
			rf.changeState(Follower)
		}
	} else {
		// reject the RPC and continues in candidate state
		rf.logDebug("rejects expired AppendEntries from s%v", args.LeaderId)
	}
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

//endregion

//region State transition

func (rf *Raft) changeState(newState State) {
	if rf.state == newState {
		return
	}
	rf.logDebug("%v -> %v", stateName(rf.state), stateName(newState))
	rf.state = newState

	switch newState {
	case Follower:
		rf.logDebug("starting election timer")
		go rf.electionTimer()
	case Candidate:
		// begin election
		rf.logDebug("starting election")
		rf.beginElection()
	case Leader:
		// set up heartbeat loop
		rf.logDebug("starting heartbeat timer")
		go rf.heartbeatTimer()
		// TODO: re-init volatile states for leader
	default:
		rf.logFatal("unexpected new state type: %v", stateName(newState))
	}
}

func (rf *Raft) beginElection() {
	rf.mu.Lock()
	rf.currentTerm++
	rf.votedFor = rf.me

	voteCh := make(chan bool, len(rf.peers)-1)

	args := RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: 0,
		LastLogTerm:  0,
	}
	for server := range rf.peers {
		if server == rf.me {
			continue
		}
		go func(server int) {
			reply := RequestVoteReply{}
			rf.logDebug("sending RequestVote to %v", server)
			ok := rf.sendRequestVote(server, &args, &reply)
			if !ok {
				rf.logInfo("couldn't send RequestVote to %v", server)
				voteCh <- false
				return
			}
			if reply.Term > rf.currentTerm {
				// someone has higher term than us, abort and become follower
				rf.currentTerm = reply.Term
				rf.changeState(Follower)
				return
			}
			voteCh <- reply.VoteGranted
		}(server)
	}
	rf.mu.Unlock()

	// always votes for myself
	voteCount := 1
	grantCount := 1
	for {
		// TODO: exit as fail if waiting for too long
		grant := <-voteCh
		voteCount++
		if grant {
			grantCount++
		}
		// when all peers have voted, or grant/deny votes have reached 1/2, stop waiting for votes
		if voteCount == len(rf.peers) ||
			grantCount*2 > len(rf.peers) ||
			(voteCount-grantCount)*2 > len(rf.peers) {
			break
		}
	}

	rf.mu.Lock()
	if grantCount*2 > len(rf.peers) {
		rf.logInfo("elected as leader for term %v", rf.currentTerm)
		rf.changeState(Leader)
	} else {
		rf.logInfo("failed election")
		rf.changeState(Follower)
	}
	rf.mu.Unlock()
}

// Timers

func (rf *Raft) heartbeatTimer() {
	for {
		rf.mu.Lock()
		if rf.state != Leader {
			// stop sending heartbeats when no longer leader
			rf.mu.Unlock()
			return
		}
		if rf.state == Leader {
			rf.logDebug("heartbeat")
			for server := range rf.peers {
				if server == rf.me {
					continue
				}
				go func(server int) {
					args := AppendEntriesArgs{
						Term:     rf.currentTerm,
						LeaderId: rf.me,
					}
					reply := AppendEntriesReply{}
					rf.sendAppendEntries(server, &args, &reply)
				}(server)
			}
		}
		rf.mu.Unlock()
		time.Sleep(HeartbeatTimeout)
	}
}

func (rf *Raft) electionTimer() {
	for {
		sleepTimeInMs := rand.Intn(ElectionTimeoutMsMax-ElectionTimeoutMsMin) + ElectionTimeoutMsMin
		timeoutDuration := time.Duration(sleepTimeInMs) * time.Millisecond
		time.Sleep(timeoutDuration)
		rf.mu.Lock()
		// if no longer follower, cancel timer
		if rf.state != Follower {
			rf.logDebug("no longer follower, canceling election timer")
			rf.mu.Unlock()
			return
		}
		// if no heartbeat received, stop looping, begin election
		if time.Now().Sub(rf.lastHeartbeat) > timeoutDuration {
			rf.logDebug("election timer timeout")
			rf.mu.Unlock()
			break
		}
		rf.mu.Unlock()
	}
	rf.changeState(Candidate)
}

//endregion

//region Lifecycle

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
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).

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
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.state = Init
	rf.mu.Lock()
	rf.changeState(Follower)
	rf.mu.Unlock()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}

//endregion
