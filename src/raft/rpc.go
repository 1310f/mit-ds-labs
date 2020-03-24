package raft

import "time"

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
	//rf.logDebug("receives RequestVote for term %v from s%v: %+v", args.Term, args.CandidateId, args)

	// 1. reply false if term < currentTerm
	if args.Term < rf.currentTerm {
		// term passed, vote no
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		//rf.logDebug("votes NO to s%v, RequestVote from term %v expired", args.CandidateId, args.Term)
		return
	}

	// update term if we receive a higher RequestVote
	if args.Term > rf.currentTerm {
		//rf.logDebug("updates term to %v, converting to follower", args.Term)
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.persist()
		rf.changeState(Follower)
	}
	reply.Term = rf.currentTerm

	// we now have rf.currentTerm == args.Term

	// 2. if votedFor is null or candidateId
	if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
		// ...and candidate's log is at least as up-to-date as receiver's log, grant vote
		if args.LastLogTerm > rf.lastLogTerm() ||
			(args.LastLogTerm == rf.lastLogTerm() && args.LastLogIndex >= rf.lastLogIndex()) {
			reply.VoteGranted = true
			rf.votedFor = args.CandidateId
			//rf.logDebug("votes YES to s%v", args.CandidateId)
			// reset heartbeat timer if voted YES
			rf.lastHeartbeat = time.Now()
		} else {
			reply.VoteGranted = false
			//rf.logDebug("votes NO to s%v due to non up-to-date log", args.CandidateId)
		}
	} else {
		reply.VoteGranted = false
		//rf.logDebug("votes NO to s%v, already voted to s%v", args.CandidateId, rf.votedFor)
	}
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
	// from extended Raft:
	// If desired, the protocol can be optimized to reduce the
	// number of rejected AppendEntries RPCs. For example,
	// when rejecting an AppendEntries request, the follower
	// can include the term of the conflicting entry and the first
	// index it stores for that term. With this information, the
	// leader can decrement nextIndex to bypass all of the conflicting
	// entries in that term; one AppendEntries RPC will
	// be required for each term with conflicting entries, rather
	// than one RPC per entry. In practice, we doubt this optimization
	// is necessary, since failures happen infrequently
	// and it is unlikely that there will be many inconsistent entries.
	ConflictTerm  int
	ConflictIndex int
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.currentTerm
	rf.logDebug("receives AppendEntries with term %v from s%v: "+
		"Entries: %v, prevLogIndex: %v, prevLogTerm: %v, LeaderCommit: %v",
		args.Term, args.LeaderId, shortLog(args.Entries, false),
		args.PrevLogIndex, args.PrevLogTerm, args.LeaderCommit)

	// 1. reply false if term < currentTerm
	if args.Term < rf.currentTerm {
		//rf.logDebug("rejects expired AppendEntries from s%v", args.LeaderId)
		reply.Success = false
		return
	}

	// reset heartbeat timer
	rf.lastHeartbeat = time.Now()

	// 2. reply false if log doesn't contain an entry at prevLogIndex whose term matches prevLogTerm
	if len(rf.log) <= args.PrevLogIndex || rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		rf.logDebug("prev log term mismatch at index %v, rejecting AppendEntries", args.PrevLogIndex)
		reply.Success = false
		// from Students' Guide to Raft:
		if args.PrevLogIndex >= len(rf.log) {
			// If a follower does not have prevLogIndex in its log,
			// it should return with conflictIndex = len(log) and conflictTerm = None.
			reply.ConflictTerm = -1
			reply.ConflictIndex = len(rf.log)
		} else {
			// If a follower does have prevLogIndex in its log, but the term does not match,
			// it should return conflictTerm = log[prevLogIndex].Term,
			reply.ConflictTerm = rf.log[args.PrevLogIndex].Term
			// and then search its log for the first index whose entry has term equal to conflictTerm.
			conflictIndex := 1
			for conflictIndex <= args.PrevLogIndex {
				if rf.log[conflictIndex].Term == reply.ConflictTerm {
					break
				}
				conflictIndex++
			}
			reply.ConflictIndex = conflictIndex
		}
		rf.logDebug("conflictIndex = %v, conflictTerm = %v", reply.ConflictIndex, reply.ConflictTerm)
		return
	}

	reply.Success = true
	lastNewEntryIndex := args.PrevLogIndex + len(args.Entries)

	// 3. if an existing entry conflicts with a new one (same index but different terms),
	// delete the existing entry and all that follow it
	for idx, entry := range args.Entries {
		// TODO: fix indexing after implementing snapshotting
		logIndex := args.PrevLogIndex + 1 + idx
		if logIndex >= len(rf.log) {
			// log shorter than what we have to append
			break
		}
		if entry.Term != rf.log[logIndex].Term {
			rf.logDebug("log entry mismatch at index %v", logIndex)
			rf.logDebug("removing log entries starting from %v", logIndex)
			rf.log = rf.log[:logIndex]
			break
		}
	}

	// if we didn't remove any entry in the previous step,
	// there might be things after lastNewEntryIndex already in our log,
	// save them to append them later
	var moreEntries []LogEntry
	if len(rf.log) > lastNewEntryIndex+1 {
		moreEntries = rf.log[lastNewEntryIndex+1:]
	}

	// 4. append any new entries not already in the log
	rf.log = append(rf.log[:args.PrevLogIndex+1], args.Entries...)

	// append the entries after lastNewEntryIndex
	rf.log = append(rf.log, moreEntries...)

	rf.persist()

	rf.logDebug("updated log: %v", shortLog(rf.log, true))

	// 5. if leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
	if args.LeaderCommit > rf.commitIndex {
		oldCommitIndex := rf.commitIndex
		if args.LeaderCommit < lastNewEntryIndex {
			rf.commitIndex = args.LeaderCommit
		} else {
			rf.commitIndex = lastNewEntryIndex
		}
		rf.logDebug("commitIndex[%v] %v -> %v", rf.me, oldCommitIndex, rf.commitIndex)
		go rf.applyEntries()
	}

	// recognize the leader as legitimate, return to follower state
	if rf.currentTerm < args.Term {
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.persist()
	}
	if rf.state != Follower {
		//rf.logDebug("receives AppendEntries from s%v, converting to follower for term %v",
		//	args.LeaderId, args.Term)
		rf.changeState(Follower)
	}

	rf.logDebug("AppendEntries from s%v successful", args.LeaderId)
}
