package raft

import (
	"time"
)

func (rf *Raft) sendAppendEntriesTimer() {
	for {
		rf.mu.Lock()
		if rf.state != Leader || rf.killed() {
			// stop sending heartbeats when no longer leader
			rf.logVerbose("no longer leader, canceling heartbeat timer")
			rf.mu.Unlock()
			return
		}
		rf.logDebug("heartbeat")
		rf.broadcastAppendEntries()
		rf.mu.Unlock()
		time.Sleep(HeartbeatTimeout)
	}
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) makeAppendEntriesArgs(server int) AppendEntriesArgs {
	// expects lock to be held
	args := AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		LeaderCommit: rf.commitIndex,
	}
	lastLogIndex, lastLogTerm := rf.lastLogIndex(), rf.lastLogTerm()
	nextIndex := rf.nextIndex[server]
	if nextIndex <= rf.lastSnapshotIndex || nextIndex > lastLogIndex {
		// either a) we already dropped the log part that peer needs,
		// or     b) peer has everything we have in log, no new entries to send
		//if nextIndex <= rf.lastSnapshotIndex {
		//	go rf.installSnapshotToFollower(server)
		//}
		args.PrevLogIndex = lastLogIndex
		args.PrevLogTerm = lastLogTerm
		return args
	}
	args.Entries = make([]LogEntry, len(rf.log[rf.relativeIndex(nextIndex):]))
	copy(args.Entries, rf.log[rf.relativeIndex(nextIndex):])
	args.PrevLogIndex = nextIndex - 1
	args.PrevLogTerm = rf.log[rf.relativeIndex(args.PrevLogIndex)].Term
	return args
}

func (rf *Raft) advanceLeaderCommitIndex() {
	// expects lock to be held
	if rf.state != Leader {
		rf.logFatal("trying to advance commitIndex as non-leader")
	}
	newCommitIndex := rf.commitIndex
	for i := rf.commitIndex + 1; i <= rf.lastLogIndex(); i++ {
		matched := 0
		for server := range rf.peers {
			if rf.matchIndex[server] >= i {
				matched++
			}
		}
		if matched*2 <= len(rf.peers) {
			break
		}
		if rf.log[rf.relativeIndex(i)].Term == rf.currentTerm {
			newCommitIndex = i
		}
	}
	if rf.commitIndex != newCommitIndex {
		//rf.logDebug("leader commitIndex[%v] %v -> %v", rf.me, rf.commitIndex, newCommitIndex)
	}
	rf.commitIndex = newCommitIndex
	go rf.applyEntries()
}

func (rf *Raft) initLeaderStates() {
	// expect the lock to be held
	lastLogIndex := rf.lastLogIndex()
	rf.nextIndex = make([]int, len(rf.peers))
	for idx := range rf.nextIndex {
		rf.nextIndex[idx] = lastLogIndex + 1
	}
	rf.matchIndex = make([]int, len(rf.peers))
	rf.matchIndex[rf.me] = lastLogIndex
}

func (rf *Raft) broadcastAppendEntries() {
	// expects lock to be held
	for server := range rf.peers {
		if server == rf.me {
			continue
		}
		go func(server int, term int) {
			for {
				rf.mu.Lock()
				if rf.currentTerm > term {
					//rf.logDebug("my AppendEntries from term %v already expired, aborting", term)
					rf.mu.Unlock()
					return
				}
				args := rf.makeAppendEntriesArgs(server)
				rf.mu.Unlock()
				reply := AppendEntriesReply{}
				ok := rf.sendAppendEntries(server, &args, &reply)
				rf.mu.Lock()
				if !ok {
					//rf.logDebug("failed to send AppendEntries to s%v, aborting", server)
					rf.mu.Unlock()
					return
				}
				if rf.currentTerm > term {
					//rf.logDebug("my AppendEntries from term %v already expired, aborting", term)
					rf.mu.Unlock()
					return
				}
				if reply.Term > rf.currentTerm {
					//rf.logDebug("received AppendEntries reply with higher term %v, "+
					//	"converting to follower and aborting", reply.Term)
					rf.changeState(Follower)
					rf.currentTerm = reply.Term
					rf.votedFor = -1
					rf.persist()
					rf.mu.Unlock()
					return
				}
				if rf.state != Leader {
					rf.logFatal("NO LONGER LEADER")
				}
				if reply.Success {
					// update nextIndex and matchIndex for follower
					rf.logDebug("AppendEntries to s%v successful", server)
					//oldMatchIndex := rf.matchIndex[server]
					//oldNextIndex := rf.nextIndex[server]
					rf.matchIndex[server] = args.PrevLogIndex + len(args.Entries)
					rf.nextIndex[server] = rf.matchIndex[server] + 1
					//if len(args.Entries) > 0 {
					//	rf.logDebug("matchIndex[%v] %v -> %v", server, oldMatchIndex, rf.matchIndex[server])
					//	rf.logDebug("nextIndex[%v] %v -> %v", server, oldNextIndex, rf.nextIndex[server])
					//}
					rf.advanceLeaderCommitIndex()
					rf.mu.Unlock()
					return
				}

				// reply.Success == false
				//if reply.ConflictTerm == 0 && reply.ConflictIndex <= rf.lastSnapshotIndex {
				//	// follower's log out of date, send InstallSnapshot to help it catch up
				//	rf.logDebug("sending InstallSnapshot to s%v", server)
				//	go rf.installSnapshotToFollower(server)
				//	rf.mu.Unlock()
				//	return
				//}

				// from Students' Guide to Raft:
				// Upon receiving a conflict response, the leader should first search its log for conflictTerm.
				// if it finds an entry in its log with that term, it should set nextIndex to be the one beyond
				// the index of the last entry in that term in its log.
				// if it does not find an entry with that term, it should set nextIndex = conflictIndex.
				newNextIndex := rf.nextIndex[server]
				for rf.relativeIndex(newNextIndex) > 0 {
					nextNextIndex := newNextIndex - 1
					if nextNextIndex == 0 {
						// does not find an entry with that term
						newNextIndex = reply.ConflictIndex
						break
					}
					if rf.log[rf.relativeIndex(nextNextIndex)].Term == reply.ConflictTerm {
						// finds an entry with that term
						break
					}
					newNextIndex = nextNextIndex
				}
				// if AppendEntries fails because of log inconsistency,
				// decrement nextIndex and retry (§5.3)
				rf.nextIndex[server] = newNextIndex
				if rf.nextIndex[server] <= rf.lastSnapshotIndex {
					rf.logDebug("sending InstallSnapshot to s%v", server)
					go rf.installSnapshotToFollower(server)
					rf.mu.Unlock()
					return
				}
				rf.logDebug("AppendEntries failed for s%v, back up nextIndex to %v and retrying",
					server, rf.nextIndex[server])
				rf.mu.Unlock()
			}
		}(server, rf.currentTerm)
	}
}
