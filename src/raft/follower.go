package raft

import "time"

func (rf *Raft) electionTimer() {
	for !rf.killed() {
		timeoutDuration := randomElectionTimeout()
		time.Sleep(timeoutDuration)
		rf.mu.Lock()
		// if no longer follower, cancel timer
		if rf.state != Follower {
			//rf.logDebug("no longer follower, canceling election timer")
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
	if rf.killed() {
		return
	}
	rf.mu.Lock()
	rf.changeState(Candidate)
	rf.mu.Unlock()
}
