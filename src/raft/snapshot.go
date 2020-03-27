package raft

func (rf *Raft) SaveSnapshot(snapshot *[]byte, snapshotIndex int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if snapshotIndex <= rf.lastSnapshotIndex {
		return
	}
	if snapshotIndex > rf.commitIndex {
		rf.logFatal("snapshotIndex %v beyond commitIndex %v", snapshotIndex, rf.commitIndex)
	}

	// log[0] reserved as last log item
	rf.logDebug("dropping log before index %v", snapshotIndex)
	rf.log = rf.log[rf.relativeIndex(snapshotIndex):]
	rf.lastSnapshotIndex = snapshotIndex
	rf.lastSnapshotTerm = rf.log[0].Term
	rf.persist()

	// use persister to save both state and snapshot
	state := rf.encodePersistentStates()
	rf.persister.SaveStateAndSnapshot(*state, *snapshot)
}

func (rf *Raft) installSnapshotToFollower(server int) {
	// send InstallSnapshot to peers
	rf.mu.Lock()
	defer rf.mu.Unlock()
	args := InstallSnapshotArgs{
		Term:              rf.currentTerm,
		LeaderId:          rf.me,
		LastIncludedIndex: rf.lastSnapshotIndex,
		LastIncludedTerm:  rf.lastSnapshotTerm,
		Offset:            0,
		Data:              rf.persister.ReadSnapshot(),
		Done:              true,
	}
	reply := InstallSnapShotReply{}
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	if !ok {
		return
	}
	if reply.Term > rf.currentTerm {
		rf.changeState(Follower)
		rf.currentTerm = reply.Term
		rf.votedFor = -1
		rf.persist()
		return
	}
}
