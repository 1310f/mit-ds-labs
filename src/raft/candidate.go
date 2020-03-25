package raft

import "time"

func (rf *Raft) beginElection() {
	rf.mu.Lock()
	rf.state = Candidate
	rf.currentTerm++
	rf.votedFor = rf.me

	voteCh := make(chan bool, len(rf.peers)-1)

	for server := range rf.peers {
		if server == rf.me {
			continue
		}
		go func(server int, term int) {
			rf.mu.Lock()
			if rf.currentTerm > term {
				// term has expired, no longer candidate
				rf.mu.Unlock()
				return
			}
			args := RequestVoteArgs{
				Term:         rf.currentTerm,
				CandidateId:  rf.me,
				LastLogIndex: rf.lastLogIndex(),
				LastLogTerm:  rf.lastLogTerm(),
			}
			reply := RequestVoteReply{}
			rf.mu.Unlock()

			// blocking
			ok := rf.sendRequestVote(server, &args, &reply)

			rf.mu.Lock()
			if !ok {
				//rf.logDebug("failed to send RequestVote to s%v", server)
				rf.mu.Unlock()
				// blocking
				voteCh <- false
				return
			}
			if rf.currentTerm > term {
				// term has expired, no longer candidate
				rf.mu.Unlock()
				return
			}
			// rf.currentTerm == term
			//rf.logDebug("sent RequestVote to s%v", server)
			if reply.Term < rf.currentTerm || rf.state != Candidate {
				//rf.logDebug("received expired vote from term %v, ignoring", reply.Term)
				rf.mu.Unlock()
				return
			}
			if reply.Term > args.Term {
				// someone has higher term than us, abort and become follower
				//rf.logDebug("received NO vote with higher term %v, "+
				//	"aborting and converting to Follower", reply.Term)
				rf.currentTerm = reply.Term
				rf.votedFor = -1
				rf.persist()
				rf.changeState(Follower)
				rf.mu.Unlock()
				return
			}
			rf.mu.Unlock()
			voteCh <- reply.VoteGranted
		}(server, rf.currentTerm)
	}
	rf.mu.Unlock()

	// always votes for myself
	voteCount := 1
	grantCount := 1
	rf.mu.Lock()
	//rf.logDebug("waiting for votes to come in...")
	rf.mu.Unlock()
	for {
		// when all peers have voted, or grant/deny votes have reached 1/2, stop waiting for votes
		if voteCount == len(rf.peers) ||
			grantCount*2 > len(rf.peers) ||
			(voteCount-grantCount)*2 > len(rf.peers) {
			rf.mu.Lock()
			//rf.logDebug("collected enough votes")
			rf.mu.Unlock()
			break
		}
		// otherwise wait for more votes to come in

		startTime := time.Now()
		timeout := randomElectionTimeout()
		select {
		case grant := <-voteCh:
			voteCount++
			if grant {
				grantCount++
			}
		case <-time.After(timeout):
			//elapsed := time.Now().Sub(startTime)
			if time.Now().Sub(startTime) > timeout {
				rf.mu.Lock()
				//rf.logDebug("election timeout (%vms), starting new election", elapsed.Milliseconds())
				rf.changeState(Candidate)
				rf.mu.Unlock()
			}
			return
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
