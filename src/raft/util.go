package raft

import (
	"fmt"
	"log"
	"math/rand"
	"strconv"
	"time"
)

// Debugging
const Debug = 1

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

func (rf *Raft) logInfo(format string, a ...interface{}) {
	newFormat := rf.prependLogTag("INFO", format)
	log.Printf(newFormat, a...)
}

func (rf *Raft) logWarning(format string, a ...interface{}) {
	newFormat := rf.prependLogTag("WARNING", format)
	log.Printf(newFormat, a...)
}

func (rf *Raft) logFatal(format string, a ...interface{}) {
	newFormat := rf.prependLogTag("FATAL", format)
	log.Fatalf(newFormat, a...)
}

func (rf *Raft) logDebug(format string, a ...interface{}) {
	if Debug > 0 {
		newFormat := rf.prependLogTag("DEBUG", format)
		log.Printf(newFormat, a...)
	}
}

func (rf *Raft) prependLogTag(level string, format string) string {
	tag := fmt.Sprintf("[%7s] [s%v] [%4d%v] [%v] ",
		level, rf.me, rf.currentTerm, shortStateName(rf.state), shortVotedFor(rf.votedFor))
	return tag + format
}

// FIXME: delete
func (rf *Raft) GetTermStateTag() string {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return fmt.Sprintf("[%4d%v]", rf.currentTerm, shortStateName(rf.state))
}

func stateName(state State) string {
	switch state {
	case Init:
		return "init"
	case Follower:
		return "follower"
	case Candidate:
		return "candidate"
	case Leader:
		return "leader"
	default:
		return "unknown"
	}
}

func shortStateName(state State) string {
	switch state {
	case Init:
		return "I"
	case Follower:
		return "F"
	case Candidate:
		return "C"
	case Leader:
		return "L"
	default:
		return "?"
	}
}

func shortVotedFor(server int) string {
	if server == -1 {
		return "X"
	} else {
		return strconv.Itoa(server)
	}
}

func randomElectionTimeout() time.Duration {
	sleepTimeInMs := rand.Intn(ElectionTimeoutMsMax-ElectionTimeoutMsMin) + ElectionTimeoutMsMin
	return time.Duration(sleepTimeInMs) * time.Millisecond
}

func shortLog(entries []LogEntry, omitFirst bool) []int {
	if omitFirst {
		if len(entries) <= 1 {
			return []int{}
		}
		var terms []int
		for _, entry := range entries[1:] {
			terms = append(terms, entry.Term)
		}
		return terms
	} else {
		var terms []int
		for _, entry := range entries {
			terms = append(terms, entry.Term)
		}
		return terms
	}
}
