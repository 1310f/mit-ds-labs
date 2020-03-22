package raft

import (
	"fmt"
	"log"
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
	tag := fmt.Sprintf("[s%v @ %3d%v] [%7s] ",
		rf.me, rf.currentTerm, shortStateName(rf.state), level)
	return tag + format
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
