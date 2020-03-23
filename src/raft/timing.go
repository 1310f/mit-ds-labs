package raft

import "time"

const (
	HeartbeatTimeout     = 150 * time.Millisecond
	ElectionTimeoutMsMin = 300
	ElectionTimeoutMsMax = 600
)
