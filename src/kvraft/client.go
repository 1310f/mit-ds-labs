package kvraft

import (
	"../labrpc"
	"crypto/rand"
	"fmt"
	"log"
	"math/big"
	mrand "math/rand"
	"time"
)

const ClientRetryTimeout = 10 * time.Millisecond

var currentClientId ClientId = 0

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	clientId ClientId
	leader   int
	seqNum   int
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	// TODO: use nrand() for clientId, using sequential for debugging
	ck.clientId = currentClientId
	currentClientId++
	ck.leader = mrand.Intn(len(servers))
	ck.seqNum = 0
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {
	// You will have to modify this function.
	ck.seqNum++
	args := GetArgs{
		Key:      key,
		ClientId: ck.clientId,
		SeqNum:   ck.seqNum,
		RetryNum: 1,
	}
	for {
		reply := GetReply{}
		ok := ck.servers[ck.leader].Call("KVServer.Get", &args, &reply)
		if !ok {
			ck.leader = (ck.leader + 1) % len(ck.servers)
			time.Sleep(ClientRetryTimeout)
			args.RetryNum++
			continue
		}
		switch reply.Err {
		case OK:
			ck.logInfo("op%v.%v Get(%v) OK: %v", ck.clientId, args.SeqNum, args.Key, reply.Value)
			return reply.Value
		case ErrNoKey:
			ck.logInfo("op%v.%v Get(%v) ErrNoKey", ck.clientId, args.SeqNum, args.Key)
			return ""
		case ErrWrongLeader:
			fallthrough
		default:
			ck.leader = (ck.leader + 1) % len(ck.servers)
			time.Sleep(ClientRetryTimeout)
			args.RetryNum++
			continue
		}
	}
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	ck.seqNum++
	args := PutAppendArgs{
		Key:      key,
		Value:    value,
		Op:       op,
		ClientId: ck.clientId,
		SeqNum:   ck.seqNum,
		RetryNum: 1,
	}
	for {
		reply := PutAppendReply{}
		ok := ck.servers[ck.leader].Call("KVServer.PutAppend", &args, &reply)
		if !ok {
			ck.leader = (ck.leader + 1) % len(ck.servers)
			time.Sleep(ClientRetryTimeout)
			args.RetryNum++
			continue
		}
		switch reply.Err {
		case OK:
			ck.logInfo("op%v.%v %v(%v, %v) OK",
				ck.clientId, args.SeqNum, args.Op, args.Key, args.Value)
			return
		case ErrNoKey:
			ck.logInfo("op%v.%v %v(%v, %v) ErrNoKey",
				ck.clientId, args.SeqNum, args.Op, args.Key, args.Value)
			return
		case ErrWrongLeader:
			fallthrough
		default:
			ck.leader = (ck.leader + 1) % len(ck.servers)
			time.Sleep(ClientRetryTimeout)
			args.RetryNum++
			continue
		}
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}

//region Logging utils

func (ck *Clerk) logInfo(format string, a ...interface{}) {
	newFormat := ck.prependLogTag("INFO", format)
	log.Printf(newFormat, a...)
}

func (ck *Clerk) logWarning(format string, a ...interface{}) {
	newFormat := ck.prependLogTag("WARNING", format)
	log.Printf(newFormat, a...)
}

func (ck *Clerk) logFatal(format string, a ...interface{}) {
	newFormat := ck.prependLogTag("FATAL", format)
	log.Fatalf(newFormat, a...)
}

func (ck *Clerk) logDebug(format string, a ...interface{}) {
	if Debug > 0 {
		newFormat := ck.prependLogTag("DEBUG", format)
		log.Printf(newFormat, a...)
	}
}

func (ck *Clerk) prependLogTag(level string, format string) string {
	tag := fmt.Sprintf("[%7s] [c%v] ",
		level, ck.clientId)
	return tag + format
}

//endregion
