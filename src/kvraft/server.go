package kvraft

import (
	"../labgob"
	"../labrpc"
	"../raft"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
)

const Debug = 1

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	ClientId ClientId
	SeqNum   int
	Method   string
	Key      string
	Value    string
}

type Result struct {
	Err   Err
	Value string
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	data     map[string]string
	resultCh map[ClientId]chan Result
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	op := Op{
		ClientId: args.ClientId,
		SeqNum:   args.SeqNum,
		Method:   "Get",
		Key:      args.Key,
		Value:    "",
	}
	_, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	kv.logInfo("received from c%v op%v: Get(%v)", args.ClientId, args.SeqNum, args.Key)
	kv.logDebug("c%v op%v sent to raft", op.ClientId, op.SeqNum)
	kv.resultCh[args.ClientId] = make(chan Result)
	result := <-kv.resultCh[args.ClientId]
	reply.Err = result.Err
	reply.Value = result.Value
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	op := Op{
		ClientId: args.ClientId,
		SeqNum:   args.SeqNum,
		Method:   args.Op,
		Key:      args.Key,
		Value:    args.Value,
	}
	_, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	kv.logInfo("received from c%v op%v: PutAppend(%v, %v, %v)",
		args.ClientId, args.SeqNum, args.Key, args.Value, args.Op)
	kv.logDebug("c%v op%v sent to raft", op.ClientId, op.SeqNum)
	kv.resultCh[args.ClientId] = make(chan Result)
	result := <-kv.resultCh[args.ClientId]
	reply.Err = result.Err
}

func (kv *KVServer) applyLoop() {
	for {
		applyMsg := <-kv.applyCh
		if applyMsg.CommandValid {
			op := applyMsg.Command.(Op)
			kv.logDebug("c%v op%v passed raft", op.ClientId, op.SeqNum)
			switch op.Method {
			case "Get":
				v := kv.data[op.Key]
				kv.resultCh[op.ClientId] <- Result{
					Err:   OK,
					Value: v,
				}
			case "Put":
				kv.data[op.Key] = op.Value
				v := kv.data[op.Key]
				kv.resultCh[op.ClientId] <- Result{
					Err:   OK,
					Value: v,
				}
			case "Append":
				kv.data[op.Key] += op.Value
				v := kv.data[op.Key]
				kv.resultCh[op.ClientId] <- Result{
					Err:   OK,
					Value: v,
				}
			default:
				kv.logFatal("trying to apply unknown op: %v", op.Method)
			}
		}
	}
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.
	kv.data = make(map[string]string)
	kv.resultCh = make(map[ClientId]chan Result)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	go kv.applyLoop()

	return kv
}

//region Logging utils

func (kv *KVServer) logInfo(format string, a ...interface{}) {
	newFormat := kv.prependLogTag("INFO", format)
	log.Printf(newFormat, a...)
}

func (kv *KVServer) logWarning(format string, a ...interface{}) {
	newFormat := kv.prependLogTag("WARNING", format)
	log.Printf(newFormat, a...)
}

func (kv *KVServer) logFatal(format string, a ...interface{}) {
	newFormat := kv.prependLogTag("FATAL", format)
	log.Fatalf(newFormat, a...)
}

func (kv *KVServer) logDebug(format string, a ...interface{}) {
	if Debug > 0 {
		newFormat := kv.prependLogTag("DEBUG", format)
		log.Printf(newFormat, a...)
	}
}

func (kv *KVServer) prependLogTag(level string, format string) string {
	tag := fmt.Sprintf("[%7s] [k%v] ", level, kv.me)
	return tag + format
}

//endregion
