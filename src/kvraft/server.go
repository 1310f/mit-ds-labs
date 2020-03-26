package kvraft

import (
	"../labgob"
	"../labrpc"
	"../raft"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"
)

const Debug = 1

const RaftTimeout = 1 * time.Second

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
	RequestId RequestId
	Method    string
	Key       string
	Value     string
}

type Result struct {
	Err   Err
	Value string
}

type CommandId struct {
	ClientId ClientId
	SeqNum   int
}

type RequestId struct {
	ClientId ClientId
	SeqNum   int
	RetryNum int
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
	lastSeen map[ClientId]int
	//resultCh map[ClientId]chan Result
	resultCh map[RequestId]chan Result

	// used to return failures
	clientLogIndex map[RequestId]int
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	requestId := RequestId{
		ClientId: args.ClientId,
		SeqNum:   args.SeqNum,
		RetryNum: args.RetryNum,
	}
	op := Op{
		RequestId: requestId,
		Method:    "Get",
		Key:       args.Key,
		Value:     "",
	}
	result := kv.sendToRaft(op)
	reply.Err = result.Err
	reply.Value = result.Value
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	requestId := RequestId{
		ClientId: args.ClientId,
		SeqNum:   args.SeqNum,
		RetryNum: args.RetryNum,
	}
	op := Op{
		RequestId: requestId,
		Method:    args.Op,
		Key:       args.Key,
		Value:     args.Value,
	}
	result := kv.sendToRaft(op)
	reply.Err = result.Err
}

func (kv *KVServer) sendToRaft(op Op) Result {
	index, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		kv.logDebug("received %v, not leader, rejecting", op.String())
		return Result{
			Err: ErrWrongLeader,
		}
	}
	kv.mu.Lock()
	kv.logInfo("received %v, creating result channel", op.String())
	kv.resultCh[op.RequestId] = make(chan Result)
	kv.clientLogIndex[op.RequestId] = index
	resultCh := kv.resultCh[op.RequestId]
	kv.mu.Unlock()
	select {
	case result := <-resultCh:
		kv.mu.Lock()
		kv.logDebug("%v finished, deleting channel", op.RequestId.String())
		delete(kv.resultCh, op.RequestId)
		kv.mu.Unlock()
		return result
	case <-time.After(RaftTimeout):
		kv.mu.Lock()
		kv.logWarning("%v timed out, deleting channel", op.RequestId.String())
		delete(kv.resultCh, op.RequestId)
		kv.mu.Unlock()
		return Result{Err: ErrTimeout}
	}
}

func (kv *KVServer) applyLoop() {
	for {
		if kv.killed() {
			kv.mu.Lock()
			kv.logInfo("killed")
			kv.mu.Unlock()
			return
		}
		applyMsg := <-kv.applyCh
		if applyMsg.CommandValid {
			kv.mu.Lock()
			op := applyMsg.Command.(Op)
			kv.logDebug("%v passed raft", op.RequestId.String())
			clientId := op.RequestId.ClientId
			seqNum := op.RequestId.SeqNum
			commandId := CommandId{
				ClientId: clientId,
				SeqNum:   seqNum,
			}
			if kv.lastSeen[clientId] >= seqNum {
				kv.logDebug("%v already executed, skipping", commandId.String())
			} else {
				switch op.Method {
				case "Get":
				case "Put":
					kv.data[op.Key] = op.Value
				case "Append":
					kv.data[op.Key] += op.Value
				default:
					kv.logFatal("trying to apply unknown op: %v", op.Method)
				}
				kv.logDebug("%v executed for the first time", commandId.String())
				kv.lastSeen[clientId] = seqNum
			}

			if resultCh, ok := kv.resultCh[op.RequestId]; ok {
				v := kv.data[op.Key]
				result := Result{
					Err:   OK,
					Value: v,
				}
				kv.logDebug("%v done, returning result through channel", op.RequestId.String())
				kv.mu.Unlock()
				// FIXME: this could block if no one's receiving on the other side
				resultCh <- result
			} else {
				kv.mu.Unlock()
			}

			kv.mu.Lock()
			var reqFailed []RequestId
			for req, index := range kv.clientLogIndex {
				if index == applyMsg.CommandIndex && req != op.RequestId {
					reqFailed = append(reqFailed, req)
				}
			}
			kv.mu.Unlock()

			for _, request := range reqFailed {
				kv.mu.Lock()
				if resultCh, ok := kv.resultCh[request]; ok {
					kv.logDebug("%v failed in raft", op.RequestId.String())
					result := Result{
						Err: ErrWrongLeader,
					}
					kv.mu.Unlock()
					resultCh <- result
				} else {
					kv.mu.Unlock()
				}
			}

			//if resultCh, ok := kv.resultCh[op.ClientId]; ok {
			//	v := kv.data[op.Key]
			//	result := Result{
			//		Err:   OK,
			//		Value: v,
			//	}
			//	kv.mu.Unlock()
			//	resultCh <- result
			//} else {
			//	kv.mu.Unlock()
			//}

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
	kv.lastSeen = make(map[ClientId]int)
	kv.clientLogIndex = make(map[RequestId]int)
	//kv.resultCh = make(map[ClientId]chan Result)
	kv.resultCh = make(map[RequestId]chan Result)

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
	tag := fmt.Sprintf("[%7s] [k%v] %v ", level, kv.me, kv.rf.GetTermStateTag())
	return tag + format
}

func (op *Op) String() string {
	return op.RequestId.String() + " " + op.CommandString()
}

func (rid *RequestId) String() string {
	return fmt.Sprintf("op%v.%v#%v", rid.ClientId, rid.SeqNum, rid.RetryNum)
}

func (cid *CommandId) String() string {
	return fmt.Sprintf("op%v.%v", cid.ClientId, cid.SeqNum)
}

func (op *Op) CommandString() string {
	switch op.Method {
	case "Get":
		return fmt.Sprintf("Get(%v)", op.Key)
	case "Put":
		fallthrough
	case "Append":
		return fmt.Sprintf("%v(%v, %v)", op.Method, op.Key, op.Value)
	default:
		return "Unknown"
	}
}

//endregion
