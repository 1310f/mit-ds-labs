package kvraft

import (
	"../labgob"
	"bytes"
)

func (kv *KVServer) startSnapshot(snapshotIndex int) {
	kv.logDebug("raftStateSize = %vB", kv.persister.RaftStateSize())
	if kv.maxraftstate == -1 || kv.persister.RaftStateSize() < kv.maxraftstate {
		// need not snapshot
		return
	}
	kv.logDebug("starting snapshot")
	snapShotData := kv.makeSnapshot()
	kv.rf.SaveSnapshot(&snapShotData, snapshotIndex)
}

func (kv *KVServer) makeSnapshot() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	_ = e.Encode(kv.data)
	_ = e.Encode(kv.lastSeen)
	data := w.Bytes()
	return data
}
