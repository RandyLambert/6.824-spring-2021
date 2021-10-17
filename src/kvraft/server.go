package kvraft

import (
	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
	"log"
	"sync"
	"sync/atomic"
	"time"
)

const Debug = false

func DPrintf(format string, a ...interface{}) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}


type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Op string // "get" "put" "append"
	Key string
	Value string
	ClientId int64
	ReqSeq int
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	clientMap map[int64]int
	kvMap     map[string]string
	waitApplyChMap map[int64]chan Op
	//leaderChangeCh chan bool
}


func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	//index, term, isLeader := kvMap.rf.Start(args)
	op := Op{
		Op:       "Get",
		Key:      args.Key,
		ClientId: args.ClintId,
		ReqSeq:   args.ReqSeq,
	}

	_ , _, isLeader := kv.rf.Start(op)
	if isLeader == false {
		reply.Err = ErrWrongLeader
		// todo checkout
		// 重新初始化
		//close(kv.leaderChangeCh)
		return
	}
	kv.mu.Lock()
	applyCh,flag := kv.waitApplyChMap[args.ClintId]
	DPrintf("Get|receive args.ClintId = %d, flag = %t, kv.me = %d, len(kv.waitApplyChMap) = %d, applyCh = %p",args.ClintId, flag, kv.me, len(kv.waitApplyChMap), applyCh)
	if flag == false {
		kv.waitApplyChMap[args.ClintId] = make(chan Op,1)
		applyCh = kv.waitApplyChMap[args.ClintId]
	}
	kv.mu.Unlock()

	timeOut := time.After(300 * time.Millisecond)

	select {
	case op =<- applyCh:
		if _,isLeader = kv.rf.GetState(); isLeader == false {
			reply.Err = ErrWrongLeader
			//close(kv.leaderChangeCh)
		} else {
			reply.Value = op.Value
			if op.Value == "" {
				reply.Err = ErrNoKey
			}
			reply.Err = OK
		}
		return
	case <-timeOut:
		reply.Err = ErrRpcTimeout
		return
	//case <-kv.leaderChangeCh:
	//	reply.Err = ErrWrongLeader
	//	return
	}

}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	//index, term, isLeader := kvMap.rf.Start(args)
	op := Op{
		Op:       args.Op,
		Key:      args.Key,
		Value:    args.Value,
		ClientId: args.ClintId,
		ReqSeq:   args.ReqSeq,
	}
	_, _, isLeader := kv.rf.Start(op)
	if isLeader == false {
		reply.Err = ErrWrongLeader
		//close(kv.leaderChangeCh)
		return
	}

	kv.mu.Lock()
	applyCh,flag := kv.waitApplyChMap[args.ClintId]
	DPrintf("PutAppend|receive args.ClintId = %d, flag = %t, kv.me = %d, len(kv.waitApplyChMap) = %d, applyCh = %p",args.ClintId, flag, kv.me, len(kv.waitApplyChMap), applyCh)
	if flag == false {
		kv.waitApplyChMap[args.ClintId] = make(chan Op,1)
		applyCh = kv.waitApplyChMap[args.ClintId]
	}
	kv.mu.Unlock()

	timeOut := time.After(300 * time.Millisecond)

	select {
	case <-applyCh:
		if _,isLeader = kv.rf.GetState(); isLeader == false {
			reply.Err = ErrWrongLeader
			//close(kv.leaderChangeCh)
		} else {
			reply.Err = OK
		}
		return
	case <-timeOut:
		reply.Err = ErrRpcTimeout
		// TODO checkout
		return
	//case <-kv.leaderChangeCh:
	//	reply.Err = ErrWrongLeader
	//	return
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

func (kv *KVServer) applyLoop() {
	for kv.killed() == false {
		select {
		case apply := <-kv.applyCh:
			kv.mu.Lock()
			DPrintf("applyLoop|apply.Command.(Op).ClientId = %d, apply.Command.(Op).Key = %s,apply.Command.(Op).Value = %s, apply.Command.(Op).Op = %s,apply.Command.(Op).ReqSeq = %d, kv.me = %d, len(kv.waitApplyChMap) = %d",apply.Command.(Op).ClientId, apply.Command.(Op).Key,apply.Command.(Op).Value,apply.Command.(Op).Op,apply.Command.(Op).ReqSeq, kv.me, len(kv.waitApplyChMap))
			kv.mu.Unlock()
			if apply.CommandValid == true {
				if apply.Command.(Op).Op == "Get" {
					kv.mu.Lock()
					ch,flag := kv.waitApplyChMap[apply.Command.(Op).ClientId]
					op := apply.Command.(Op)
					if value, ok := kv.kvMap[apply.Command.(Op).Key]; ok == true {
						op.Value = value
					}
					if apply.Command.(Op).ReqSeq > kv.clientMap[apply.Command.(Op).ClientId] {
						kv.clientMap[apply.Command.(Op).ClientId] = apply.Command.(Op).ReqSeq
					}
					kv.mu.Unlock()
					//&& apply.seq == client.seq
					if flag == true {
						//DPrintf("ch = %p, kv.me = %d,len(kv.waitApplyChMap) = %d", ch, kv.me, len(kv.waitApplyChMap))
						ch <- op
						kv.mu.Lock()
						delete(kv.waitApplyChMap, apply.Command.(Op).ClientId)
						kv.mu.Unlock()
						//go func() {ch <- op}()
					}
				} else {
					kv.mu.Lock()
					ch,flag := kv.waitApplyChMap[apply.Command.(Op).ClientId]
					if kv.clientMap[apply.Command.(Op).ClientId] >= apply.Command.(Op).ReqSeq {
						kv.mu.Unlock()
						//&& client.seq == apply.seq
						if flag == true {
							//go func () {ch <- apply.Command.(Op)}()
							ch <- apply.Command.(Op)
							kv.mu.Lock()
							delete(kv.waitApplyChMap, apply.Command.(Op).ClientId)
							kv.mu.Unlock()
						}
						break
					}

					kv.clientMap[apply.Command.(Op).ClientId] = apply.Command.(Op).ReqSeq

					op := apply.Command.(Op)
					if apply.Command.(Op).Op == "Put" {
						kv.kvMap[apply.Command.(Op).Key] = apply.Command.(Op).Value
					} else if apply.Command.(Op).Op == "Append" {
						if value, ok := kv.kvMap[apply.Command.(Op).Key];ok == false {
							kv.kvMap[apply.Command.(Op).Key] = apply.Command.(Op).Value
						} else {
							value += apply.Command.(Op).Value
							kv.kvMap[apply.Command.(Op).Key] = value
							op.Value = value
						}
					}

					kv.mu.Unlock()
					//&& client.seq == apply.seq
					if flag == true {
						//go func() {ch <- op}()
						ch <- op
						kv.mu.Lock()
						delete(kv.waitApplyChMap, apply.Command.(Op).ClientId)
						kv.mu.Unlock()
					}
				}

			} else if apply.SnapshotValid == true {
				// TODO
				// Snapshot 重建
			}
		}
	}
}

func (kv *KVServer) InstallSnapShot(snapshot []byte) {
	if snapshot == nil || len(snapshot) < 1 { // bootstrap without any state?
		return
	}

	//r := bytes.NewBuffer(snapshot)
	//d := labgob.NewDecoder(r)
	//
	//var kvMap map[string]string
	//var lastRequestId map[int64]int

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
	kv.clientMap = make(map[int64]int)
	kv.kvMap = make(map[string]string)
	kv.waitApplyChMap = make(map[int64]chan Op)

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// 读 snapshot 重建
	snapshot := persister.ReadSnapshot()
	if len(snapshot) > 0 {
		kv.InstallSnapShot(snapshot)
	}
	// You may need initialization code here.

	go kv.applyLoop()
	return kv
}
