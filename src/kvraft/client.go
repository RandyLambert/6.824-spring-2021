package kvraft

import (
	"6.824/labrpc"
	"sync"
)
import "crypto/rand"
import "math/big"


type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	ClintId  int64
	ReqSeq   int
	leaderId int
	mu       sync.Mutex
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
	ck.ClintId = nrand()
	// You'll have to add code here.
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
	ck.mu.Lock()
	ck.ReqSeq++
	reqSeq := ck.ReqSeq
	leaderId := ck.leaderId
	ck.mu.Unlock()
	for {
		args := GetArgs{
			Key:     key,
			ClintId: ck.ClintId,
			ReqSeq:  reqSeq,
		}
		reply := GetReply{}
		ok := ck.servers[leaderId].Call("KVServer.Get", &args, &reply)
		DPrintf("KVServer.Get| status = %s , args.Key = %s, args.ClintId = %d, leaderId = %d",reply.Err, args.Key, args.ClintId, leaderId)

		if ok == false || reply.Err == ErrWrongLeader {
			for i := 0;i < len(ck.servers);i++ {
				reply = GetReply{}
				ok = ck.servers[i].Call("KVServer.Get", &args, &reply)
				if ok == false {
					continue
				}

				if reply.Err == ErrWrongLeader {
					continue
				}
				ck.mu.Lock()
				ck.leaderId = i
				leaderId = ck.leaderId
				ck.mu.Unlock()
				if reply.Err == OK || reply.Err == ErrNoKey {
					return reply.Value
				}
				break
			}
		}

		if reply.Err == OK || reply.Err == ErrNoKey {
			return reply.Value
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
	ck.mu.Lock()
	ck.ReqSeq++
	reqSeq := ck.ReqSeq
	leaderId := ck.leaderId
	ck.mu.Unlock()
	for {
		//DPrintf("PutAppend|in loop")
		args := PutAppendArgs {
			Key:           key,
			Value:         value,
			Op:            op,
			ClintId:       ck.ClintId,
			ReqSeq:        reqSeq,
		}
		reply := PutAppendReply{}
		ok := ck.servers[leaderId].Call("KVServer.PutAppend", &args, &reply)

		if ok == false || reply.Err == ErrWrongLeader {
			for i := 0;i < len(ck.servers);i++ {
				reply = PutAppendReply{}
				ok = ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
				if ok == false {
					continue
				}

				if reply.Err == ErrWrongLeader {
					continue
				}
				ck.mu.Lock()
				ck.leaderId = i
				leaderId = ck.leaderId
				ck.mu.Unlock()
				if reply.Err == OK {
					return
				}
				break
			}
		}
		DPrintf("KVServer.PutAppend| status = %s , args.Value = %s, args.ClintId = %d, leaderId = %d",reply.Err, args.Value, args.ClintId, leaderId)

		if reply.Err == OK {
			return
		}
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
