package kvraft

import (
	"labrpc"
	"time"
)
import "crypto/rand"
import "math/big"

var retryTime = 100 * time.Millisecond

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	leaderID int
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
	ck.leaderID = 0
	return ck
}

func (ck *Clerk) Call(svcMeth string, args interface{}, reply interface{}) bool {
	return ck.servers[ck.leaderID].Call(svcMeth, args, reply)
}

//
// fetch the current Value for a Key.
// returns "" if the Key does not exist.
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
	args := GetArgs{Key:key}
	reply := GetReply{}
	// You will have to modify this function.
	for {
		// 成功获取
		if ck.Call("KVServer.Get", &args, &reply) && reply.Err == OK {
			DPrintf("Get %s success, Value is %s", key, reply.Value)
			return reply.Value
		}
		// 失败更新
		ck.leaderID = (ck.leaderID + 1) % len(ck.servers)
		time.Sleep(retryTime)
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
	args := PutAppendArgs{
		Key:   key,
		Value: value,
		Op:    op,
	}
	reply := PutAppendReply{}
	for {
		// 成功
		if ck.Call("KVServer.PutAppend", &args, &reply) && reply.Err == OK {
			DPrintf("OP: %s, Key: %s, Value: %s success", op, key, value)
			return
		}
		ck.leaderID = (ck.leaderID + 1) % len(ck.servers)
		time.Sleep(retryTime)
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
