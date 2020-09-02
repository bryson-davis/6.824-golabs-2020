package kvraft

import (
	"labgob"
	"labrpc"
	"log"
	"raft"
	"sync"
	"sync/atomic"
)

const Debug = 0

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
	Action string
	Seq  int
	ClerkID int64
	Key    string
	Value  string
}

// 用于接收到raft的提交信号之后进行封装并提交给rpc处理函数进行处理
type NotifyMsg struct {
	err Err
	value string
}


type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()
	kvStore map[string]string
	clientCurSeqMap map[int64]int

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	notifyChanMap map[int]chan NotifyMsg
}


func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	op := Op{
		Action: "Get",
		Key:    args.Key,
	}
	reply.Err, reply.Value = kv.DoAction(op)
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	op := Op{
		Action: args.Op,
		Seq: args.Seq,
		Key:    args.Key,
		Value:  args.Value,
		ClerkID: args.ClerkID,
	}
	reply.Err, _ = kv.DoAction(op)
}

func (kv *KVServer) DoAction(args interface{}) (Err, string) {
	// 调用kv.raft的start，并等待结果
	index, _, isLeader := kv.rf.Start(args)
	if !isLeader {
		return ErrWrongLeader, ""
	}
	// 配置通知通道
	kv.mu.Lock()
	notifyCh := make(chan NotifyMsg)
	kv.notifyChanMap[index] = notifyCh
	kv.mu.Unlock()

	// 等待通知信号
	select {
	case msg := <-notifyCh:
		//if msg.term != term {
		//	return ErrWrongLeader,  ""
		//}
		return msg.err, msg.value
	}
}

// 启动kvServer，等待commit信号
func (kv *KVServer) run() {
	for {
		select {
		case msg := <-kv.applyCh:
			kv.apply(&msg)
		}
	}
}

// 接收到commit信号之后进行apply到kvstore,并通知到相应的通道
func (kv *KVServer) apply(msg *raft.ApplyMsg) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	notifyMsg := NotifyMsg{err:OK}
	op := msg.Command.(Op)
	if op.Action == "Get" {
		notifyMsg.value = kv.kvStore[op.Key]
	} else if kv.clientCurSeqMap[op.ClerkID] < op.Seq {
		log.Println("clerkID: ", op.ClerkID)
		log.Println("map: ", kv.clientCurSeqMap[op.ClerkID])
		log.Println("seq: ", op.Seq)
		if op.Action == "Put" {
			kv.kvStore[op.Key] = op.Value
		} else {
			kv.kvStore[op.Key] = kv.kvStore[op.Key] + op.Value
		}
		kv.clientCurSeqMap[op.ClerkID] = op.Seq
	} else {
		log.Println("has been apply request")
	}
	notifyMsg.value = kv.kvStore[op.Key]

	// 进行通道通知
	index := msg.CommandIndex
	notifyCh := kv.notifyChanMap[index]
	delete(kv.notifyChanMap, index)
	notifyCh <- notifyMsg
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
// form the fault-tolerant Key/Value service.
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

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.

	kv.notifyChanMap = make(map[int]chan NotifyMsg, 0)
	kv.kvStore = make(map[string]string, 0)
	kv.clientCurSeqMap = make(map[int64]int, 0)
	go kv.run()
	return kv
}


