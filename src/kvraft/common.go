package kvraft

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongLeader = "ErrWrongLeader"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	Seq   int // 序列号，用于唯一标识一个Put/Append请求，主要使用于请求超时重发的时候(rpc返回false)，避免raft重复执行
	Key   string
	Value string
	Op    string // "Put" or "Append"
	ClerkID int64
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
}

type GetReply struct {
	Err   Err
	Value string
}
