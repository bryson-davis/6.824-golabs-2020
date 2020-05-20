package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"fmt"
	"math/rand"
	"sync"
	"time"
)
import "sync/atomic"
//import "../labrpc"
import "labrpc"

// import "bytes"
// import "../labgob"

type ServerState string

const (
	FOLLOWER ServerState = "follower"
	LEADER ServerState = "leader"
	CANDIDATE ServerState = "candidate"
)

//
// as each Raft peer becomes aware that successive log Entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

type LogEntry struct {
	Term int
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	state ServerState   //当成节点的状态
	expireTime time.Time

	currentTerm int
	voterFor int
	log []LogEntry
	commitIndex int
	lastApplied int

	nextIndex []int
	matchIndex []int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	term = rf.currentTerm
	isleader = rf.state == LEADER
	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}


//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}




//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.currentTerm > args.Term {
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		return
	}

	if rf.currentTerm < args.Term {
		rf.currentTerm = args.Term
		rf.voterFor = -1
		rf.state = FOLLOWER
	}

	if rf.voterFor == -1 || rf.voterFor == args.CandidateId {
		var lastLogIndex, lastLogTerm int
		if len(rf.log) == 0 {
			lastLogIndex = -1
			lastLogTerm = -1
		} else {
			lastLogIndex = len(rf.log) -1
			lastLogTerm = rf.log[lastLogIndex].Term
		}
		if args.LastLogTerm > lastLogTerm || (args.LastLogTerm == lastLogTerm && args.LastLogIndex >= lastLogIndex) {
			rf.state = FOLLOWER
			rf.voterFor = args.CandidateId
			rf.refreshExpireTime()

			reply.VoteGranted = true
			reply.Term = rf.currentTerm
			return
		}
	}

	reply.Term = rf.currentTerm
	reply.VoteGranted = false
	return

}

type AppendEntriesArgs struct {
	Term        int
	LeaderId    int
	PreLogIndex int
	PreLogTerm  int
	Entries     []LogEntry
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}


func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// 什么情况下可以认为心跳是合法的呢
	if args.Term < rf.currentTerm {
		reply.Success = false
		reply.Term = rf.currentTerm
		return
	}
	//心跳处理
	//处理是老leader的情况/candidater的情况
	fmt.Printf("debug: hearbeat from server-%d, server-%d %s->FOLLOWER\n", args.LeaderId, rf.me, rf.state)
	rf.state = FOLLOWER
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.voterFor = -1
	}
	rf.refreshExpireTime()
	reply.Term = rf.currentTerm //设置完自己的再返回
	reply.Success = true
	return

}
//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).


	return index, term, isLeader
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	fmt.Println("debug: make raft")
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.voterFor = -1

	rf.state = FOLLOWER
	rf.refreshExpireTime()

	rf.currentTerm = 0
	rf.voterFor = -1
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.log = make([]LogEntry, 1) //从1开始，

	// Your initialization code here (2A, 2B, 2C).
	//进入死循环，不断更新自己的状态
	go rf.tick()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	return rf
}

func (rf *Raft) refreshExpireTime() {
	switch rf.state {
	case CANDIDATE, FOLLOWER:
		rf.expireTime = time.Now().Add(time.Duration(200 + rand.Intn(100)) * time.Millisecond)
	case LEADER:
		rf.expireTime = time.Now().Add(time.Duration(100) * time.Millisecond)
	}
}

// 周期性进行更新与操作
func (rf *Raft) tick() {
	for !rf.killed() {

		//检查是否超时
		if rf.expireTime.After(time.Now()) {
			time.Sleep(10 * time.Millisecond)
			continue
		}
		switch rf.state {
		case LEADER:
			//周期性发送心跳
			rf.heartbeat()
			//打印下当前leader的情况
		case FOLLOWER:
			rf.state = CANDIDATE
			fmt.Printf("debug: server-%d FOLLOWER->CANDIDATE in term %d\n", rf.me, rf.currentTerm)
			fallthrough
		case CANDIDATE:
			//fmt.Printf("debug: %d CANDIDATE request vote\n", rf.me)
			rf.requestVote()
		}
	}

}

func (rf *Raft) requestVote() {
	rf.currentTerm++
	var count int32 = 1
	rf.voterFor = rf.me
	rf.refreshExpireTime()
	args := &RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
	}
	if len(rf.log) == 0 {
		args.LastLogIndex = -1
		args.LastLogTerm = -1
	} else {
		args.LastLogIndex = len(rf.log) - 1
		args.LastLogTerm = rf.log[len(rf.log)-1].Term
	}

	for i, _ := range rf.peers {
		if i == rf.me {
			continue
		}
		//需要修改为协程处理
		go func(i int) {
			reply := &RequestVoteReply{}
			ok := rf.sendRequestVote(i, args, reply)
			//网络不通
			if !ok {
				fmt.Println("rpc failed")
				return
			}
			rf.mu.Lock()
			defer rf.mu.Unlock()

			//获取了一个更新的响应
			if reply.Term > rf.currentTerm {
				rf.currentTerm = reply.Term
				rf.state = FOLLOWER
				rf.voterFor = -1
				return
			}

			if reply.VoteGranted && rf.state == CANDIDATE{
				fmt.Printf("debug: server-%d got vote from server-%d in term %d, the count is %d \n", rf.me, i, rf.currentTerm, int(atomic.LoadInt32(&count)) + 1)
				if int(atomic.AddInt32(&count, 1)) > len(rf.peers) / 2 {
					fmt.Printf("debug: server-%d CANDIDATE->LEADER in term %d\n", rf.me, rf.currentTerm)
					rf.state = LEADER
					rf.heartbeat()
				}
			}
		}(i)
	}

}

func (rf *Raft) heartbeat() {
	rf.refreshExpireTime()
	for i, _ := range rf.peers {
		if i == rf.me {
			continue
		}
		args := &AppendEntriesArgs{
			Term:     rf.currentTerm,
			LeaderId: rf.me,
			Entries:  nil,
		}
		go func(i int) {
			reply := &AppendEntriesReply{}
			ok := rf.sendAppendEntries(i, args, reply)
			if !ok {
				return
			}
			//收到一个新leader的响应，这里务必记得加锁，不然会导致CANDIDATE变为FOLLOWER的信息无法及时同步；可以试试把锁删除，看看效果
			rf.mu.Lock()
			defer rf.mu.Unlock()

			if reply.Term > rf.currentTerm {
				rf.state = FOLLOWER
				rf.voterFor = -1
				rf.currentTerm = reply.Term
				rf.refreshExpireTime()
			}
		}(i)

	}
}
