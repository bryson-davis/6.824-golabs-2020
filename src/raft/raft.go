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
	"math/rand"
	"os"
	"sync"
	"time"
)
import "sync/atomic"
//import "../labrpc"
import "labrpc"

import "bytes"
//import "../labgob"
import "labgob"



//
// as each Raft peer becomes aware that successive log entries are
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
	DoSnapshot bool
}

func randTimeout(lower int,upper int) int {
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	diff := upper - lower
	return lower + r.Intn(diff)
}

const (
	LEADER int = iota
	CANDIDATE
	FOLLOWER
)

type LogEntry struct {
	Term int
	Index int
	Command interface{}
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

	CurrentTerm int
	VotedFor    int
	commitIndex int
	lastApplied int
	nextIndex 	[]int
	matchIndex 	[]int
	role 		int
	lastHeartBeatReceived time.Time
	applyCh chan ApplyMsg
	Logs []LogEntry
	Applycond *sync.Cond

	LastIncludeIndex int
	LastIncludeTerm int
	isInstallingSnapshot []int
	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
}

func (rf* Raft) TruncatePrefixLog(index int) {
	i := len(rf.Logs) - 1
	for ; i >= 0 ; i-- {
		if rf.Logs[i].Index == index {
			break
		}
	}
	if i == len(rf.Logs) - 1 {
		entry := rf.Logs[i]
		rf.Logs = []LogEntry{}
		rf.Logs = append(rf.Logs, entry)
	} else if i < 0 {

	} else {
		rf.Logs = rf.Logs[i:]
	}
}

func (rf* Raft) FirstEntryWithTerm(term int) int {
	findTerm := false
	index := -1
	for i := len(rf.Logs) - 1; i >=0 ; i-- {
		if rf.Logs[i].Term != term && findTerm {
			index = rf.Logs[i + 1].Index
			break
		}
		if rf.Logs[i].Term == term {
			findTerm = true
		}
	}
	return  index
}

func (rf* Raft) LastEntryWithTerm(term int) int {
	index := -1
	for i := len(rf.Logs) - 1; i >= 0; i-- {
		if rf.Logs[i].Term < term {
			break
		}
		if rf.Logs[i].Term == term {
			index = rf.Logs[i].Index
			break
		}
	}
	return  index
}

// return CurrentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	{
		rf.mu.Lock()
		term = int(rf.CurrentTerm)
		if rf.role == LEADER{
			isleader = true
		} else {
			isleader = false
		}
		rf.mu.Unlock()
	}
	// Your code here (2A).
	return term, isleader
}
func (rf* Raft) Role() int {
	var role int
	rf.mu.Lock()
	role = rf.role
	rf.mu.Unlock()
	return role
}

func (rf* Raft) TermForLog(index int) int {
	entry,ok := rf.GetLogAtIndexWithLock(index)
	if ok {
		return entry.Term
	} else {
		return -1
	}
}

func (rf* Raft) GetLogAtIndexWithLock(index int) (LogEntry,bool) {
	if index == 0  {
		return LogEntry{},true
	} else if len(rf.Logs) == 0 {
		return LogEntry{},false
	} else if (index < rf.Logs[0].Index) || (index > rf.Logs[len(rf.Logs) - 1].Index) {
		return LogEntry{},false
	} else  {
		localIndex := index - rf.Logs[0].Index
		return rf.Logs[localIndex],true
	}
}
func (rf* Raft) GetLogAtIndex(index int) (LogEntry,bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.GetLogAtIndexWithLock(index)
}

func (rf* Raft) GetLastLogEntryWithLock() LogEntry {
	entry := LogEntry{}
	if len(rf.Logs) == 0 {
		entry.Term = 0
		entry.Index = 0
	} else {
		entry = rf.Logs[len(rf.Logs) - 1]
	}
	return  entry
}

func (rf* Raft) GetLastLogEntry() LogEntry {
	entry := LogEntry{}
	rf.mu.Lock()
	entry = rf.GetLastLogEntryWithLock()
	rf.mu.Unlock()
	return  entry
}
func (rf* Raft) LastHeartBeatReceived() time.Time  {
	var time time.Time
	rf.mu.Lock()
	time = rf.lastHeartBeatReceived
	rf.mu.Unlock()
	return time
}

func (rf* Raft) setLastHeartBeatReceived(receivedTime time.Time) {
	rf.mu.Lock()
	rf.lastHeartBeatReceived = receivedTime
	rf.mu.Unlock()
}

func (rf* Raft) becomeLeader() {
	lastLogIndex := 0
	rf.mu.Lock()
	//go rf.heartBeatTimer()
	rf.role = LEADER
	lastLogIndex = rf.GetLastLogEntryWithLock().Index
	for i := 0; i < len(rf.peers); i++ {
		rf.nextIndex[i] = lastLogIndex + 1
		rf.matchIndex[i] = 0
	}

	//go rf.heartBeatTimer()
	rf.mu.Unlock()
	go rf.heartBeatTimer()
	DPrintf("%v become leader in term %v \n",rf.me,rf.CurrentTerm)
}

func (rf* Raft) becomeFollowerWithLock() {
	if rf.role == LEADER {
		rf.role = FOLLOWER
		go rf.electionTimer()
	} else if rf.role == CANDIDATE {
		rf.role = FOLLOWER
	}
}
//for candidate and follower only
func (rf* Raft) electionTimer() {
	for rf.Role() != LEADER {
		interval := randTimeout(300,600)
		time.Sleep(time.Millisecond * time.Duration(interval))
		role := rf.Role()
		if role == FOLLOWER {
			diff := time.Since(rf.LastHeartBeatReceived())
			if diff < time.Duration(interval) * time.Millisecond {
				continue
			} else {
				rf.handleElectionTimeout()
				return
			}
		} else if role == CANDIDATE {
			rf.handleElectionTimeout()
			return
		} else {
			return
		}

	}
	//DPrintf("%v end election timer \n",rf.me)
}

//for leader only
func (rf* Raft) heartBeatTimer() {
	//send heart per 150 milseconds
	for rf.Role() == LEADER {
		time.Sleep(time.Millisecond * 150)
		for i := 0; i < len(rf.peers); i++ {
			if i == rf.me {
				continue
			}
			go rf.RequestAppendNewEntries(i,true)
		}
		//time.Sleep(time.Millisecond * 150)
	}
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
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.CurrentTerm)
	e.Encode(rf.VotedFor)
	e.Encode(rf.Logs)
	e.Encode(rf.LastIncludeIndex)
	e.Encode(rf.LastIncludeTerm)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)

}

//for leader only
func (rf* Raft) SaveStateAndSnapShot(snapshot []byte,lastIndex int) {
	rf.mu.Lock()
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	rf.TruncatePrefixLog(lastIndex)
	term := rf.TermForLog(lastIndex)
	rf.LastIncludeIndex = lastIndex
	rf.LastIncludeTerm = term
	e.Encode(rf.CurrentTerm)
	e.Encode(rf.VotedFor)
	e.Encode(rf.Logs)
	e.Encode(rf.LastIncludeIndex)
	e.Encode(rf.LastIncludeTerm)
	data := w.Bytes()
	rf.mu.Unlock()
	rf.persister.SaveStateAndSnapshot(data,snapshot)
}

func (rf* Raft) SaveStateAndSnapShotWithLock(args* InstallSnapshotRequest) {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	rf.LastIncludeIndex = args.LastIncludeIndex
	rf.LastIncludeTerm = args.LastIncludeTerm
	e.Encode(rf.CurrentTerm)
	e.Encode(rf.VotedFor)
	e.Encode(rf.Logs)
	e.Encode(rf.LastIncludeIndex)
	e.Encode(rf.LastIncludeTerm)
	data := w.Bytes()
	rf.persister.SaveStateAndSnapshot(data,args.Data)
}

func (rf* Raft) RaftStateSize() int {
	return rf.persister.RaftStateSize()
}

func (rf* Raft) ReadSnapshot() ([]byte ,int) {
	data := rf.persister.ReadSnapshot()
	rf.mu.Lock()
	index := rf.LastIncludeIndex
	rf.mu.Unlock()
	return data,index
}
func (rf* Raft) ReadSnapshotWithLock() ([]byte ,int) {
	data := rf.persister.ReadSnapshot()
	index := rf.LastIncludeIndex
	return data,index
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
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var CurrentTerm int
	var VotedFor int
	var Logs []LogEntry
	var LastIncludeIndex int
	var LastIncludeTerm int
	d.Decode(&CurrentTerm)
	d.Decode(&VotedFor)
	d.Decode(&Logs)
	d.Decode(&LastIncludeIndex)
	d.Decode(&LastIncludeTerm)
	rf.CurrentTerm = CurrentTerm
	rf.VotedFor = VotedFor
	rf.Logs = Logs
	rf.LastIncludeIndex = LastIncludeIndex
	rf.LastIncludeTerm = LastIncludeTerm
}





//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term int
	CandidateId int
	LastLogIndex int
	LastLogTerm int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term int
	VoteGranted bool
}
type AppendEntriesRequest struct {
	Src int
	Term int
	PrevLogIndex int
	PrevLogTerm int
	LeaderCommit int
	Entries []LogEntry
}
type AppendEntriesResponse struct {
	Term int
	Success bool
	XTerm int
	XIndex int
	LogLen int
}

type InstallSnapshotRequest struct {
	Term int
	Src int
	LastIncludeIndex int
	LastIncludeTerm int
	Data []byte
}

type InstallSnapshotResponse struct {
	Term int
}
//
// example RequestVote RPC handler.
//
func (rf* Raft) updateTerm(term int) {
	rf.mu.Lock()
	if term > rf.CurrentTerm {
		rf.CurrentTerm = term
		rf.VotedFor = -1
		rf.becomeFollowerWithLock()
		rf.persist()
	}
	rf.mu.Unlock()
}
func (rf* Raft) handleRequestVoteResponse(request RequestVoteArgs,reply RequestVoteReply) bool {
	rf.updateTerm(reply.Term)
	rf.mu.Lock()
	if rf.CurrentTerm != request.Term {
		rf.mu.Unlock()
		return false
	}
	granted := reply.VoteGranted
	rf.mu.Unlock()
	return granted
}
//for candidate and follower requestvote
func (rf* Raft) handleElectionTimeout() {
	finished := 0
	granted := 0
	reply_ := RequestVoteReply{}
	request_ := RequestVoteArgs{}
	cond := sync.NewCond(&rf.mu)
	rf.mu.Lock()
	rf.CurrentTerm++
	rf.VotedFor = rf.me
	rf.role = CANDIDATE
	rf.persist()
	rf.lastHeartBeatReceived = time.Now()
	granted++
	finished++
	request_.CandidateId = rf.me
	request_.Term = rf.CurrentTerm
	entry := rf.GetLastLogEntryWithLock()
	request_.LastLogIndex = entry.Index
	request_.LastLogTerm = entry.Term
	rf.mu.Unlock()
	go rf.electionTimer()
	//DPrintf("%v is requesting vote in term %v \n",rf.me,request_.Term)
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		go func(index int,request RequestVoteArgs,reply RequestVoteReply) {
			//sendrequestvote
			ok := rf.sendRequestVote(index,&request,&reply)
			if ok {
				ok = rf.handleRequestVoteResponse(request,reply)
			}
			rf.mu.Lock()
			finished++
			if ok {
				granted++
				DPrintf("%v granted by %v \n",rf.me,index)
			}
			cond.Signal()
			rf.mu.Unlock()
		}(i,request_,reply_)
	}

	rf.mu.Lock()
	for finished != len(rf.peers) && granted < len(rf.peers) / 2 + 1 {
		cond.Wait()
	}
	rf.mu.Unlock()
	term,_ := rf.GetState()
	if request_.Term == term && granted >= len(rf.peers) / 2 + 1 && rf.Role() == CANDIDATE {
		rf.becomeLeader()
	}  else {
		DPrintf("%v fail in request vote in term %v \n",rf.me,request_.Term)
	}

}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.updateTerm(args.Term)
	rf.mu.Lock()
	reply.Term = rf.CurrentTerm
	reply.VoteGranted = false
	if args.Term < rf.CurrentTerm {
		reply.VoteGranted = false
	} else if rf.VotedFor == -1 || rf.VotedFor == args.CandidateId {
		DPrintf("%v received voteRequest from %v LogIndex : %v %v ",rf.me,args.CandidateId,args.LastLogIndex,args.LastLogTerm)
		entry := rf.GetLastLogEntryWithLock()
		logok := (args.LastLogTerm > entry.Term) ||
			(args.LastLogTerm == entry.Term && args.LastLogIndex >= entry.Index)
		if logok {
			reply.VoteGranted = true
			rf.VotedFor = args.CandidateId
			rf.lastHeartBeatReceived = time.Now()
			rf.persist()
			DPrintf("%v:%v vote for %v:%v\n",rf.me,rf.CurrentTerm,args.CandidateId,args.Term)
		} else {
			//DPrintf("%v reject vote for %v --- log not ok \n",rf.me,args.CandidateId)
			reply.VoteGranted = false
		}
	} else {
		reply.VoteGranted = false
		DPrintf("%v reject vote for %v --- has voted before \n",rf.me,args.CandidateId)
	}
	rf.mu.Unlock()
}

// when LastLogEntry.Index >= nextIndex send new entries
func (rf* Raft) RequestAppendNewEntries(peerIndex int,isHeartBeat bool) {
	var entries []LogEntry
	if rf.Role() == LEADER {
		rf.mu.Lock()
		if rf.isInstallingSnapshot[peerIndex] == 1 {
			rf.mu.Unlock()
			return
		}
		for i := rf.nextIndex[peerIndex] ; i <= rf.GetLastLogEntryWithLock().Index; i++ {
			entry,find := rf.GetLogAtIndexWithLock(i)
			if !find {
				entries = []LogEntry{}
				//fmt.Printf("could not find entry when request appendEntry %v %v %v",rf.nextIndex[peerIndex],rf.LastIncludeIndex,rf.Logs)
				//os.Exit(-1)
				break
			}
			entries = append(entries, entry)
		}

		if len(entries) == 0 && isHeartBeat == false{
			rf.mu.Unlock()
			return
		}
		request := AppendEntriesRequest{}
		reply := AppendEntriesResponse{}
		request.Entries = entries
		request.Term = rf.CurrentTerm
		request.Src = rf.me
		request.PrevLogIndex = rf.nextIndex[peerIndex] - 1
		request.LeaderCommit = rf.commitIndex
		request.PrevLogTerm = rf.TermForLog(request.PrevLogIndex)
		rf.mu.Unlock()
		ok := rf.sendAppendEntries(peerIndex,&request,&reply)
		if ok {
			isContinue := rf.handleAppendEntriesResponse(request,reply,peerIndex)
			if isContinue {
				go rf.RequestAppendNewEntries(peerIndex,false)
			}
		}
	}

}

func (rf* Raft) AppendEntries(args *AppendEntriesRequest, reply *AppendEntriesResponse) {
	term := args.Term
	reply.Success = false
	reply.LogLen = -1
	reply.XTerm = -1
	reply.XIndex = -1
	isUpdateCommit := false
	rf.updateTerm(term)
	rf.mu.Lock()
	if term == rf.CurrentTerm {
		rf.lastHeartBeatReceived = time.Now()
		if rf.role == CANDIDATE {
			rf.becomeFollowerWithLock()
		}
		entry ,find := rf.GetLogAtIndexWithLock(args.PrevLogIndex)
		//logOk := args.PrevLogIndex == 0 ||
		//	(args.PrevLogIndex <= len(rf.Logs) && rf.Logs[args.PrevLogIndex - 1].Term == args.PrevLogTerm)
		logOk := args.PrevLogIndex == 0 ||
			(find && entry.Term == args.PrevLogTerm)
		if !logOk {
			if !find {
				reply.LogLen = len(rf.Logs)
				if rf.LastIncludeIndex != -1 {
					reply.LogLen = len(rf.Logs) + rf.LastIncludeIndex
				}
			} else {
				reply.XTerm = entry.Term
				reply.XIndex = rf.FirstEntryWithTerm(reply.XTerm)
			}
		}
		if logOk {
			reply.Success = true
			isconflicts := false
			conflictIndex := -1
			for _, entry := range args.Entries {
				LocalEntry,hasFind := rf.GetLogAtIndexWithLock(entry.Index)
				if hasFind {
					//If an existing entry conflicts with a new one
					if entry.Term != LocalEntry.Term{
						DPrintf("%v received conflicted Logs in %v\n",rf.me,entry.Index)
						isconflicts = true
						conflictIndex = LocalEntry.Index - rf.Logs[0].Index //localIndex
						break
					}
				} else {
					rf.Logs = append(rf.Logs, entry)
				}
			}
			if isconflicts {
				rf.Logs = rf.Logs[:conflictIndex]
				beginIndex := rf.GetLastLogEntryWithLock().Index + 1
				for _, entry := range args.Entries {
					if entry.Index >= beginIndex {
						rf.Logs = append(rf.Logs, entry)

					}
				}
			}
			rf.persist()
			if args.LeaderCommit > rf.commitIndex {
				isUpdateCommit = true
				if rf.GetLastLogEntryWithLock().Index < args.LeaderCommit {
					rf.commitIndex = rf.GetLastLogEntryWithLock().Index
				} else {
					rf.commitIndex = args.LeaderCommit
				}
			}
		}
	}
	reply.Term = rf.CurrentTerm
	if isUpdateCommit {
		rf.Applycond.Signal()
	}
	rf.mu.Unlock()
}
//return true if we should continue send entries
func (rf* Raft) handleAppendEntriesResponse(request AppendEntriesRequest,reply AppendEntriesResponse,peerIndex int) bool {
	rf.updateTerm(reply.Term)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if request.Term != rf.CurrentTerm  {
		//now we are not leader
		return false
	}
	isContinue := false
	isUpdated := false
	//update matchedIndex ,nextIndex and commitIndex
	if reply.Success {
		rf.matchIndex[peerIndex] = len(request.Entries) + request.PrevLogIndex
		rf.nextIndex[peerIndex] = rf.matchIndex[peerIndex] + 1
		isUpdated = rf.updateCommitForLeader()
	} else {
		if reply.LogLen != -1 {
			rf.nextIndex[peerIndex] = reply.LogLen
		} else {
			index := rf.LastEntryWithTerm(reply.XTerm)
			if index == -1 {
				rf.nextIndex[peerIndex] = reply.XIndex
			} else {
				rf.nextIndex[peerIndex] = index
			}
		}
		if rf.nextIndex[peerIndex] < 1 {
			rf.nextIndex[peerIndex] = 1
		}
		if rf.nextIndex[peerIndex] < rf.LastIncludeIndex {
			go rf.RequestInstallSnapshot(peerIndex)
			return  false
		}
		isContinue = true
	}
	if isUpdated {
		rf.Applycond.Signal()
	}
	return isContinue
}

func (rf* Raft) RequestInstallSnapshot(peerIndex int) {
	rf.mu.Lock()
	if rf.isInstallingSnapshot[peerIndex] == 0 {
		rf.isInstallingSnapshot[peerIndex] = 1
	} else {
		rf.mu.Unlock()
		return
	}
	data,_ := rf.ReadSnapshotWithLock()
	request := InstallSnapshotRequest{rf.CurrentTerm,rf.me,rf.LastIncludeIndex,rf.LastIncludeTerm,data}
	reply := InstallSnapshotResponse{}
	rf.mu.Unlock()
	ok := rf.sendInstallSnapshot(peerIndex,&request,&reply)
	if ok {
		rf.handleInstallSnapshotResponse(peerIndex,&request,&reply)
	} else {
		rf.mu.Lock()
		rf.isInstallingSnapshot[peerIndex] = 0
		rf.mu.Unlock()
	}
}

func (rf* Raft) InstallSnapshot(args *InstallSnapshotRequest, reply *InstallSnapshotResponse) {
	rf.updateTerm(args.Term)
	rf.mu.Lock()
	reply.Term = rf.CurrentTerm
	if rf.CurrentTerm == args.Term {
		rf.lastHeartBeatReceived = time.Now()
		_,find := rf.GetLogAtIndexWithLock(args.LastIncludeIndex)
		if find {
			rf.TruncatePrefixLog(args.LastIncludeIndex)
			rf.SaveStateAndSnapShotWithLock(args)
			rf.mu.Unlock()
			return
		} else {
			rf.Logs = []LogEntry{}
			rf.Logs = append(rf.Logs, LogEntry{args.LastIncludeTerm,args.LastIncludeIndex,nil})
			rf.SaveStateAndSnapShotWithLock(args)
		}
		rf.commitIndex = args.LastIncludeIndex
		rf.Applycond.Signal()
		//applyMsg := ApplyMsg{true,nil,-1,true}
		//rf.lastApplied = args.LastIncludeIndex
		//fmt.Printf("update %v ",args.LastIncludeIndex)
		//rf.mu.Unlock()
		//rf.applyCh <- applyMsg
		//rf.mu.Lock()
	}
	rf.mu.Unlock()
}

func (rf* Raft) handleInstallSnapshotResponse(peerIndex int,args *InstallSnapshotRequest, reply *InstallSnapshotResponse) {
	rf.updateTerm(reply.Term)
	rf.mu.Lock()
	if rf.CurrentTerm == args.Term {
		rf.isInstallingSnapshot[peerIndex] = 0
		rf.nextIndex[peerIndex] = args.LastIncludeIndex + 1
		rf.matchIndex[peerIndex] = args.LastIncludeIndex
	}
	rf.mu.Unlock()
	go rf.RequestAppendNewEntries(peerIndex,false)
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

func (rf* Raft) sendAppendEntries(server int, args *AppendEntriesRequest, reply *AppendEntriesResponse) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}
func (rf* Raft) sendInstallSnapshot(server int, args *InstallSnapshotRequest, reply *InstallSnapshotResponse) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
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
//rf.Logs[Entry(1)][Entry[2].....[Entry(n)]
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).
	rf.mu.Lock()
	if rf.role == LEADER {
		isLeader = true
		entry := LogEntry{}
		entry.Term = rf.CurrentTerm
		entry.Index = rf.GetLastLogEntryWithLock().Index + 1
		entry.Command = command
		index = entry.Index
		term = entry.Term
		rf.Logs = append(rf.Logs, entry)
		rf.persist()
		for i := 0; i < len(rf.peers); i++ {
			if i == rf.me {
				continue
			}
			go rf.RequestAppendNewEntries(i,false)
		}
	} else {
		isLeader = false
	}
	rf.mu.Unlock()
	return index, term, isLeader
}
//return true if update success
func (rf* Raft) updateCommitForLeader() bool {
	beginIndex := rf.commitIndex + 1
	lastCommittedIndex := -1
	updated := false
	for ; beginIndex <= rf.GetLastLogEntryWithLock().Index; beginIndex++ {
		granted := 1
		for peerIndex := 0; peerIndex < len(rf.matchIndex); peerIndex++ {
			if peerIndex == rf.me {
				continue
			}
			if rf.matchIndex[peerIndex] >= beginIndex {
				granted++
			}
		}

		if granted >= len(rf.peers) / 2 + 1 && (rf.TermForLog(beginIndex) == rf.CurrentTerm) {
			lastCommittedIndex = beginIndex
		}
	}
	if lastCommittedIndex > rf.commitIndex {
		rf.commitIndex = lastCommittedIndex
		updated = true
	}
	return  updated
}

func (rf* Raft) doCommit() {
	for  {
		rf.mu.Lock()
		for rf.commitIndex <= rf.lastApplied  {
			rf.Applycond.Wait()
		}
		if rf.lastApplied < rf.LastIncludeIndex {
			//fmt.Printf("begin apply snapshot")
			applyMsg := ApplyMsg{true,nil,-1,true}
			rf.mu.Unlock()
			rf.applyCh <- applyMsg
			rf.mu.Lock()
			rf.lastApplied = rf.LastIncludeIndex
			rf.mu.Unlock()
			continue
		}

		i := rf.lastApplied + 1
		for ; i <= rf.commitIndex; i++ {
			entry ,find := rf.GetLogAtIndexWithLock(i)
			if !find || entry.Index == rf.LastIncludeIndex {
				continue
			}
			msg := ApplyMsg{true,entry.Command,i,false}
			if msg.Command == nil {
				//fmt.Printf("%v %v %v %v %v",rf.LastIncludeIndex,rf.lastApplied,rf.commitIndex,i,rf.Logs)
				os.Exit(-1)
			}
			rf.mu.Unlock()
			rf.applyCh <- msg
			rf.mu.Lock()
			rf.lastApplied = i
		}
		//DPrintf("server-%d log: %v", rf.me, rf.Logs)
		rf.mu.Unlock()
	}
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
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.mu = sync.Mutex{}
	rf.Applycond = sync.NewCond(&rf.mu)

	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.CurrentTerm = 0
	rf.VotedFor = -1
	rf.applyCh = applyCh
	rf.role = FOLLOWER
	rf.lastHeartBeatReceived = time.Now()
	for i := 0; i < len(peers); i++ {
		rf.nextIndex = append(rf.nextIndex, 1)
		rf.matchIndex = append(rf.matchIndex, 0)
		rf.isInstallingSnapshot = append(rf.isInstallingSnapshot, 0)
	}
	rf.LastIncludeIndex = -1
	rf.LastIncludeTerm = -1
	rf.lastHeartBeatReceived = time.Now()
	go rf.electionTimer()
	go rf.doCommit()
	// Your initialization code here (2A, 2B, 2C).

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	return rf
}