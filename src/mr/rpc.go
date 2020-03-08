package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

type MapTask struct {
	FileName string
	MapIndex int //map任务的下标
	ReduceNumber int //reduce的数量，也就是nReduce
}

type ReduceTask struct {
	MapNumber int
	ReduceIndex int
}

type Task struct {
	Phase string
	MapTask MapTask
	ReduceTask ReduceTask
}

const (
	TASK_PHASE_MAP = "map"
	TASK_PHASE_REDUCE = "reduce"
)

// Add your RPC definitions here.
type AskForTaskArgs struct {
	CompleteTask Task //上一次执行的任务，用于向master反馈该任务已经完成
}

type AskForTaskReply struct {
	Done bool //是否已经结束，没有新的task
	Task Task

}


// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the master.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func masterSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
