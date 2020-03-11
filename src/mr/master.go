package mr

import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"


const (
	SCHEDULE_TASK_SUCCESS = "success"
	SCHEDULE_TASK_FAILED = "fail" //调度不成功，
	SCHEDULE_TASK_DONE = "done" //已经全部调度完毕
)

type Master struct {
	// Your definitions here.
	files []string

	//m个map和n个reduce
	nReduce int
	mMap int

	//管理字段,通道用于通信/等待
	mapIndexChan chan int //map 任务下标的通道
	reduceIndexChan chan int
	//用于保存正在运行的任务，
	runningMapTask map[int]int64
	runningReduceTask map[int]int64
}


func (m *Master) AskForTask(args *AskForTaskArgs, reply *AskForTaskReply) error {

	//判断该worker有没有上次执行的complete task，并执行
	m.finishTask(args.CompleteTask)

	//进行任务分配
	for {
		task, result := m.scheduleTask()
	}

	return nil
}

//分配任务
func (m *Master) scheduleTask() (*Task, string) {
	//先分配map task，保证全部的map task都分配出去了并完成了，才进入reduce的环节
	select {
	case mapIndex := <-m.mapIndexChan:
		task := &Task{
			Phase:      TASK_PHASE_MAP,
			MapTask:    MapTask{
				FileName: m.files[mapIndex],
				MapIndex: mapIndex,
				ReduceNumber: m.nReduce,
			},
		}
		return task, SCHEDULE_TASK_SUCCESS
	default:
		//表示任务已经分配完毕，但可能还有正在运行的任务
		if len(m.runningReduceTask) > 0 {
			return nil, SCHEDULE_TASK_FAILED
		}
	}

	select {
	case reduceIndex := <-m.reduceIndexChan:
		task := &Task{
			Phase:      TASK_PHASE_REDUCE,
			ReduceTask: ReduceTask{
				MapNumber: m.mMap,
				ReduceIndex: reduceIndex,
			},
		}
		return task, SCHEDULE_TASK_SUCCESS
	default:
		if len(m.runningReduceTask) > 0 {
			return nil, SCHEDULE_TASK_FAILED
		}
	}

	return nil, SCHEDULE_TASK_DONE
}

//处理已经处理成功的任务
func (m *Master) finishTask(task Task) {
	switch task.Phase {
	case TASK_PHASE_MAP:
		//判断是否在running里面
		if _, ok := m.runningMapTask[task.MapTask.MapIndex]; !ok {
			//可能已经被其他worker处理了,因为有超时重试的机制
			return
		}
		delete(m.runningMapTask, task.MapTask.MapIndex)

	case TASK_PHASE_REDUCE:
		if _, ok := m.runningReduceTask[task.ReduceTask.ReduceIndex]; !ok {
			return
		}
		delete(m.runningReduceTask, task.ReduceTask.ReduceIndex)
	}
}

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}


//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	ret := false

	// Your code here.


	return ret
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}

	// Your code here.
	m.files = files

	m.mMap = len(files)
	m.nReduce = nReduce

	m.mapIndexChan = make(chan int, m.mMap)
	m.reduceIndexChan = make(chan int, m.nReduce)
	m.runningMapTask = make(map[int]int64, 0)
	m.runningReduceTask = make(map[int]int64, 0)

	//启动任务通讯
	for i := 0; i < m.mMap; i++ {
		m.mapIndexChan <- i //每个值代表一个任务序号
	}

	for i := 0; i < m.nReduce; i++ {
		m.reduceIndexChan <- i
	}

	m.server()
	return &m
}
