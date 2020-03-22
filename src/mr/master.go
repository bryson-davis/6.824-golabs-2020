package mr

import (
	"log"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"


const (
	SCHEDULE_TASK_SUCCESS = "success"
	SCHEDULE_TASK_NOAVAILABLE = "noavailable" //调度不成功，
	SCHEDULE_TASK_DONE = "done" //已经全部调度完毕
)

type Master struct {
	mu sync.Mutex
	cond *sync.Cond //https://cyent.github.io/golang/goroutine/sync_cond/

	files []string

	//m个map和n个reduce
	nReduce int
	mMap int
	//记录已经完成的个数
	completedMapTask int
	completedReduceTask int

	//管理字段,通道用于通信/等待
	mapIndexChan chan int //map 任务下标的通道
	reduceIndexChan chan int
	//用于保存正在运行的任务，key是map地址，value是启动时间
	runningMapTask map[int]int64
	runningReduceTask map[int]int64
}


func (m *Master) AskForTask(args *AskForTaskArgs, reply *AskForTaskReply) error {
	//log.Println(args.Pid)
	m.cond.L.Lock()
	defer m.cond.L.Unlock()

	//判断该worker有没有上次执行的complete task，并执行
	m.finishTask(args.CompleteTask)

	//进行任务分配
	//worker在这里需要等待,sync.Cond/time.Sleep
	for {
		var result string
		task, result := m.scheduleTask()
		switch result {
		case SCHEDULE_TASK_SUCCESS:
			reply.Task = *task
			reply.Done = false
			return nil
		case SCHEDULE_TASK_NOAVAILABLE:
			//需要等待
			m.cond.Wait()
		case SCHEDULE_TASK_DONE:
			reply.Done = true
			return nil
		}
	}
}

//分配任务
func (m *Master) scheduleTask() (*Task, string) {
	//先分配map task，保证全部的map task都分配出去了并完成了，才进入reduce的环节
	now := time.Now().Unix()
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
		m.runningMapTask[mapIndex] = now
		return task, SCHEDULE_TASK_SUCCESS
	default:
		//表示任务已经分配完毕，但可能还有正在运行的任务
		if len(m.runningMapTask) > 0 {
			return nil, SCHEDULE_TASK_NOAVAILABLE
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
		m.runningReduceTask[reduceIndex] = now
		return task, SCHEDULE_TASK_SUCCESS
	default:
		if len(m.runningReduceTask) > 0 {
			return nil, SCHEDULE_TASK_NOAVAILABLE
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
		m.completedMapTask += 1
		m.cond.Broadcast() //in order to avoid askfortask wait forever (for reduce paralissm pass)
	case TASK_PHASE_REDUCE:
		if _, ok := m.runningReduceTask[task.ReduceTask.ReduceIndex]; !ok {
			return
		}
		delete(m.runningReduceTask, task.ReduceTask.ReduceIndex)
		m.completedReduceTask += 1
		m.cond.Broadcast() //in order to avoid askfortask wait forever (for reduce paralissm pass)
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
	m.cond.L.Lock()
	defer m.cond.L.Unlock()
	ret := m.completedMapTask == m.mMap && m.completedReduceTask == m.nReduce
	return ret
}

//每10s检查下已经发送的任务是否已经完成，没有完成的话，需要重新发送给其他的worker执行
func (m *Master) taskChecker() {
	//轮询
	const TIMEOUT = 10
	for {
		if m.Done() == true {
			return
		}
		m.cond.L.Lock()
		reissue := false
		now := time.Now().Unix()
		for mapIndex, startTime  := range m.runningMapTask {
			if startTime + TIMEOUT < now {
				delete(m.runningMapTask, mapIndex)
				m.mapIndexChan <- mapIndex //reschedule
				reissue = true
			}
		}
		for reduceIndex, startTime := range m.runningReduceTask {
			if startTime + TIMEOUT < now {
				delete(m.runningReduceTask, reduceIndex)
				m.reduceIndexChan <- reduceIndex //reschedule
				reissue = true
			}
		}
		if reissue {
			m.cond.Broadcast()
		}
		m.cond.L.Unlock()
		time.Sleep(time.Second)
	}
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}

	m.cond = sync.NewCond(&m.mu)
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

	//log.Println("master start: ", m)

	go m.taskChecker()

	m.server()
	return &m
}
