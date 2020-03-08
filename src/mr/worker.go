package mr

import "fmt"
import "log"
import "net/rpc"
import "hash/fnv"


//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}


//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	//进入循环中，不断从向master发送rpc请求，获取任务
	//构建请求，一直会用
	args := &AskForTaskArgs{}
	for {
		//调用rpc（将上一次的请求结果放置到请求中）
		reply, ok := AskForTask(args)
		//根据响应的task信息，执行map或者reduce，或者退出
		if !ok || reply.Done {
			break
		}
		//获取task结构体信息
		task := &reply.Task
		args.CompleteTask = *task
		if task.Phase == TASK_PHASE_MAP {
			mapTask := task.MapTask
			Map(mapTask.FileName, mapTask.MapIndex, mapTask.ReduceNumber, mapf)
		} else if task.Phase == TASK_PHASE_REDUCE {
			reduceTask := task.ReduceTask
			Reduce(reduceTask.MapNumber, reduceTask.ReduceIndex, reducef)
		}
	}

}

//
// example function to show how to make an RPC call to the master.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	call("Master.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

//传入一个已经有的args，用于反馈上一个任务已经完成
func AskForTask(args *AskForTaskArgs) (*AskForTaskReply, bool) {
	reply := &AskForTaskReply{}
	ok := call("Master.AskForTask", args, reply)
	return reply, ok
}

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}

//定义Map与Reduce的通用方法
func Map(fileName string, mapIndex int, reduceNumber, mapf func(string, string)[]KeyValue) {

}

func Reduce(mapNumber int, reduceIndex int, reducef func(string, string) []KeyValue) {
	
}