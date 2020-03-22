package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"sort"
)
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
			//log.Println("worker exist")
			break
		}
		//获取task结构体信息
		task := &reply.Task

		if task.Phase == TASK_PHASE_MAP {
			mapTask := task.MapTask
			Map(mapTask.FileName, mapTask.MapIndex, mapTask.ReduceNumber, mapf)
		} else if task.Phase == TASK_PHASE_REDUCE {
			reduceTask := task.ReduceTask
			Reduce(reduceTask.MapNumber, reduceTask.ReduceIndex, reducef)
		}
		args.CompleteTask = *task
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
	call("Master.Example1", &args, &reply)
	//log.Println("ok: ", ok)

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

//默认map保存到一个地方，而reduce从同一个地方读取（//实际场景这是不可能的，需要将map的保存路径发送给master）

//定义临时文件名
func genIntermediateFileName(mapIndex int, reduceIndex int) string {
	return fmt.Sprintf("mr-%d-%d", mapIndex, reduceIndex)
}

func genOutFileName(reduceIndex int) string {
	return fmt.Sprintf("mr-out-%d", reduceIndex)
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

type Bucket []KeyValue

//定义Map与Reduce的通用方法
func Map(fileName string, mapIndex int, reduceNumber int, mapf func(string, string)[]KeyValue) {
	//from mrsequential.go
	buckets := make([]Bucket, reduceNumber)

	file,err := os.Open(fileName)
	if err != nil {
		log.Fatalf("cannot open %v", fileName)
	}
	content, err := ioutil.ReadAll(file)
	file.Close()
	//执行map计算
	kva := mapf(fileName, string(content))
	//将结果进行分桶放置，每个桶对应一个reduce的读取位置
	for _, item := range kva {
		index := ihash(item.Key) % reduceNumber
		buckets[index] = append(buckets[index], item)
	}
	for reduceIndex, bucket := range buckets {
		//将内容写到临时文件
		//file, err := ioutil.TempFile("./tmp", "map-temp")
		file, err := ioutil.TempFile("", "map-temp")
		if err != nil {
			log.Fatalf("create temp file failed %v", err)
		}
		enc := json.NewEncoder(file)
		for _, kv := range bucket {
			//log.Println("bucket    ", kv)
			err := enc.Encode(&kv)
			if err != nil {
				log.Fatalf("json encode file err %v", err)
			}
		}
		//获取文件名保存
		filename := genIntermediateFileName(mapIndex, reduceIndex)
		//err = os.Rename(file.Name(), "./tmp/"+filename)
		err = os.Rename(file.Name(), filename)
		if err != nil {
			log.Fatalf("rename faile, %v", err)
		}
		//log.Println("fileName: ", file.Name())
	}

}

//根据reduceIndex，读取该 reduce task所有相关的临时文件，然后执行reduce任务，保存到genoutfile()文件
func Reduce(mapNumber int, reduceIndex int, reducef func(string, []string) string) {
	//读取全部的临时文件内容
	intermediate := make([]KeyValue, 0)
	for i := 0; i < mapNumber; i++ {
		//fileName := "./tmp/" + genIntermediateFileName(i, reduceIndex)
		fileName := genIntermediateFileName(i, reduceIndex)
		file, err := os.Open(fileName)
		if err != nil {
			log.Fatalf("cannot open %v", fileName)
		}
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			intermediate = append(intermediate, kv)
		}
		file.Close()
	}
	//进行排序
	sort.Sort(ByKey(intermediate))
	outfileName := genOutFileName(reduceIndex)
	//f, _ := ioutil.TempFile("./tmp", "reduce_temp")
	f, _ := ioutil.TempFile("", "reduce_temp")
	//进行统计
	i := 0
	for i < len(intermediate) {
		//生成 ( key, list() ) 列表
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		//处理每个键值
		output := reducef(intermediate[i].Key, values)

		//log.Println("reduce ", intermediate[i].Key, " ", output)
		//reduce output
		fmt.Fprintf(f, "%v %v\n", intermediate[i].Key, output)

		i = j
	}
	f.Close()
	//os.Rename(f.Name(), "./tmp/" + outfileName)
	os.Rename(f.Name(),  outfileName)
	//log.Println("reduce output file: ", outfileName)
}