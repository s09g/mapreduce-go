package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
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

	// Your worker implementation here.
	//6. 获得task之后交给对应的worker job
	log.Println("6. 获得task之后交给对应的worker job")

	for {
		task := getTask()
		switch task.Status {
		case MapTask:
			mapper(task, mapf)
		case ReduceTask:
			reducer(task, reducef)
		case NoTask:
			return
		default:
			return
		}
	}

	// uncomment to send the Example RPC to the master.
	// CallExample()

}

func mapper(task TaskMeta, mapf func(string, string) []KeyValue) []KeyValue {
	log.Println("7. 获得map task,执行mapf")

	content, err := ioutil.ReadFile(task.Filename)
	if err != nil {
		log.Fatal("fail to read file: " + task.Filename, err)
	}
	intermediates := mapf(task.Filename, string(content))

	log.Println("8.1 中间结果切分成R份（reducer数量）")
	buffer := make([][] KeyValue, task.NReducer)
	for _, intermediate := range intermediates {
		slot := ihash(intermediate.Key) % task.NReducer
		buffer[slot] = append(buffer[slot], intermediate)
	}

	log.Println("8.2 中间结果写到本地磁盘")
	mapOutput := make([] string, task.NReducer)
	for i := 0; i < task.NReducer; i++ {
		mapOutput = append(mapOutput, writeToLocalFile(task.MapTaskNumber, i, buffer[i]))

	}
}

func writeToLocalFile(x int, y int, kvs []KeyValue) string {
	dir, _ := os.Getwd()
	tempFile, err := ioutil.TempFile(dir, "mr-tmp-*")
	if err != nil {
		log.Fatal("Fail to create temp file", err)
	}
	enc := json.NewEncoder(tempFile)
	for _, kv := range kvs {
		if err := enc.Encode(&kv); err != nil {
			log.Fatal("fail to write kv pair", err)
		}
	}
	outputName := fmt.Sprintf("mr-%d-%d", x, y)
	os.Rename(tempFile.Name(), outputName)
	tempFile.Close()
	return filepath.Join(dir, outputName)
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

// 5. master给worker分配任务
func getTask() TaskMeta {
	log.Println("5. master给worker分配任务")
	args := ExampleArgs{}
	reply := TaskMeta{}
	call("Master.AssignTask", &args, &reply)
	return reply
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
