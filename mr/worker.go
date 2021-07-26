package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"sort"
	"time"
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

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	//6. 获得task之后交给对应的worker job
	log.Println("worker: 6. 获得task之后交给对应的worker job")

	for {
		task := getTask()
		switch task.State {
		case MapTask:
			mapper(&task, mapf)
			TaskCompleted(&task)
		case ReduceTask:
			reducer(&task, reducef)
			TaskCompleted(&task)
		case WaitTask:
			log.Println("sleep and waiting")
			time.Sleep(5 * time.Second)
		case NoTask:
			return
		default:
			return
		}
	}

	// uncomment to send the Example RPC to the master.
	// CallExample()

}

func reducer(task *TaskMeta, reducef func(string, []string) string) {
	log.Println("worker: 10. 获得reduce task, 执行reducef")
	intermediate := *readFromLocalFile(task.Intermediates)
	sort.Sort(ByKey(intermediate))

	oname := fmt.Sprintf("mr-out-%d", task.TaskNumber)
	ofile, _ := os.Create(oname)

	//
	// call Reduce on each distinct key in intermediate[],
	// and print the result to mr-out-0.
	//
	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}

	ofile.Close()
	task.Output = oname
}

func mapper(task *TaskMeta, mapf func(string, string) []KeyValue) {
	log.Println("worker: 7. 获得map task,执行mapf")

	content, err := ioutil.ReadFile(task.Input)
	if err != nil {
		log.Fatal("fail to read file: " + task.Input, err)
	}
	intermediates := mapf(task.Input, string(content))

	log.Println("worker: 8.1 中间结果切分成R份（reducer数量）")
	buffer := make([][] KeyValue, task.NReducer)
	for _, intermediate := range intermediates {
		slot := ihash(intermediate.Key) % task.NReducer
		buffer[slot] = append(buffer[slot], intermediate)
	}

	log.Println("worker: 8.2 中间结果写到本地磁盘")
	mapOutput := make([] string, 0)
	for i := 0; i < task.NReducer; i++ {
		mapOutput = append(mapOutput, writeToLocalFile(task.TaskNumber, i, &buffer[i]))
	}

	log.Println("worker: 8.3 将R份文件位置发送给master")
	task.Intermediates = mapOutput
}

func TaskCompleted(task *TaskMeta) {
	log.Println("worker: 8.3 通知master map任务完成，将R份文件位置发送给master")
	reply := ExampleReply{}
	call("Master.TaskCompleted", task, &reply)
}

func writeToLocalFile(x int, y int, kvs *[]KeyValue) string {
	dir, _ := os.Getwd()
	tempFile, err := ioutil.TempFile(dir ,"mr-tmp-*")
	if err != nil {
		log.Fatal("Fail to create temp file", err)
	}
	enc := json.NewEncoder(tempFile)
	for _, kv := range *kvs {
		if err := enc.Encode(&kv); err != nil {
			log.Fatal("fail to write kv pair", err)
		}
	}
	outputName := fmt.Sprintf("mr-tmp-%d-%d", x, y)
	os.Rename(tempFile.Name(), outputName)
	tempFile.Close()
	return filepath.Join(dir, outputName)
}

func readFromLocalFile(files []string) *[]KeyValue{
	kva := []KeyValue{}
	for _, filepath := range files{
		file, err := os.Open(filepath)
		if err != nil {
			log.Fatal("Fail to open file " + filepath, err)
		}
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kva = append(kva, kv)
		}
		file.Close()
	}
	return &kva
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
	log.Println("worker: 5. master给worker分配任务")
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
