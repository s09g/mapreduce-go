package mr

import (
	"container/list"
	"errors"
	"log"
)
import "net"
import "os"
import "net/rpc"
import "net/http"


type Master struct {
	// Your definitions here.
	MapTaskQueue *list.List
	MapTaskStatus map[int]MasterTaskStatus

	ReduceTaskQueue *list.List
	ReduceTaskStatus map[int]MasterTaskStatus

}

type MasterTaskStatus int

const (
	Idle MasterTaskStatus = iota
	InProgress
	Completed
)


// Your code here -- RPC handlers for the worker to call.

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
	m := Master{
		MapTaskQueue: list.New(),
		MapTaskStatus: make(map[int]MasterTaskStatus),
		ReduceTaskQueue: list.New(),
		ReduceTaskStatus: make(map[int]MasterTaskStatus),
	}


	// Your code here.

	// 1. 切成16MB-64MB的文件
	log.Println("1 make master")
	// 2. 创建任务副本
	log.Println("2 创建任务副本")
	for idx, filename := range files {
		m.MapTaskQueue.PushBack(MapTaskMeta{
			Filename: filename,
			Status:   MapTask,
			NReducer: nReduce,
			MapTaskNumber: idx,
		})
		m.MapTaskStatus[idx] = Idle
	}
	for i := 0; i < nReduce; i++ {
		m.ReduceTaskQueue.PushBack(ReduceTaskMeta{
			Status:           ReduceTask,
			ReduceTaskNumber: i,
		})
		m.ReduceTaskStatus[i] = Idle
	}
	// 3. 一个程序成为master，其他成为worker
	log.Println("3 启动server服务器")
	m.server()
	return &m
}

// 4. master等待worker 调用
func (m *Master) AssignTask(args *ExampleArgs, reply *MapTaskMeta) error {
	if m.MapTaskQueue.Len() > 0 {
		log.Println("4. master给worker分配map任务")

		element := m.MapTaskQueue.Front()
		m.MapTaskQueue.Remove(element)
		if task, ok := element.Value.(MapTaskMeta); ok {
			reply = &task
			m.MapTaskStatus[task.MapTaskNumber] = InProgress
		} else {
			return errors.New("Cannot cast to MapTaskMeta")
		}
	}

	return nil
}

func (m *Master) MapTaskCompleted(task *MapTaskMeta, reply *MapTaskMeta) error {
	log.Println("9.1 master 收到map的结果")
	m.MapTaskStatus[task.MapTaskNumber] = Completed

}