package mr

import (
	"log"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type MasterPhase int

const (
	Map MasterPhase = iota
	Reduce
	Done
)

type Master struct {
	// Your definitions here.
	TaskQueue       chan *TaskMeta
	TaskStatus      map[int]MasterTaskStatus
	TaskCollections []*TaskMeta
	Phase           MasterPhase
	NReduce         int
	InputFiles 		[]string
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
	ret := m.Phase == Done

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
		TaskQueue:       make(chan *TaskMeta, max(nReduce, len(files))),
		TaskStatus:      make(map[int]MasterTaskStatus),
		TaskCollections: make([]*TaskMeta, 0),
		Phase:           Map,
		NReduce:         nReduce,
		InputFiles: 	 files,
	}


	// Your code here.

	// 1. 切成16MB-64MB的文件
	log.Println("1 make master")
	// 2. 创建任务副本
	log.Println("2 创建Map任务副本")
	createMapTask(&m)

	// 3. 一个程序成为master，其他成为worker
	log.Println("3 启动server服务器")
	m.server()
	return &m
}

func createMapTask(m *Master) {
	for idx, filename := range m.InputFiles {
		m.TaskQueue <- &TaskMeta{
			Filename:   filename,
			State:      MapTask,
			NReducer:   m.NReduce,
			TaskNumber: idx,
		}
		m.TaskStatus[idx] = Idle
	}
}

func max(a int, b int) int {
	if a > b {
		return a
	}
	return b
}

// 4. master等待worker 调用
func (m *Master) AssignTask(args *ExampleArgs, reply *TaskMeta) error {
	if len(m.TaskQueue) > 0 {
		log.Println("4. master给worker分配map任务")
		reply = <- m.TaskQueue
		m.TaskStatus[reply.TaskNumber] = InProgress
		return nil
	}
	if m.Phase == Done {
		reply = &TaskMeta{State: NoTask,}
		return nil
	}
	reply = &TaskMeta{State: WaitTask,}
	return nil
}

func (m *Master) TaskCompleted(task *TaskMeta, reply *ExampleReply) error {
	log.Println("收到completed task")
	switch task.State {
	case MapTask:
		log.Println("9.1 master 收到map的结果")
		m.TaskStatus[task.TaskNumber] = Completed
		m.TaskCollections = append(m.TaskCollections, task)
		if allTaskDone(m) {
			log.Println("9.2 结束Map阶段 进入reduce阶段")

		}
	}

}

func allTaskDone(m *Master) bool {
	for _, task := range m.TaskCollections {
		if m.TaskStatus[task.TaskNumber] != Completed {
			return false
		}
	}
	return true
}