package mr

import (
	"errors"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
)

type MasterPhase int

const (
	Map MasterPhase = iota
	Reduce
	Exit
)

type Master struct {
	// Your definitions here.
	TaskQueue       chan *TaskMeta
	TaskStatus      map[int]MasterTaskStatus
	Phase           MasterPhase
	NReduce         int
	InputFiles      []string
	Intermediates   [][]string
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
// main/mrmaster.go calls Exit() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	ret := m.Phase == Exit

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
		Phase:           Map,
		NReduce:         nReduce,
		InputFiles:      files,
		Intermediates:   make([][]string, nReduce),
	}

	// Your code here.

	// 1. 切成16MB-64MB的文件
	Println("1 make master")
	// 2. 创建任务副本
	Println("2 创建Map任务副本")
	m.createMapTask()

	// 3. 一个程序成为master，其他成为worker
	Println("3 启动server服务器")
	m.server()
	return &m
}

func (m *Master) createMapTask() {
	for idx, filename := range m.InputFiles {
		m.TaskQueue <- &TaskMeta{
			Input:      filename,
			State:      MapTask,
			NReducer:   m.NReduce,
			TaskNumber: idx,
		}
		m.TaskStatus[idx] = Idle
	}
}

func (m *Master) createReduceTask() {
	Println("createReduceTask")

	m.TaskStatus = make(map[int]MasterTaskStatus)
	for idx, files := range m.Intermediates {
		m.TaskQueue <- &TaskMeta{
			State:         ReduceTask,
			NReducer:      m.NReduce,
			TaskNumber:    idx,
			Intermediates: files,
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
		Println("4. master给worker分配任务")
		*reply = *<-m.TaskQueue
		m.TaskStatus[reply.TaskNumber] = InProgress
	} else if m.Phase == Exit {
		*reply = TaskMeta{State: NoTask}
	} else {
		*reply = TaskMeta{State: WaitTask}
	}
	return nil
}

func (m *Master) TaskCompleted(task *TaskMeta, reply *ExampleReply) error {
	Println("收到completed task")
	if m.TaskStatus[task.TaskNumber] == Completed {
		Println("straggler: already have results from primary job, discard backup job output")
		return nil
	}
	m.TaskStatus[task.TaskNumber] = Completed

	switch task.State {
	case MapTask:
		Println("9.1 master 收到map的结果")

		for reduceTaskId, filePath := range task.Intermediates {
			m.Intermediates[reduceTaskId] = append(m.Intermediates[reduceTaskId], filePath)
		}

		if allTaskDone(m) {
			Println("9.2 结束Map阶段 进入reduce阶段")
			m.createReduceTask()
			m.Phase = Reduce
		}
	case ReduceTask:
		Println("12 master 收到reduce的结果")
		if allTaskDone(m) {
			Println("13 结束reduce阶段")
			m.Phase = Exit
		}
	default:
		return errors.New("no task info")
	}
	return nil
}

func allTaskDone(m *Master) bool {
	for _, status := range m.TaskStatus {
		if status != Completed {
			return false
		}
	}
	return true
}
