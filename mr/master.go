package mr

import (
	"errors"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type MasterPhase int

const (
	Map MasterPhase = iota
	Reduce
	Exit
)

type Master struct {
	// Your definitions here.
	TaskQueue     chan *Task
	TaskMeta      map[int]*MasterTask
	Phase         MasterPhase
	NReduce       int
	InputFiles    []string
	Intermediates [][]string
}

type MasterTask struct {
	TaskStatus MasterTaskStatus
	StartTime time.Time
	TaskReference *Task
}

type MasterTaskStatus int

const (
	Idle MasterTaskStatus = iota
	InProgress
	Completed
)

var mu sync.Mutex
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
	mu.Lock()
	ret := m.Phase == Exit
	mu.Unlock()
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
		TaskQueue:     make(chan *Task, max(nReduce, len(files))),
		TaskMeta:      make(map[int]*MasterTask),
		Phase:         Map,
		NReduce:       nReduce,
		InputFiles:    files,
		Intermediates: make([][]string, nReduce),
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
	go m.catchTimeOut()
	return &m
}

func (m *Master) catchTimeOut() {
	for {
		time.Sleep(5 * time.Second)
		mu.Lock()
		Println("catchTimeOut Lock!")
		if m.Phase == Exit {
			return
		}
		for _, masterTask := range m.TaskMeta {
			if masterTask.TaskStatus == InProgress && time.Now().Sub(masterTask.StartTime) > 10 * time.Second {
				Printf("task timeout %v\n", masterTask)
				m.TaskQueue <- masterTask.TaskReference
				masterTask.TaskStatus = Idle
			}
		}
		Println("catchTimeOut unLock!")
		mu.Unlock()
	}
}

func (m *Master) createMapTask() {
	for idx, filename := range m.InputFiles {
		taskMeta := Task{
			Input:      filename,
			State:      MapTask,
			NReducer:   m.NReduce,
			TaskNumber: idx,
		}
		m.TaskQueue <- &taskMeta
		m.TaskMeta[idx] = &MasterTask{
			TaskStatus:    Idle,
			TaskReference: &taskMeta,
		}
	}
}

func (m *Master) createReduceTask() {
	Println("createReduceTask")
	mu.Lock()
	Println("createReduceTask()  Lock!")
	m.TaskMeta = make(map[int]*MasterTask)
	Println("createReduceTask() UnLock!")
	mu.Unlock()
	for idx, files := range m.Intermediates {
		taskMeta := Task{
			State:         ReduceTask,
			NReducer:      m.NReduce,
			TaskNumber:    idx,
			Intermediates: files,
		}
		m.TaskQueue <- &taskMeta
		m.TaskMeta[idx] = &MasterTask{
			TaskStatus:    Idle,
			TaskReference: &taskMeta,
		}
	}
}

func max(a int, b int) int {
	if a > b {
		return a
	}
	return b
}

// 4. master等待worker 调用
func (m *Master) AssignTask(args *ExampleArgs, reply *Task) error {
	if len(m.TaskQueue) > 0 {
		Println("4. master给worker分配任务")
		*reply = *<-m.TaskQueue
		mu.Lock()
		Println("AssignTask Lock!")

		m.TaskMeta[reply.TaskNumber].TaskStatus = InProgress
		m.TaskMeta[reply.TaskNumber].StartTime = time.Now()
		Println("AssignTask UnLock!")

		mu.Unlock()
	} else if m.Phase == Exit {
		*reply = Task{State: NoTask}
	} else {
		*reply = Task{State: WaitTask}
	}
	return nil
}

func (m *Master) TaskCompleted(task *Task, reply *ExampleReply) error {
	{
		mu.Lock()
		Println("TaskCompleted Lock!")
		Println("收到completed task")
		if m.TaskMeta[task.TaskNumber].TaskStatus == Completed {
			Println("straggler: already have results from primary job, discard backup job output")
			Println("Unlock")
			mu.Unlock()
			return nil
		}
		m.TaskMeta[task.TaskNumber].TaskStatus = Completed
		Println("TaskCompleted Unlock")
		mu.Unlock()
	}

	switch task.State {
	case MapTask:
		Println("9.1 master 收到map的结果")

		for reduceTaskId, filePath := range task.Intermediates {
			m.Intermediates[reduceTaskId] = append(m.Intermediates[reduceTaskId], filePath)
		}

		if allTaskDone(m) {
			Println("9.2 结束Map阶段 进入reduce阶段")
			m.createReduceTask()
			mu.Lock()
			Println("TaskCompleted Lock!")
			defer mu.Unlock()
			m.Phase = Reduce
			Println("TaskCompleted UnLock!")
		}
	case ReduceTask:
		Println("12 master 收到reduce的结果")
		if allTaskDone(m) {
			Println("13 结束reduce阶段")
			mu.Lock()
			Println("TaskCompleted Lock!")
			defer mu.Unlock()
			m.Phase = Exit
			Println("TaskCompleted UnLock!")
		}
	default:
		return errors.New("no task info")
	}
	return nil
}

func allTaskDone(m *Master) bool {
	mu.Lock()
	Println("allTaskDone Lock!")
	defer mu.Unlock()
	for _, task := range m.TaskMeta {
		if task.TaskStatus != Completed {
			Println("not allTask Done UnLock!")
			return false
		}
	}
	Println("allTaskDone UnLock!")

	return true
}
