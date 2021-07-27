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
	TaskStatus    MasterTaskStatus
	StartTime     time.Time
	TaskReference *Task
}

type MasterTaskStatus int

const (
	Idle MasterTaskStatus = iota
	InProgress
	Completed
)

var mu sync.Mutex

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

	// 1. 切成16MB-64MB的文件，GFS负责这一步
	// 2. 创建map任务
	m.createMapTask()

	// 3. 一个程序成为master，其他成为worker
	//这里就是启动master 服务器就行了，
	//拥有master代码的就是master，别的发RPC过来的都是worker
	m.server()
	//
	go m.catchTimeOut()
	return &m
}

func (m *Master) catchTimeOut() {
	for {
		time.Sleep(5 * time.Second)
		mu.Lock()
		if m.Phase == Exit {
			return
		}
		for _, masterTask := range m.TaskMeta {
			if masterTask.TaskStatus == InProgress && time.Now().Sub(masterTask.StartTime) > 10*time.Second {
				m.TaskQueue <- masterTask.TaskReference
				masterTask.TaskStatus = Idle
			}
		}
		mu.Unlock()
	}
}

func (m *Master) createMapTask() {
	// 根据传入的filename，每个文件对应一个map task
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
	mu.Lock()
	m.TaskMeta = make(map[int]*MasterTask)
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

// 4. master等待worker调用
func (m *Master) AssignTask(args *ExampleArgs, reply *Task) error {
	// assignTask就看看自己queue里面还有没有task
	if len(m.TaskQueue) > 0 {
		//有就发出去
		*reply = *<-m.TaskQueue
		mu.Lock()
		// 记录task的启动时间
		m.TaskMeta[reply.TaskNumber].TaskStatus = InProgress
		m.TaskMeta[reply.TaskNumber].StartTime = time.Now()
		mu.Unlock()
	} else if m.Phase == Exit {
		*reply = Task{State: NoTask}
	} else {
		// 没有task就让worker 等待
		*reply = Task{State: WaitTask}
	}
	return nil
}

func (m *Master) TaskCompleted(task *Task, reply *ExampleReply) error {
	//更新task状态
	mu.Lock()
	if m.TaskMeta[task.TaskNumber].TaskStatus == Completed {
		// 因为worker写在同一个文件磁盘上，对于重复的结果要丢弃
		mu.Unlock()
		return nil
	}
	m.TaskMeta[task.TaskNumber].TaskStatus = Completed
	mu.Unlock()

	switch task.State {
	case MapTask:
		//收集intermediate信息
		for reduceTaskId, filePath := range task.Intermediates {
			m.Intermediates[reduceTaskId] = append(m.Intermediates[reduceTaskId], filePath)
		}
		if allTaskDone(m) {
			//获得所以map task后，进入reduce阶段
			m.createReduceTask()
			mu.Lock()
			defer mu.Unlock()
			m.Phase = Reduce
		}
	case ReduceTask:
		if allTaskDone(m) {
			//获得所以reduce task后，进入exit阶段
			mu.Lock()
			defer mu.Unlock()
			m.Phase = Exit
		}
	default:
		return errors.New("no task info")
	}
	return nil
}

func allTaskDone(m *Master) bool {
	mu.Lock()
	defer mu.Unlock()
	for _, task := range m.TaskMeta {
		if task.TaskStatus != Completed {
			return false
		}
	}
	return true
}
