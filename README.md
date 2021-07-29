# mapreduce

MIT 6.824 (2020 Spring) Lab1

Lab code has changed in the new semester. Here is the old version. Don't copy my answer.

---

## MapReduce的执行流程

根据论文第三节，MapReduce的执行流程分这么几步：

> 1. The MapReduce library in the user program first splits the input files into M pieces of typically 16 megabytes to 64 megabytes (MB) per piece (controllable by the user via an optional parameter). It then starts up many copies of the program on a cluster of machines.

启动MapReduce, 将输入文件切分成大小在16-64MB之间的文件。然后在一组多个机器上启动用户程序

> 2. One of the copies of the program is special – the master. The rest are workers that are assigned work by the master. There are M map tasks and R reduce tasks to assign. The master picks idle workers and assigns each one a map task or a reduce task.

其中一个副本将成为master, 余下成为worker. master给worker指定任务（M个map任务，R个reduce任务）。master选择空闲的worker给予map或reduce任务

> 3. A worker who is assigned a map task reads the contents of the corresponding input split. It parses key/value pairs out of the input data and passes each pair to the user-defined Map function. The intermediate key/value pairs produced by the Map function are buffered in memory.

Map worker 接收切分后的input，执行Map函数，将结果缓存到内存

> 4. Periodically, the buffered pairs are written to local disk, partitioned into R regions by the partitioning function. The locations of these buffered pairs on the local disk are passed back to the master, who is responsible for forwarding these locations to the reduce workers.

缓存后的中间结果会周期性的写到本地磁盘，并切分成R份（reducer数量）。R个文件的位置会发送给master, master转发给reducer

> 5. When a reduce worker is notified by the master about these locations, it uses remote procedure calls to read the buffered data from the local disks of the map workers. When a reduce worker has read all intermediate data, it sorts it by the intermediate keys so that all occurrences of the same key are grouped together. The sorting is needed because typically many different keys map to the same reduce task. If the amount of intermediate data is too large to fit in memory, an external sort is used.

Reduce worker 收到中间文件的位置信息，通过RPC读取。读取完先根据中间<k, v>排序，然后按照key分组、合并。

> 6. The reduce worker iterates over the sorted intermediate data and for each unique intermediate key encountered, it passes the key and the corresponding set of intermediate values to the user’s Reduce function. The output of the Reduce function is appended to a final output file for this reduce partition.

Reduce worker在排序后的数据上迭代，将中间<k, v> 交给reduce 函数处理。最终结果写给对应的output文件（分片）

> 7. When all map tasks and reduce tasks have been completed, the master wakes up the user program. At this point, the MapReduce call in the user program returns back to the user code.

所有map和reduce任务结束后，master唤醒用户程序

---

## MapReduce的实现

### Master的数据结构设计

这一部分对论文的3.2节有所改动，相比于原论文有所简化。

论文提到每个(Map或者Reduce)Task有分为idle, in-progress, completed 三种状态。

```go
type MasterTaskStatus int

const (
	Idle MasterTaskStatus = iota
	InProgress
	Completed
)
```

Master存储这些Task的信息。与论文不同的是，这里我并没有保留worked的ID，因此master不会主动向worker发送`心跳检测`

```go
type MasterTask struct {
	TaskStatus    MasterTaskStatus
	StartTime     time.Time
	TaskReference *Task
}
```

此外Master存储Map任务产生的R个中间文件的信息。

```go
type Master struct {
	TaskQueue     chan *Task          // 等待执行的task
	TaskMeta      map[int]*MasterTask // 当前所有task的信息
	MasterPhase   State               // Master的阶段
	NReduce       int
	InputFiles    []string
	Intermediates [][]string // Map任务产生的R个中间文件的信息
}
```
Map和Reduce的Task应该负责不同的事情，但是在实现代码的过程中发现同一个Task结构完全可以兼顾两个阶段的任务。

```go
type Task struct {
	Input         string
	TaskState     State
	NReducer      int
	TaskNumber    int
	Intermediates []string
	Output        string
}
```

此外我将task和master的状态合并成一个State。task和master的状态应该一致。如果在Reduce阶段收到了迟来MapTask结果，应该直接丢弃。

```go
type State int

const (
	Map State = iota
	Reduce
	Exit
	Wait
)
```

### MapReduce执行过程的实现

**1. 启动master**

```go
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{
		TaskQueue:     make(chan *Task, max(nReduce, len(files))),
		TaskMeta:      make(map[int]*MasterTask),
		MasterPhase:   Map,
		NReduce:       nReduce,
		InputFiles:    files,
		Intermediates: make([][]string, nReduce),
	}

	// 切成16MB-64MB的文件
	// 创建map任务
	m.createMapTask()

	// 一个程序成为master，其他成为worker
	//这里就是启动master 服务器就行了，
	//拥有master代码的就是master，别的发RPC过来的都是worker
	m.server()
	// 启动一个goroutine 检查超时的任务
	go m.catchTimeOut()
	return &m
}
```

**2. master监听worker RPC调用，分配任务**

```go
// master等待worker调用
func (m *Master) AssignTask(args *ExampleArgs, reply *Task) error {
	// assignTask就看看自己queue里面还有没有task
	mu.Lock()
	defer mu.Unlock()
	if len(m.TaskQueue) > 0 {
		//有就发出去
		*reply = *<-m.TaskQueue
		// 记录task的启动时间
		m.TaskMeta[reply.TaskNumber].TaskStatus = InProgress
		m.TaskMeta[reply.TaskNumber].StartTime = time.Now()
	} else if m.MasterPhase == Exit {
		*reply = Task{TaskState: Exit}
	} else {
		// 没有task就让worker 等待
		*reply = Task{TaskState: Wait}
	}
	return nil
}
```

**3. 启动worker**

```go
func Worker(mapf func(string, string) []KeyValue, 
            reducef func(string, []string) string) {
	// 启动worker
	for {
		// worker从master获取任务
		task := getTask()

		// 拿到task之后，根据task的state，map task交给mapper， reduce task交给reducer
		// 额外加两个state，让 worker 等待 或者 直接退出
		switch task.TaskState {
		case Map:
			mapper(&task, mapf)
		case Reduce:
			reducer(&task, reducef)
		case Wait:
			time.Sleep(5 * time.Second)
		case Exit:
			return
		}
	}
}
```

**4. worker向master发送RPC请求任务**

```go
func getTask() Task {
	// worker从master获取任务
	args := ExampleArgs{}
	reply := Task{}
	call("Master.AssignTask", &args, &reply)
	return reply
}
```

**5. worker获得MapTask，交给mapper处理**

```go
func mapper(task *Task, mapf func(string, string) []KeyValue) {
	//从文件名读取content
	content, err := ioutil.ReadFile(task.Input)
	if err != nil {
		log.Fatal("fail to read file: "+task.Input, err)
	}
	//将content交给mapf，缓存结果
	intermediates := mapf(task.Input, string(content))

	//缓存后的结果会写到本地磁盘，并切成R份
	//切分方式是根据key做hash
	buffer := make([][]KeyValue, task.NReducer)
	for _, intermediate := range intermediates {
		slot := ihash(intermediate.Key) % task.NReducer
		buffer[slot] = append(buffer[slot], intermediate)
	}
	mapOutput := make([]string, 0)
	for i := 0; i < task.NReducer; i++ {
		mapOutput = append(mapOutput, writeToLocalFile(task.TaskNumber, i, &buffer[i]))
	}
	//R个文件的位置发送给master
	task.Intermediates = mapOutput
	TaskCompleted(task)
}
```

**6. worker任务完成后通知master**

```go
func TaskCompleted(task *Task) {
	//通过RPC，把task信息发给master
	reply := ExampleReply{}
	call("Master.TaskCompleted", task, &reply)
}
```

**7. master收到完成后的Task**

+ 如果所有的MapTask都已经完成，创建ReduceTask，转入Reduce阶段
+ 如果所有的ReduceTask都已经完成，转入Exit阶段

```go
func (m *Master) TaskCompleted(task *Task, reply *ExampleReply) error {
	//更新task状态
	mu.Lock()
	if task.TaskState != m.MasterPhase || m.TaskMeta[task.TaskNumber].TaskStatus == Completed {
		// 因为worker写在同一个文件磁盘上，对于重复的结果要丢弃
		return nil
	}
	m.TaskMeta[task.TaskNumber].TaskStatus = Completed
	mu.Unlock()
	defer m.processTaskResult(task)
	return nil
}

func (m *Master) processTaskResult(task *Task)  {
	mu.Lock()
	defer mu.Unlock()
	switch task.TaskState {
	case Map:
		//收集intermediate信息
		for reduceTaskId, filePath := range task.Intermediates {
			m.Intermediates[reduceTaskId] = append(m.Intermediates[reduceTaskId], filePath)
		}
		if m.allTaskDone() {
			//获得所以map task后，进入reduce阶段
			m.createReduceTask()
			m.MasterPhase = Reduce
		}
	case Reduce:
		if m.allTaskDone() {
			//获得所以reduce task后，进入exit阶段
			m.MasterPhase = Exit
		}
	}
}
```

这里使用一个辅助函数判断是否当前阶段所有任务都已经完成

```go
func (m *Master) allTaskDone() bool {
	for _, task := range m.TaskMeta {
		if task.TaskStatus != Completed {
			return false
		}
	}
	return true
}
```

**8. 转入Reduce阶段，worker获得ReduceTask，交给reducer处理**

```go
func reducer(task *Task, reducef func(string, []string) string) {
	//先从filepath读取intermediate的KeyValue
	intermediate := *readFromLocalFile(task.Intermediates)
	//根据kv排序
	sort.Sort(ByKey(intermediate))

	dir, _ := os.Getwd()
	tempFile, err := ioutil.TempFile(dir, "mr-tmp-*")
	if err != nil {
		log.Fatal("Fail to create temp file", err)
	}
	// 这部分代码修改自mrsequential.go
	i := 0
	for i < len(intermediate) {
		//将相同的key放在一起分组合并
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		//交给reducef，拿到结果
		output := reducef(intermediate[i].Key, values)
		//写到对应的output文件
		fmt.Fprintf(tempFile, "%v %v\n", intermediate[i].Key, output)
		i = j
	}
	tempFile.Close()
	oname := fmt.Sprintf("mr-out-%d", task.TaskNumber)
	os.Rename(tempFile.Name(), oname)
	task.Output = oname
	TaskCompleted(task)
}
```

**9. master确认所有ReduceTask都已经完成，转入Exit阶段，终止所有master和worker goroutine**

```go
func (m *Master) Done() bool {
	mu.Lock()
	defer mu.Unlock()
	ret := m.MasterPhase == Exit
	return ret
}
```


**10. 上锁**
master跟多个worker通信，master的数据是共享的，其中`TaskMeta, Phase, Intermediates, TaskQueue` 都有读写发生。`TaskQueue`使用`channel`实现，自己带锁。只有涉及`Intermediates, TaskMeta, Phase`的操作需要上锁。*PS.写的糙一点，那就是master每个方法都要上锁，master直接变成同步执行。。。*

另外go -race并不能检测出所有的datarace。我曾一度任务`Intermediates`写操作发生在map阶段，读操作发生在reduce阶段读，逻辑上存在`barrier`，所以不会有datarace. 但是后来想到两个write也可能造成datarace，然而Go Race Detector并没有检测出来。


**11. carsh处理**

test当中有容错的要求，不过只针对worker。mapreduce论文中提到了：

1. 周期性向worker发送心跳检测
+ 如果worker失联一段时间，master将worker标记成failed
+ worker失效之后
  + 已完成的map task被重新标记为idle
  + 已完成的reduce task不需要改变
  + 原因是：map的结果被写在local disk，worker machine 宕机会导致map的结果丢失；reduce结果存储在GFS，不会随着machine down丢失
2. 对于in-progress 且超时的任务，启动backup执行


tricky的在于Lab1是在单机上用多进程模拟了多机器，但并不会因为进程终止导致写好的文件丢失，这也是为什么我前面没有按照论文保留workerID.

针对Lab1修改后的容错设计：

1. 周期性检查task是否完成。将超时未完成的任务，交给新的worker，backup执行

```go
func (m *Master) catchTimeOut() {
	for {
		time.Sleep(5 * time.Second)
		mu.Lock()
		if m.MasterPhase == Exit {
			mu.Unlock()
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
```

2. 从第一个完成的worker获取结果，将后序的backup结果丢弃

```go
if task.TaskState != m.MasterPhase || m.TaskMeta[task.TaskNumber].TaskStatus == Completed {
	// 因为worker写在同一个文件磁盘上，对于重复的结果要丢弃
	return nil
}
m.TaskMeta[task.TaskNumber].TaskStatus = Completed
```

---

## 测试

开启race detector，执行`test-mr.sh`

```bash
$ ./test-mr.sh        
*** Starting wc test.
--- wc test: PASS
*** Starting indexer test.
--- indexer test: PASS
*** Starting map parallelism test.
--- map parallelism test: PASS
*** Starting reduce parallelism test.
--- reduce parallelism test: PASS
*** Starting crash test.
--- crash test: PASS
*** PASSED ALL TESTS
```

PS. Lab1要求的Go版本是1.13，如果使用的更新的版本，会报出
```bash
rpc.Register: method "Done" has 1 input parameters; needs exactly three
```
可以直接无视
