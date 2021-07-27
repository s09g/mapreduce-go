package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"fmt"
	"os"
)
import "strconv"

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

type Task struct {
	Input         string
	State         TaskState
	NReducer      int
	TaskNumber    int
	Intermediates []string
	Output 		  string
}

type TaskState int

const (
	MapTask TaskState = iota
	ReduceTask
	WaitTask
	NoTask
)

// Add your RPC definitions here.


// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the master.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func masterSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
var debugging bool = false
func Println(v ...interface{}) {
	if debugging {
		fmt.Println(v...)
	}
}

func Printf(format string, v ...interface{}) {
	if debugging {
		fmt.Printf(format, v...)
	}
}