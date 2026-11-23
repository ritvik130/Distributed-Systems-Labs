package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

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

// Types of responses the coordinator can give to a worker.
type TaskType string

const (
	MapTask    TaskType = "map"
	ReduceTask TaskType = "reduce"
	WaitTask   TaskType = "wait"
	ExitTask   TaskType = "exit"
)

// Worker requests a new task from the coordinator.
type RequestTaskArgs struct {
	WorkerID uint64
}

type FileLocation struct {
	MapTaskID int
	AttemptID int
	WorkerID  uint64
	Address   string
	FileName  string
}

// Coordinator responds with a task or tells the worker to wait or exit.
type RequestTaskReply struct {
	Type                  TaskType
	TaskID                int
	AttemptID             int
	InputFile             string
	InputURL              string
	NReduce               int
	IntermediateFiles     []string
	IntermediateLocations []FileLocation
}

// Worker reports the result of a completed or failed task.
type ReportTaskArgs struct {
	WorkerID    uint64
	Type        TaskType
	TaskID      int
	AttemptID   int
	Success     bool
	OutputFiles []string
}

// Coordinator tells the worker whether its result was accepted.
type ReportTaskReply struct {
	Accepted bool
}

type RegisterWorkerArgs struct {
	RegistrationToken string
	Address           string
}

type RegisterWorkerReply struct {
	WorkerID uint64
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
