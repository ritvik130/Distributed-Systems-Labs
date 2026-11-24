
package mr

import (
	"os"
	"strconv"
)

// Example RPC definitions.

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Types of responses the Coordinator can give to a Worker.

type TaskType string

const (
	MapTask    TaskType = "map"
	ReduceTask TaskType = "reduce"
	WaitTask   TaskType = "wait"
	ExitTask   TaskType = "exit"
)

// Worker requests a new task from the Coordinator.

type RequestTaskArgs struct {
	WorkerID uint64
}

// Identifies an intermediate file produced by a Map task.

type FileLocation struct {
	MapTaskID int
	AttemptID int
	WorkerID  uint64
	Address   string
	FileName  string
}

// Coordinator responds with a task or tells the Worker to wait or exit.

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

// Worker reports the result of a completed or failed Map task.

type ReportTaskArgs struct {
	WorkerID    uint64
	Type        TaskType
	TaskID      int
	AttemptID   int
	Success     bool
	OutputFiles []string
}

type ReportTaskReply struct {
	Accepted bool
}

// Worker registration.

type RegisterWorkerArgs struct {
	RegistrationToken string
	Address           string
}

type RegisterWorkerReply struct {
	WorkerID uint64
}

// Worker heartbeat.

type HeartbeatArgs struct {
	WorkerID uint64
}

type HeartbeatReply struct {
	Accepted bool
}

// Reduce Worker reports an unavailable intermediate file.

type ReportMissingIntermediateArgs struct {
	WorkerID        uint64
	ReduceTaskID    int
	ReduceAttemptID int
	Location        FileLocation
}

type ReportMissingIntermediateReply struct {
	Accepted bool
}

// Coordinator Unix-domain socket name.

func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
