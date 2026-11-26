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

// Types of responses the coordinator can give to a worker.
type TaskType string

const (
	MapTask    TaskType = "map"
	ReduceTask TaskType = "reduce"
	WaitTask   TaskType = "wait"
	ExitTask   TaskType = "exit"
)

type TaskStage int

const (
	StageIdle TaskStage = iota

	// Map stages
	StageMapDownload
	StageMapExecute
	StageMapPartition
	StageMapWrite
	StageMapReport

	// Reduce stages
	StageReduceDownload
	StageReduceDecodeSort
	StageReduceExecuteWrite
	StageReduceUpload
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

type HeartbeatArgs struct {
	WorkerID  uint64
	TaskType  TaskType
	TaskID    int
	AttemptID int
	Stage     TaskStage
}

type HeartbeatReply struct {
	Accepted bool
	JobDone  bool
}

type ReportMissingIntermediateArgs struct {
	WorkerID        uint64
	ReduceTaskID    int
	ReduceAttemptID int
	Location        FileLocation
}

type ReportMissingIntermediateReply struct {
	Accepted bool
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
