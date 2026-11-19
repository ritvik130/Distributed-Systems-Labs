package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

// TaskState is the lifecycle stage of a Map or Reduce task.
type TaskState string

const (
	Idle       TaskState = "idle"
	InProgress TaskState = "in_progress"
	Completed  TaskState = "completed"
)

// Task is the coordinator's bookkeeping for one Map or Reduce task.
type Task struct {
	State           TaskState
	InputFile       string    // Map tasks only
	LastAssigned    time.Time // used to detect timed-out workers
	AttemptsIssued  int       // also serves as the latest AttemptID
	AcceptedAttempt int
	OutputFiles     []string // Map: one file per reduce partition; Reduce: the final file
}

// Coordinator hands out tasks to workers and tracks their progress.
type Coordinator struct {
	mu          sync.Mutex // RPC handlers run concurrently
	MapTasks    []Task
	ReduceTasks []Task
	NReduce     int
}

// allTasksCompleted reports whether every task in the slice is Completed.
func allTasksCompleted(tasks []Task) bool {
	for _, task := range tasks {
		if task.State != Completed {
			return false
		}
	}

	return true
}

// RequestTask is the RPC handler workers call to ask for work.
//
// Map tasks are handed out first, then Reduce tasks (only after all Map
// tasks are done), then ExitTask. A task is assignable if it is idle or has
// been in progress for 10s or more, in which case the worker is timed out.
func (c *Coordinator) RequestTask(
	args *RequestTaskArgs,
	reply *RequestTaskReply,
) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Find the first available Map task
	for i := range c.MapTasks {
		task := &c.MapTasks[i]

		if task.State == Idle ||
			(task.State == InProgress && time.Since(task.LastAssigned) >= 10*time.Second) {
			task.State = InProgress
			task.AttemptsIssued++
			task.LastAssigned = time.Now()

			reply.Type = MapTask
			reply.TaskID = i
			reply.AttemptID = task.AttemptsIssued
			reply.InputFile = task.InputFile
			reply.NReduce = c.NReduce

			return nil
		}
	}

	// No idle Map tasks, but some are still running.
	if !allTasksCompleted(c.MapTasks) {
		reply.Type = WaitTask
		return nil
	}

	// All Map tasks are completed. Assign a Reduce task.
	for i := range c.ReduceTasks {
		task := &c.ReduceTasks[i]

		if task.State == Idle ||
			(task.State == InProgress && time.Since(task.LastAssigned) >= 10*time.Second) {
			task.State = InProgress
			task.AttemptsIssued++
			task.LastAssigned = time.Now()

			reply.Type = ReduceTask
			reply.TaskID = i
			reply.AttemptID = task.AttemptsIssued

			// Reduce task i reads partition i from every Map task's output.
			for _, mapTask := range c.MapTasks {
				reply.IntermediateFiles = append(
					reply.IntermediateFiles,
					mapTask.OutputFiles[i],
				)
			}

			return nil
		}
	}

	// No idle Reduce tasks, but some are still running
	if !allTasksCompleted(c.ReduceTasks) {
		reply.Type = WaitTask
		return nil
	}

	// Otherwise all tasks are completed
	reply.Type = ExitTask
	return nil
}

// ReportTask is the RPC handler workers call to report a task's result.
//
// Invalid, stale, duplicate and failed reports are ignored. Only the first
// valid successful report for a task is accepted (reply.Accepted = true).
func (c *Coordinator) ReportTask(
	args *ReportTaskArgs,
	reply *ReportTaskReply,
) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	var task *Task

	switch args.Type {
	case MapTask:
		if args.TaskID < 0 || args.TaskID >= len(c.MapTasks) {
			return nil
		}
		task = &c.MapTasks[args.TaskID]
	case ReduceTask:
		if args.TaskID < 0 || args.TaskID >= len(c.ReduceTasks) {
			return nil
		}
		task = &c.ReduceTasks[args.TaskID]
	default:
		return nil
	}

	// Validate the attempt and its result.
	if task.State == Completed {
		return nil
	}
	if args.AttemptID <= 0 || args.AttemptID > task.AttemptsIssued {
		return nil
	}
	if !args.Success {
		return nil
	}
	// check output files
	if args.Type == MapTask && len(args.OutputFiles) != c.NReduce {
		return nil
	}
	if args.Type == ReduceTask && len(args.OutputFiles) != 1 {
		return nil
	}

	if args.Type == MapTask {
		task.State = Completed
		task.AcceptedAttempt = args.AttemptID
		task.OutputFiles = args.OutputFiles
		reply.Accepted = true
	}
	if args.Type == ReduceTask {
		// 1. Construct the final output filename.
		oldpath := args.OutputFiles[0]
		newpath := fmt.Sprintf("mr-out-%d", args.TaskID)
		// 2. Rename the worker's output file. Doing this only on acceptance
		// ensures just one attempt's output becomes the final file.
		err := os.Rename(oldpath, newpath)
		if err != nil {
			return nil
		}
		task.State = Completed
		task.AcceptedAttempt = args.AttemptID
		task.OutputFiles = []string{newpath}
		reply.Accepted = true
	}

	return nil

}

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {

	c.mu.Lock()
	defer c.mu.Unlock()

	return allTasksCompleted(c.MapTasks) && allTasksCompleted(c.ReduceTasks)
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	c.NReduce = nReduce

	// One Map task per input file.
	c.MapTasks = make([]Task, len(files))
	for i, file := range files {
		c.MapTasks[i] = Task{
			State:     Idle,
			InputFile: file,
		}
	}

	c.ReduceTasks = make([]Task, nReduce)
	for i := 0; i < nReduce; i++ {
		c.ReduceTasks[i].State = Idle
	}

	c.server()
	return &c
}
