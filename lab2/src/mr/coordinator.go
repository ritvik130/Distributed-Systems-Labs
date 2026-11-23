package mr

import (
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"strings"
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
	Attempts        map[int]*AttemptInfo
	OutputFiles     []string // Map: one file per reduce partition; Reduce: the final file
}

type AttemptInfo struct {
	WorkerID   uint64
	AssignedAt time.Time
}

type WorkerInfo struct {
	ID      uint64
	Address string
	Token   string
}

// Coordinator hands out tasks to workers and tracks their progress.
type Coordinator struct {
	mu          sync.Mutex
	MapTasks    []Task
	ReduceTasks []Task
	NReduce     int

	// Existing worker fields...
	Workers      map[uint64]*WorkerInfo
	WorkerTokens map[string]uint64
	NextWorkerID uint64

	InputBaseURL string // HTTP address used to download Map inputs
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

	_, ok := c.Workers[args.WorkerID]
	if !ok {
		return fmt.Errorf("Worker %d is not registered", args.WorkerID)
	}
	// Find the first available Map task
	for i := range c.MapTasks {
		task := &c.MapTasks[i]

		if task.State == Idle ||
			(task.State == InProgress && time.Since(task.LastAssigned) >= 10*time.Second) {
			task.State = InProgress
			task.AttemptsIssued++
			task.LastAssigned = time.Now()

			if task.Attempts == nil {
				task.Attempts = make(map[int]*AttemptInfo)
			}
			task.Attempts[task.AttemptsIssued] = &AttemptInfo{
				WorkerID:   args.WorkerID,
				AssignedAt: task.LastAssigned,
			}

			reply.Type = MapTask
			reply.TaskID = i
			reply.AttemptID = task.AttemptsIssued
			reply.InputFile = task.InputFile
			reply.NReduce = c.NReduce
			if c.InputBaseURL != "" {
				reply.InputURL = fmt.Sprintf("%s/inputs/%d", c.InputBaseURL, i)
			}

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

			if task.Attempts == nil {
				task.Attempts = make(map[int]*AttemptInfo)
			}

			task.Attempts[task.AttemptsIssued] = &AttemptInfo{
				WorkerID:   args.WorkerID,
				AssignedAt: task.LastAssigned,
			}

			reply.Type = ReduceTask
			reply.TaskID = i
			reply.AttemptID = task.AttemptsIssued

			for mapID, mapTask := range c.MapTasks {
				attemptID := mapTask.AcceptedAttempt
				workerID := mapTask.Attempts[attemptID].WorkerID
				workerAddr := c.Workers[workerID].Address

				location := FileLocation{
					MapTaskID: mapID,
					AttemptID: attemptID,
					WorkerID:  workerID,
					Address:   workerAddr,
					FileName:  mapTask.OutputFiles[i],
				}

				reply.IntermediateLocations = append(
					reply.IntermediateLocations,
					location,
				)

				// Keep the Basic field temporarily.
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

	// Reduce outputs are handled through HTTP POST.
	if args.Type != MapTask {
		return nil
	}

	if args.TaskID < 0 || args.TaskID >= len(c.MapTasks) {
		return nil
	}

	task := &c.MapTasks[args.TaskID]

	// Check that this attempt belongs to the worker.
	attempt, exists := task.Attempts[args.AttemptID]

	if !exists || attempt == nil || attempt.WorkerID != args.WorkerID {
		return nil
	}

	// Accept duplicate reports from the attempt that completed the task.
	if task.State == Completed {
		if args.Success && args.AttemptID == task.AcceptedAttempt {
			reply.Accepted = true
		}
		return nil
	}

	if !args.Success {
		return nil
	}

	// Each Map task must produce one file per Reduce partition.
	if len(args.OutputFiles) != c.NReduce {
		return nil
	}

	// The first successful Map attempt wins.
	task.State = Completed
	task.AcceptedAttempt = args.AttemptID
	task.OutputFiles = args.OutputFiles
	reply.Accepted = true

	return nil
}

func (c *Coordinator) RegisterWorker(
	args *RegisterWorkerArgs,
	reply *RegisterWorkerReply,
) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if args.RegistrationToken == "" || args.Address == "" {
		return fmt.Errorf("worker token and address can't be empty")
	}

	val, ok := c.WorkerTokens[args.RegistrationToken]

	if !ok {
		c.NextWorkerID++
		c.Workers[c.NextWorkerID] = &WorkerInfo{
			ID:      c.NextWorkerID,
			Address: args.Address,
			Token:   args.RegistrationToken,
		}

		c.WorkerTokens[args.RegistrationToken] = c.NextWorkerID
		reply.WorkerID = c.NextWorkerID
	} else {
		c.Workers[val].Address = args.Address
		reply.WorkerID = val
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

func (c *Coordinator) serveMapInput(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	idString := strings.TrimPrefix(r.URL.Path, "/inputs/")
	taskID, err := strconv.Atoi(idString)

	if err != nil || taskID < 0 || taskID >= len(c.MapTasks) {
		http.Error(w, "Invalid Map task ID", http.StatusNotFound)
		return
	}

	fileName := c.MapTasks[taskID].InputFile
	http.ServeFile(w, r, fileName)
}

func (c *Coordinator) serveReduceOutput(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	query := r.URL.Query()

	taskID, err1 := strconv.Atoi(query.Get("task"))
	attemptID, err2 := strconv.Atoi(query.Get("attempt"))
	workerID, err3 := strconv.ParseUint(query.Get("worker"), 10, 64)

	if err1 != nil || err2 != nil || err3 != nil ||
		taskID < 0 || taskID >= len(c.ReduceTasks) ||
		attemptID <= 0 || workerID == 0 {
		http.Error(w, "Invalid task information", http.StatusBadRequest)
		return
	}

	// Next: validate the attempt and receive the file.
	c.mu.Lock()
	task := &c.ReduceTasks[taskID]
	attempt, exists := task.Attempts[attemptID]

	if !exists || attempt == nil || attempt.WorkerID != workerID {
		c.mu.Unlock()
		http.Error(w, "Invalid worker or attempt", http.StatusForbidden)
		return
	}

	// If this attempt was already accepted, return success again.
	if task.State == Completed {
		accepted := task.AcceptedAttempt == attemptID
		c.mu.Unlock()
		if accepted {
			w.WriteHeader(http.StatusOK)
		} else {
			http.Error(w, "Task already completed", http.StatusConflict)
		}
		return
	}
	c.mu.Unlock()

	outFile, err := os.CreateTemp(".", "reduce-upload-")
	if err != nil {
		http.Error(w, "Cannot create output file", http.StatusInternalServerError)
		return
	}

	tempName := outFile.Name()
	defer os.Remove(tempName)

	_, copyErr := io.Copy(outFile, r.Body)
	closeErr := outFile.Close()

	if copyErr != nil || closeErr != nil {
		http.Error(w, "Failed to save output", http.StatusInternalServerError)
		return
	}

	// Check again because another attempt may have completed during upload.
	c.mu.Lock()

	// If this attempt was already accepted, return success again.
	if task.State == Completed {
		accepted := task.AcceptedAttempt == attemptID
		c.mu.Unlock()
		if accepted {
			w.WriteHeader(http.StatusOK)
		} else {
			http.Error(w, "Task already completed", http.StatusConflict)
		}
		return
	}

	// Validation if job is completed

	finalName := fmt.Sprintf("mr-out-%d", taskID)

	err = os.Rename(tempName, finalName)
	if err != nil {
		c.mu.Unlock()
		http.Error(w, "Failed to publish output", http.StatusInternalServerError)
		return
	}

	task.State = Completed
	task.AcceptedAttempt = attemptID
	task.OutputFiles = []string{finalName}
	c.mu.Unlock()
	fmt.Fprintln(w, "Success!")

}

func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()

	// Serve Map input files over HTTP.
	http.HandleFunc("/inputs/", c.serveMapInput)

	http.HandleFunc("/outputs", c.serveReduceOutput)

	// Keep the existing Unix socket for RPC.
	sockname := coordinatorSock()
	os.Remove(sockname)

	l, err := net.Listen("unix", sockname)
	if err != nil {
		log.Fatal("listen error:", err)
	}

	go http.Serve(l, nil)

	// Start a TCP listener when an address is configured.
	if addr := os.Getenv("MR_COORDINATOR_HTTP_ADDR"); addr != "" {
		tcpListener, err := net.Listen("tcp", addr)
		if err != nil {
			log.Fatal("TCP listen error:", err)
		}

		go http.Serve(tcpListener, nil)
	}
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

	c.InputBaseURL = os.Getenv("MR_COORDINATOR_PUBLIC_URL")

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
	c.Workers = make(map[uint64]*WorkerInfo)
	c.WorkerTokens = make(map[string]uint64)
	c.NextWorkerID = 0

	c.server()
	return &c
}
