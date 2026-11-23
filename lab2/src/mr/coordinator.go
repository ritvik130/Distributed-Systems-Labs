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

// TaskState tracks whether a task is waiting, running, or finished.
type TaskState string

const (
	Idle       TaskState = "idle"
	InProgress TaskState = "in_progress"
	Completed  TaskState = "completed"
)

// Task keeps the state and attempt history of a Map or Reduce task.
type Task struct {
	State           TaskState
	InputFile       string    // Map tasks only
	LastAssigned    time.Time // used to detect timed-out workers
	AttemptsIssued  int       // next assigned attempt uses this counter
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

// Coordinator assigns tasks and keeps track of workers and accepted results.
type Coordinator struct {
	mu          sync.Mutex
	MapTasks    []Task
	ReduceTasks []Task
	NReduce     int

	// Worker registration and reconnect information.
	Workers      map[uint64]*WorkerInfo
	WorkerTokens map[string]uint64
	NextWorkerID uint64

	InputBaseURL string // HTTP address used to download Map inputs
}

// allTasksCompleted checks whether a phase has finished.
func allTasksCompleted(tasks []Task) bool {
	for _, task := range tasks {
		if task.State != Completed {
			return false
		}
	}

	return true
}

// RequestTask assigns Map tasks first, then Reduce tasks once all Maps finish.
// Unfinished tasks can be assigned again after 10 seconds; each assignment
// gets a new AttemptID so the first accepted result can be identified.
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

	// Assign an idle Map task, or retry one that has been running too long.
	for i := range c.MapTasks {
		task := &c.MapTasks[i]

		if task.State == Idle ||
			(task.State == InProgress && time.Since(task.LastAssigned) >= 10*time.Second) {
			task.State = InProgress
			task.AttemptsIssued++
			task.LastAssigned = time.Now()

			// Remember which worker owns this attempt.
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

			// The Worker downloads input from the Coordinator, not a shared disk.
			if c.InputBaseURL != "" {
				reply.InputURL = fmt.Sprintf("%s/inputs/%d", c.InputBaseURL, i)
			}

			return nil
		}
	}

	// Wait until all Map results have been accepted.
	if !allTasksCompleted(c.MapTasks) {
		reply.Type = WaitTask
		return nil
	}

	// Each Reduce task receives the accepted Map output for its partition.
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

			// Give the Reduce Worker the locations of its intermediate files.
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

				// Keep the Basic field until the Worker switches to locations.
				reply.IntermediateFiles = append(
					reply.IntermediateFiles,
					mapTask.OutputFiles[i],
				)
			}

			return nil
		}
	}

	// Wait for the remaining Reduce results.
	if !allTasksCompleted(c.ReduceTasks) {
		reply.Type = WaitTask
		return nil
	}

	// Both phases are complete.
	reply.Type = ExitTask
	return nil
}

// ReportTask accepts finished Map tasks. Reduce results use HTTP POST instead.
// Only a valid successful attempt is accepted; retries from its worker are safe.
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

	// Reject reports from unknown attempts or a different worker.
	attempt, exists := task.Attempts[args.AttemptID]

	if !exists || attempt == nil || attempt.WorkerID != args.WorkerID {
		return nil
	}

	// A lost RPC reply should not cause the winning Worker to discard its files.
	if task.State == Completed {
		if args.Success && args.AttemptID == task.AcceptedAttempt {
			reply.Accepted = true
		}
		return nil
	}

	if !args.Success {
		return nil
	}

	// Map outputs are ordered by Reduce partition.
	if len(args.OutputFiles) != c.NReduce {
		return nil
	}

	// Record the accepted output locations for future Reduce assignments.
	task.State = Completed
	task.AcceptedAttempt = args.AttemptID
	task.OutputFiles = args.OutputFiles
	reply.Accepted = true

	return nil
}

// RegisterWorker gives each process a stable ID while it reuses its token.
// A repeated registration updates its address instead of creating another ID.
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

// Example is the original sample RPC handler kept for compatibility.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// serveMapInput sends the original file for a Map task over HTTP GET.
// Workers request a task ID rather than supplying an arbitrary file path.
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

// serveReduceOutput receives a final Reduce result over HTTP POST.
// It validates the attempt and publishes only the first accepted output.
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

	// Check the attempt before accepting an upload.
	c.mu.Lock()
	task := &c.ReduceTasks[taskID]
	attempt, exists := task.Attempts[attemptID]

	if !exists || attempt == nil || attempt.WorkerID != workerID {
		c.mu.Unlock()
		http.Error(w, "Invalid worker or attempt", http.StatusForbidden)
		return
	}

	// Accept retries from the winner without overwriting mr-out-X.
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

	// Stage the upload locally so a partial transfer cannot become final output.
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

	// Another attempt may have finished while this Worker was uploading.
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

	// Publish the winner and update its state while holding the same lock.
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

// server serves RPC and file transfers through HTTP, preserving the Unix
// listener for Basic compatibility and enabling TCP when configured.
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()

	// Register file-transfer routes alongside the RPC endpoint.
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

// Done is polled by the coordinator main process until both phases finish.
func (c *Coordinator) Done() bool {

	c.mu.Lock()
	defer c.mu.Unlock()

	return allTasksCompleted(c.MapTasks) && allTasksCompleted(c.ReduceTasks)
}

// MakeCoordinator creates a new job and starts its RPC/HTTP server.
// nReduce determines how many Reduce tasks (output partitions) are created.
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
