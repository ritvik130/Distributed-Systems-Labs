package mr

import (
	"log"
	"os"
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
	ID       uint64
	Address  string
	Token    string
	LastSeen time.Time `json:"-"`
	Alive    bool      `json:"-"`
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
	StateFile    string // runtime config
}

// MakeCoordinator creates or restores a job and starts its RPC/HTTP server.
// nReduce determines how many Reduce tasks (output partitions) are created.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	c.InputBaseURL = os.Getenv("MR_COORDINATOR_PUBLIC_URL")
	c.StateFile = os.Getenv("MR_COORDINATOR_STATE_FILE")

	if c.StateFile == "" {
		c.StateFile = "coordinator-state.json"
	}

	_, err := os.Stat(c.StateFile)

	if err == nil {
		// Restore a previous Coordinator state.
		err = c.loadState()
		if err != nil {
			log.Fatalf("Error loading Coordinator state: %v", err)
		}

		// Make sure the saved state belongs to this job.
		if c.NReduce != nReduce || len(c.MapTasks) != len(files) || len(c.ReduceTasks) != nReduce {
			log.Fatal("Saved state does not match the current job")
		}

		for i, file := range files {
			if c.MapTasks[i].InputFile != file {
				log.Fatal("Saved input files do not match the current job")
			}
		}
		if c.Workers == nil || c.WorkerTokens == nil {
			log.Fatal("Saved worker state is missing")
		}

		// Give workers time to reconnect after a Coordinator restart.
		for _, worker := range c.Workers {
			worker.LastSeen = time.Now()
			worker.Alive = true
		}

	} else if os.IsNotExist(err) {
		// No saved state: initialize a new job.
		c.NReduce = nReduce

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

		// Save the initial job before accepting Worker requests.
		err = c.saveState()
		if err != nil {
			log.Fatalf("Error saving initial state: %v", err)
		}

	} else {
		log.Fatalf("Error checking state file: %v", err)
	}

	c.server()
	go c.monitorWorkers()
	return &c
}

// Done is polled by the coordinator main process until both phases finish.
func (c *Coordinator) Done() bool {

	c.mu.Lock()
	defer c.mu.Unlock()

	if len(c.ReduceTasks) > 0 {
		return allTasksCompleted(c.ReduceTasks)
	}

	return allTasksCompleted(c.MapTasks)
}
