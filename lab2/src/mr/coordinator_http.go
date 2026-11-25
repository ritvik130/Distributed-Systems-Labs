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
)

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
	addr := os.Getenv("MR_COORDINATOR_HTTP_ADDR")
	if addr != "" {
		tcpListener, err := net.Listen("tcp", addr)
		if err != nil {
			log.Fatal("TCP listen error:", err)
		}

		go http.Serve(tcpListener, nil)
	}
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

	c.mu.Lock()
	fileName := c.MapTasks[taskID].InputFile
	c.mu.Unlock()

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

	// Use a separate temporary file for each HTTP request, including retries.
	outFile, err := os.CreateTemp(".", "reduce-upload-")
	if err != nil {
		http.Error(w, "Cannot create output file", http.StatusInternalServerError)
		return
	}

	tempName := outFile.Name()
	defer os.Remove(tempName)

	_, copyErr := io.Copy(outFile, r.Body)
	var syncErr error
	if copyErr == nil {
		syncErr = outFile.Sync()
	}
	closeErr := outFile.Close()

	if copyErr != nil || syncErr != nil || closeErr != nil {
		http.Error(w, "Failed to save output", http.StatusInternalServerError)
		return
	}

	// Another attempt may have finished while this Worker was uploading.
	c.mu.Lock()

	// Recheck the winner after uploading, since the mutex was released.
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

	// Make the final filename durable before marking the task completed.
	dir, err := os.Open(".")
	if err != nil {
		c.mu.Unlock()
		http.Error(w, "Failed to open output directory", http.StatusInternalServerError)
		return
	}
	syncErr = dir.Sync()
	dir.Close()
	if syncErr != nil {
		c.mu.Unlock()
		http.Error(w, "Failed to sync output directory", http.StatusInternalServerError)
		return
	}

	previous := *task
	task.State = Completed
	task.AcceptedAttempt = attemptID
	task.OutputFiles = []string{finalName}

	err = c.saveState()
	if err != nil {
		*task = previous
		// The output is not committed in the state; a later upload can replace it.
		c.mu.Unlock()
		http.Error(w, "Failed to save Coordinator state", http.StatusInternalServerError)
		return
	}

	c.mu.Unlock()
	fmt.Fprintln(w, "Success!")

}
