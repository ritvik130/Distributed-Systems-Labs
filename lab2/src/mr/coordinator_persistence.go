package mr

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
)

// CoordinatorState contains only the data needed to resume a job.
// Runtime resources such as the mutex and HTTP server are not saved.
type CoordinatorState struct {
	MapTasks     []Task
	ReduceTasks  []Task
	NReduce      int
	Workers      map[uint64]*WorkerInfo
	WorkerTokens map[string]uint64
	NextWorkerID uint64
}

// saveState writes a consistent snapshot. The caller must hold c.mu if
// other goroutines can change Coordinator state concurrently.
func (c *Coordinator) saveState() error {
	if c.StateFile == "" {
		c.StateFile = "coordinator-state.json"
	}

	coordState := &CoordinatorState{
		MapTasks:     c.MapTasks,
		ReduceTasks:  c.ReduceTasks,
		NReduce:      c.NReduce,
		Workers:      c.Workers,
		WorkerTokens: c.WorkerTokens,
		NextWorkerID: c.NextWorkerID,
	}

	data, err := json.MarshalIndent(coordState, "", "  ")
	if err != nil {
		return fmt.Errorf("error marshaling state: %w", err)
	}

	// Write to a temporary file in the same directory as the state file.
	tempFile, err := os.CreateTemp(filepath.Dir(c.StateFile), "temp-cord-state-*.json")
	if err != nil {
		return fmt.Errorf("error creating temp file: %w", err)
	}

	tempName := tempFile.Name()
	defer os.Remove(tempName)

	_, err = tempFile.Write(data)
	if err != nil {
		tempFile.Close()
		return fmt.Errorf("error writing state: %w", err)
	}

	if err := tempFile.Sync(); err != nil {
		tempFile.Close()
		return fmt.Errorf("error syncing state: %w", err)
	}

	if err := tempFile.Close(); err != nil {
		return fmt.Errorf("error closing state file: %w", err)
	}

	// Open the directory before replacing the previous snapshot.
	dir, err := os.Open(filepath.Dir(c.StateFile))
	if err != nil {
		return fmt.Errorf("error opening state directory: %w", err)
	}
	defer dir.Close()

	// Atomically replace the previous state file.
	if err := os.Rename(tempName, c.StateFile); err != nil {
		return fmt.Errorf("error renaming state file: %w", err)
	}

	// After rename, the new state may already be on disk. Stop rather
	// than let callers roll back RAM to a potentially different state.
	if err := dir.Sync(); err != nil {
		log.Fatalf("error syncing state directory after rename: %v", err)
	}

	return nil
}

// loadState restores a saved job before the Coordinator starts serving requests.
func (c *Coordinator) loadState() error {
	if c.StateFile == "" {
		c.StateFile = "coordinator-state.json"
	}

	jsonData, err := os.ReadFile(c.StateFile)
	if err != nil {
		return fmt.Errorf("error opening state file: %w", err)
	}

	var state CoordinatorState
	if err := json.Unmarshal(jsonData, &state); err != nil {
		return fmt.Errorf("error loading state: %w", err)
	}

	// Reject incomplete state before modifying the live Coordinator.
	if state.Workers == nil || state.WorkerTokens == nil ||
		len(state.ReduceTasks) != state.NReduce || state.NReduce < 0 {
		return fmt.Errorf("invalid saved Coordinator state")
	}

	c.MapTasks = state.MapTasks
	c.ReduceTasks = state.ReduceTasks
	c.NReduce = state.NReduce
	c.Workers = state.Workers
	c.WorkerTokens = state.WorkerTokens
	c.NextWorkerID = state.NextWorkerID

	return nil
}
