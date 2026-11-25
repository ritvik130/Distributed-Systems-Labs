package mr

import (
	"fmt"
	"time"
)

// RequestTask assigns Map tasks first, then Reduce tasks once all Maps finish.
func (c *Coordinator) RequestTask(
	args *RequestTaskArgs,
	reply *RequestTaskReply,
) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	worker, ok := c.Workers[args.WorkerID]
	if !ok {
		return fmt.Errorf("Worker %d is not registered", args.WorkerID)
	}

	worker.LastSeen = time.Now()
	worker.Alive = true

	// Final outputs are sufficient even if a Map result was invalidated.
	if len(c.ReduceTasks) > 0 && allTasksCompleted(c.ReduceTasks) {
		reply.Type = ExitTask
		return nil
	}

	// Workers request new work only after leaving their previous attempt.
	err := c.abandonPreviousAttempts(args.WorkerID)
	if err != nil {
		return fmt.Errorf("error saving abandoned attempts: %w", err)
	}

	// Map phase.
	if !allTasksCompleted(c.MapTasks) {
		mapID := c.selectTask(c.MapTasks, args.WorkerID)
		if mapID == -1 {
			reply.Type = WaitTask
			return nil
		}

		task := &c.MapTasks[mapID]
		err := c.assignAttempt(task, args.WorkerID)

		if err != nil {
			return fmt.Errorf("error saving Map assignment: %w", err)
		}

		reply.Type = MapTask
		reply.TaskID = mapID
		reply.AttemptID = task.AttemptsIssued
		reply.InputFile = task.InputFile
		reply.NReduce = c.NReduce

		// Remote workers download the input over HTTP.
		if c.InputBaseURL != "" {
			reply.InputURL = fmt.Sprintf("%s/inputs/%d", c.InputBaseURL, mapID)
		}
		return nil
	}

	// Reduce phase.
	if !allTasksCompleted(c.ReduceTasks) {
		reduceID := c.selectTask(c.ReduceTasks, args.WorkerID)
		if reduceID == -1 {
			reply.Type = WaitTask
			return nil
		}

		task := &c.ReduceTasks[reduceID]
		err := c.assignAttempt(task, args.WorkerID)

		if err != nil {
			return fmt.Errorf("error saving Reduce assignment: %w", err)
		}

		reply.Type = ReduceTask
		reply.TaskID = reduceID
		reply.AttemptID = task.AttemptsIssued

		// Give the Reduce Worker the locations of its intermediate files.
		for mapID, mapTask := range c.MapTasks {
			attemptID := mapTask.AcceptedAttempt
			workerID := mapTask.Attempts[attemptID].WorkerID
			location := FileLocation{
				MapTaskID: mapID,
				AttemptID: attemptID,
				WorkerID:  workerID,
				Address:   c.Workers[workerID].Address,
				FileName:  mapTask.OutputFiles[reduceID],
			}
			reply.IntermediateLocations = append(reply.IntermediateLocations, location)
		}
		return nil
	}

	reply.Type = ExitTask
	return nil
}

// assignAttempt creates and persists a new attempt. The caller must hold c.mu.
func (c *Coordinator) assignAttempt(task *Task, workerID uint64) error {
	previous := *task

	task.State = InProgress
	task.AttemptsIssued++
	task.LastAssigned = time.Now()
	if task.Attempts == nil {
		task.Attempts = make(map[int]*AttemptInfo)
	}
	task.Attempts[task.AttemptsIssued] = &AttemptInfo{WorkerID: workerID, AssignedAt: task.LastAssigned}

	// Undo the assignment if it cannot be saved.
	err := c.saveState()
	if err != nil {
		delete(task.Attempts, task.AttemptsIssued)
		*task = previous
		return err
	}
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

	if args.Type != MapTask {
		return nil
	}

	if args.TaskID < 0 || args.TaskID >= len(c.MapTasks) {
		return nil
	}

	task := &c.MapTasks[args.TaskID]

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

	// A failed attempt is handled when the worker requests new work or times out.
	if !args.Success {
		return nil
	}

	// Map outputs are ordered by Reduce partition.
	if len(args.OutputFiles) != c.NReduce {
		return nil
	}

	previous := *task
	task.State = Completed
	task.AcceptedAttempt = args.AttemptID
	task.OutputFiles = args.OutputFiles
	task.CompletedDuration = time.Since(attempt.AssignedAt)

	// Roll back the completion if it cannot be persisted.
	err := c.saveState()
	if err != nil {
		*task = previous
		return fmt.Errorf("error saving state: %w", err)
	}

	reply.Accepted = true

	return nil
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
