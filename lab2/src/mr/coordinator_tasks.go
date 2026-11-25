package mr

import (
	"fmt"
	"time"
)

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

	// Final outputs are sufficient even if a Map result was invalidated.
	if len(c.ReduceTasks) > 0 && allTasksCompleted(c.ReduceTasks) {
		reply.Type = ExitTask
		return nil
	}

	// Assign an idle Map task, or retry one that has been running too long.
	for i := range c.MapTasks {
		task := &c.MapTasks[i]

		if task.State == Idle ||
			(task.State == InProgress && time.Since(task.LastAssigned) >= 10*time.Second) {
			previous := *task
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

			// Do not keep an assignment in RAM if saving it fails.
			err := c.saveState()
			if err != nil {
				delete(task.Attempts, task.AttemptsIssued)
				*task = previous
				return fmt.Errorf("error saving Map assignment: %w", err)
			}

			// Send the assigned task to the Worker.
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
			previous := *task
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

			// Save Reduce attempts too; restore RAM if the write fails.
			err := c.saveState()
			if err != nil {
				delete(task.Attempts, task.AttemptsIssued)
				*task = previous
				return fmt.Errorf("error saving Reduce assignment: %w", err)
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
	previous := *task
	task.State = Completed
	task.AcceptedAttempt = args.AttemptID
	task.OutputFiles = args.OutputFiles

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
