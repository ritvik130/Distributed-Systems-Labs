package mr

import (
	"fmt"
	"log"
	"time"
)

const (
	workerTimeout   = 5 * time.Second
	monitorInterval = 1 * time.Second
)

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

	workerID, exists := c.WorkerTokens[args.RegistrationToken]

	previousID := c.NextWorkerID
	var previousWorker WorkerInfo

	if exists {
		previousWorker = *c.Workers[workerID]
	}

	if !exists {
		c.NextWorkerID++
		workerID = c.NextWorkerID

		c.Workers[workerID] = &WorkerInfo{
			ID:       workerID,
			Address:  args.Address,
			Token:    args.RegistrationToken,
			LastSeen: time.Now(),
			Alive:    true,
		}

		c.WorkerTokens[args.RegistrationToken] = workerID

	} else {
		c.Workers[workerID].Address = args.Address
		c.Workers[workerID].LastSeen = time.Now()
		c.Workers[workerID].Alive = true
	}

	// A failed save must not leave an unconfirmed Worker registration in RAM.
	err := c.saveState()
	if err != nil {
		if !exists {
			delete(c.Workers, workerID)
			delete(c.WorkerTokens, args.RegistrationToken)
			c.NextWorkerID = previousID
		} else {
			*c.Workers[workerID] = previousWorker
		}

		return fmt.Errorf("error saving Worker registration: %w", err)
	}

	reply.WorkerID = workerID
	return nil
}

// Heartbeat updates Worker liveness and current task progress.
func (c *Coordinator) Heartbeat(
	args *HeartbeatArgs,
	reply *HeartbeatReply,
) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	worker, exists := c.Workers[args.WorkerID]

	if !exists {
		reply.Accepted = false
		return nil
	}

	worker.LastSeen = time.Now()
	worker.Alive = true
	reply.Accepted = true

	// Idle heartbeats only update liveness.
	if args.Stage == StageIdle {
		return nil
	}

	var task *Task

	if args.TaskType == MapTask {
		task = &c.MapTasks[args.TaskID]
	} else {
		task = &c.ReduceTasks[args.TaskID]
	}

	if task.State != InProgress {
		return nil
	}

	attempt := task.Attempts[args.AttemptID]

	// Ignore progress from invalidated or abandoned attempts.
	if attempt == nil || attempt.Abandoned {
		return nil
	}

	// Do not reset the stage timer for repeated or older heartbeats.
	if args.Stage > attempt.Stage {
		attempt.Stage = args.Stage
		attempt.StageStarted = time.Now()
	}

	return nil
}

// monitorWorkers checks worker liveness until the job finishes.
func (c *Coordinator) monitorWorkers() {
	ticker := time.NewTicker(monitorInterval)
	defer ticker.Stop()

	for range ticker.C {
		if c.Done() {
			return
		}
		c.checkWorkers()
	}
}

// checkWorkers marks workers as unreachable after the timeout.
func (c *Coordinator) checkWorkers() {
	c.mu.Lock()
	defer c.mu.Unlock()

	for workerID, worker := range c.Workers {
		if worker.Alive && time.Since(worker.LastSeen) >= workerTimeout {

			worker.Alive = false

			log.Printf("Worker %d is unreachable", workerID)
		}
	}
}

// ReportMissingIntermediate handles a Reduce worker that cannot fetch a Map output.
func (c *Coordinator) ReportMissingIntermediate(
	args *ReportMissingIntermediateArgs,
	reply *ReportMissingIntermediateReply,
) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	location := args.Location

	if args.ReduceTaskID < 0 || args.ReduceTaskID >= len(c.ReduceTasks) ||
		location.MapTaskID < 0 || location.MapTaskID >= len(c.MapTasks) {
		return nil
	}

	reduceTask := &c.ReduceTasks[args.ReduceTaskID]
	mapTask := &c.MapTasks[location.MapTaskID]

	attempt := reduceTask.Attempts[args.ReduceAttemptID]

	if reduceTask.State != InProgress ||
		attempt == nil || attempt.WorkerID != args.WorkerID {
		return nil
	}

	previousMap := *mapTask
	previousReduce := *reduceTask

	// Invalidate the Map output if it has not already been replaced.
	mapInvalidated := c.invalidateMapOutput(
		location.MapTaskID,
		location.AttemptID,
	)

	// Remove the failed Reduce attempt without interrupting other attempts.
	delete(reduceTask.Attempts, args.ReduceAttemptID)

	// No attempts left: the Reduce task goes back to Idle.
	if len(reduceTask.Attempts) == 0 {
		reduceTask.State = Idle
		reduceTask.LastAssigned = time.Time{}
	} else {
		// Keep the assignment time of the most recent remaining attempt.
		reduceTask.LastAssigned = time.Time{}

		for _, otherAttempt := range reduceTask.Attempts {
			if otherAttempt.AssignedAt.After(reduceTask.LastAssigned) {
				reduceTask.LastAssigned = otherAttempt.AssignedAt
			}
		}
	}

	// Persist the recovery state and roll back if saving fails.
	err := c.saveState()
	if err != nil {
		*mapTask = previousMap
		*reduceTask = previousReduce
		reduceTask.Attempts[args.ReduceAttemptID] = attempt

		return fmt.Errorf("error saving recovery state: %w", err)
	}

	if mapInvalidated {
		log.Printf("Map %d output lost, scheduling recovery",
			location.MapTaskID)
	}

	reply.Accepted = true
	return nil
}

// invalidateMapOutput resets a Map task whose accepted output was lost.
// The caller must hold c.mu.
func (c *Coordinator) invalidateMapOutput(mapID, attemptID int) bool {
	if mapID < 0 || mapID >= len(c.MapTasks) {
		return false
	}

	task := &c.MapTasks[mapID]

	// Ignore reports referring to an old or already invalidated result.
	if task.State != Completed || task.AcceptedAttempt != attemptID {
		return false
	}

	// Reset the task so the scheduler treats it as new work.
	task.State = Idle
	task.AcceptedAttempt = 0
	task.Attempts = nil
	task.OutputFiles = nil
	task.LastAssigned = time.Time{}
	task.CompletedDuration = 0

	return true
}
