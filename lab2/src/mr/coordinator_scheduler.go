package mr

import (
	"sort"
	"time"
)

const (
	minSpeculationTime = 4 * time.Second
	slowTaskFactor     = 2
)

// medianTaskDuration returns the median duration of completed tasks.
func medianTaskDuration(tasks []Task) time.Duration {
	durations := []time.Duration{}

	for _, task := range tasks {
		if task.State != Completed || task.CompletedDuration <= 0 {
			continue
		}

		durations = append(durations, task.CompletedDuration)
	}

	n := len(durations)

	if n == 0 {
		return 0
	}

	sort.Slice(durations, func(i, j int) bool {
		return durations[i] < durations[j]
	})

	if n%2 != 0 {
		return durations[n/2]
	}

	return (durations[n/2] + durations[n/2-1]) / 2
}

// liveAttempts counts non-abandoned attempts on live workers.
// Return the attempt itself if exactly one is alive.
func (c *Coordinator) liveAttempts(task *Task) (int, *AttemptInfo) {
	var numAlive int
	var onlyAttempt *AttemptInfo

	for _, attempt := range task.Attempts {
		worker := c.Workers[attempt.WorkerID]
		if !attempt.Abandoned && worker != nil && worker.Alive {
			numAlive++
			onlyAttempt = attempt
		}
	}

	if numAlive == 1 {
		return 1, onlyAttempt
	}

	return numAlive, nil
}

// findStraggler returns the task stuck longest in one stage, or -1.
func (c *Coordinator) findStraggler(tasks []Task, workerID uint64) int {
	median := medianTaskDuration(tasks)

	// Without completed tasks there is no baseline to compare against.
	if median == 0 {
		return -1
	}

	// Use the larger of the minimum time and twice the median duration.
	threshold := minSpeculationTime

	if median*slowTaskFactor > threshold {
		threshold = median * slowTaskFactor
	}

	bestTaskID := -1
	var longestStageDuration time.Duration

	for i := range tasks {
		task := &tasks[i]

		if task.State != InProgress {
			continue
		}

		numAlive, attempt := c.liveAttempts(task)

		// Allow at most one extra live attempt at the time of scheduling.
		if numAlive != 1 {
			continue
		}

		if attempt.WorkerID == workerID {
			continue
		}

		// Wait until progress information is available.
		if attempt.Stage == StageIdle || attempt.StageStarted.IsZero() {
			continue
		}

		// Do not duplicate work when the worker is already reporting or uploading.
		if attempt.Stage == StageMapReport ||
			attempt.Stage == StageReduceUpload {
			continue
		}

		stageDuration := time.Since(attempt.StageStarted)

		if stageDuration < threshold {
			continue
		}

		if stageDuration > longestStageDuration {
			bestTaskID = i
			longestStageDuration = stageDuration
		}
	}

	return bestTaskID
}

// abandonPreviousAttempts marks unfinished attempts left by a worker requesting new work.
// The caller must hold c.mu.
func (c *Coordinator) abandonPreviousAttempts(workerID uint64) error {
	abandoned := []*AttemptInfo{}

	for _, tasks := range [][]Task{c.MapTasks, c.ReduceTasks} {
		for _, task := range tasks {
			if task.State != InProgress {
				continue
			}

			for _, attempt := range task.Attempts {
				if attempt.WorkerID == workerID && !attempt.Abandoned {
					attempt.Abandoned = true
					abandoned = append(abandoned, attempt)
				}
			}
		}
	}

	if len(abandoned) == 0 {
		return nil
	}

	// Persist the change and undo it if saving fails.
	err := c.saveState()
	if err != nil {
		for _, attempt := range abandoned {
			attempt.Abandoned = false
		}
		return err
	}

	return nil
}

// selectTask prefers recovery, then idle tasks, then speculation.
// The caller must hold c.mu.
func (c *Coordinator) selectTask(tasks []Task, workerID uint64) int {
	// 1. Recovery: running tasks that have no live attempt left.
	for i := range tasks {
		task := &tasks[i]
		if task.State != InProgress {
			continue
		}

		numAlive, _ := c.liveAttempts(task)
		if numAlive == 0 {
			return i
		}
	}

	// 2. Any task that has not been started yet.
	for i := range tasks {
		if tasks[i].State == Idle {
			return i
		}
	}

	// 3. Speculative execution of a slow task.
	return c.findStraggler(tasks, workerID)
}
