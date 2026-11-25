package mr

import (
	"encoding/json"
	"fmt"
	"os"
)

// executeMap partitions Map output and reports the local intermediate files.
func executeMap(
    workerID uint64,
    data []byte,
    fileName string,
    mapf func(string, string) []KeyValue,
    reply *RequestTaskReply,
    progress *progressState,
) {
	if reply.NReduce <= 0 {
		fmt.Println("Invalid number of reduce tasks")
		progress.setStage(StageMapReport)
		reportTask(workerID, reply, false, nil)
		return
	}

	progress.setStage(StageMapExecute)
	kvs := mapf(fileName, string(data))

	// Hash each key so all values for that key reach the same Reduce task.
	progress.setStage(StageMapPartition)
	partitions := make([][]KeyValue, reply.NReduce)

	for _, kv := range kvs {
		partition := ihash(kv.Key) % reply.NReduce
		partitions[partition] = append(partitions[partition], kv)
	}

	intermediateFiles := make([]string, reply.NReduce)
	
	// Include the attempt ID to distinguish outputs from speculative copies.
	progress.setStage(StageMapWrite)
	for r, partition := range partitions {
		filename := fmt.Sprintf(
			"mr-%d-%d-%d",
			reply.TaskID,
			reply.AttemptID,
			r,
		)

		f, err := os.Create(filename)
		if err != nil {
			fmt.Println("Error creating intermediate file:", err)
			cleanupFiles(intermediateFiles)
			progress.setStage(StageMapReport)
			reportTask(workerID, reply, false, nil)
			return
		}

		encoder := json.NewEncoder(f)
		writeFailed := false

		for _, kv := range partition {
			if err := encoder.Encode(&kv); err != nil {
				fmt.Println("Error encoding intermediate data:", err)
				writeFailed = true
				break
			}
		}

		closeErr := f.Close()

		if closeErr != nil || writeFailed {
			if closeErr != nil {
				fmt.Println("Error closing intermediate file:", closeErr)
			}

			os.Remove(filename)
			cleanupFiles(intermediateFiles)
			progress.setStage(StageMapReport)
			reportTask(workerID, reply, false, nil)
			return
		}

		intermediateFiles[r] = filename
	}
	progress.setStage(StageMapReport)
	// Report only after all partition files are closed. Keep them until
	// the Coordinator confirms whether this attempt won.
	if !reportTask(workerID, reply, true, intermediateFiles) {
		cleanupFiles(intermediateFiles)
	}
}