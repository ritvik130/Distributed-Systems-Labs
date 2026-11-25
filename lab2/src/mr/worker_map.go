package mr

import (
	"encoding/json"
	"fmt"
	"os"
)

func executeMap(
	workerID uint64,
	data []byte,
	fileName string,
	mapf func(string, string) []KeyValue,
	reply *RequestTaskReply,
) {
	if reply.NReduce <= 0 {
		fmt.Println("Invalid number of reduce tasks")
		reportTask(workerID, reply, false, nil)
		return
	}

	kvs := mapf(fileName, string(data))

	partitions := make([][]KeyValue, reply.NReduce)

	for _, kv := range kvs {
		partition := ihash(kv.Key) % reply.NReduce
		partitions[partition] = append(partitions[partition], kv)
	}

	intermediateFiles := make([]string, reply.NReduce)

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
			reportTask(workerID, reply, false, nil)
			return
		}

		intermediateFiles[r] = filename
	}

	// Keep files if the RPC response is lost.
	// The Coordinator may already have accepted the result.
	if !reportTask(workerID, reply, true, intermediateFiles) {
		cleanupFiles(intermediateFiles)
	}
}