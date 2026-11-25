package mr

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"sort"
)

func executeReduce(
	workerID uint64,
	content [][]byte,
	reducef func(string, []string) string,
	reply *RequestTaskReply,
) {
	data := make(map[string][]string)

	for _, c := range content {
		decoder := json.NewDecoder(bytes.NewReader(c))

		for {
			var kv KeyValue
			err := decoder.Decode(&kv)

			if err == io.EOF {
				break
			}

			if err != nil {
				fmt.Println("Error decoding intermediate data:", err)
				return
			}

			data[kv.Key] = append(data[kv.Key], kv.Value)
		}
	}

	keys := make([]string, 0, len(data))

	for key := range data {
		keys = append(keys, key)
	}

	sort.Strings(keys)

	outputFile := fmt.Sprintf(
		"reduce-%d-a%d",
		reply.TaskID,
		reply.AttemptID,
	)

	f, err := os.Create(outputFile)
	if err != nil {
		fmt.Println("Error creating reduce output:", err)
		return
	}

	writeFailed := false

	for _, key := range keys {
		result := reducef(key, data[key])

		if _, err := fmt.Fprintf(f, "%v %v\n", key, result); err != nil {
			fmt.Println("Error writing reduce output:", err)
			writeFailed = true
			break
		}
	}

	closeErr := f.Close()

	if closeErr != nil || writeFailed {
		if closeErr != nil {
			fmt.Println("Error closing reduce output:", closeErr)
		}

		cleanupFiles([]string{outputFile})
		return
	}

	accepted, err := sendFile(workerID, reply, outputFile)

	if err != nil {
		fmt.Println("Error uploading Reduce output:", err)
		// Retain the output for inspection.
		return
	}

	if accepted {
		fmt.Println("Reduce output accepted")
	} else {
		fmt.Println("Reduce output was not accepted; another attempt may have won")
	}

	// 200, 403 and 409 are definitive responses.
	cleanupFiles([]string{outputFile})
}