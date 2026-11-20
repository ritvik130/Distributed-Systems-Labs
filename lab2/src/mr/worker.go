package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"net/rpc"
	"os"
	"sort"
	"time"
)

type KeyValue struct {
	Key   string
	Value string
}

func ihash(key string) int {
	h := fnv.New32a()
	_, _ = h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

func Worker(
	mapf func(string, string) []KeyValue,
	reducef func(string, []string) string,
) {
	for {
		args := new(RequestTaskArgs)
		reply := new(RequestTaskReply)

		ok := call("Coordinator.RequestTask", args, reply)
		if !ok {
			fmt.Println("Error requesting task")
			time.Sleep(time.Second)
			continue
		}

		switch reply.Type {
		case MapTask:
			executeMap(reply.InputFile, mapf, reply)

		case ReduceTask:
			executeReduce(reply.IntermediateFiles, reducef, reply)

		case WaitTask:
			time.Sleep(300 * time.Millisecond)

		case ExitTask:
			fmt.Println("Exiting")
			return

		default:
			fmt.Println("Unknown task type:", reply.Type)
			time.Sleep(time.Second)
		}
	}
}

func executeMap(
	fileName string,
	mapf func(string, string) []KeyValue,
	reply *RequestTaskReply,
) {
	if reply.NReduce <= 0 {
		fmt.Println("Invalid number of reduce tasks")
		reportTask(reply, false, nil)
		return
	}

	content, err := os.ReadFile(fileName)
	if err != nil {
		fmt.Println("Error reading input file:", err)
		reportTask(reply, false, nil)
		return
	}

	kvs := mapf(fileName, string(content))

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
			reportTask(reply, false, nil)
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
			reportTask(reply, false, nil)
			return
		}

		intermediateFiles[r] = filename
	}

	if !reportTask(reply, true, intermediateFiles) {
		cleanupFiles(intermediateFiles)
	}
}

func executeReduce(
	fileNames []string,
	reducef func(string, []string) string,
	reply *RequestTaskReply,
) {
	data := make(map[string][]string)

	for _, fileName := range fileNames {
		file, err := os.Open(fileName)
		if err != nil {
			fmt.Println("Error opening intermediate file:", err)
			reportTask(reply, false, nil)
			return
		}

		decoder := json.NewDecoder(file)

		for {
			var kv KeyValue
			err := decoder.Decode(&kv)

			if err == io.EOF {
				break
			}

			if err != nil {
				fmt.Println("Error decoding intermediate data:", err)
				file.Close()
				reportTask(reply, false, nil)
				return
			}

			data[kv.Key] = append(data[kv.Key], kv.Value)
		}

		if err := file.Close(); err != nil {
			fmt.Println("Error closing intermediate file:", err)
			reportTask(reply, false, nil)
			return
		}
	}

	outputFile := fmt.Sprintf(
		"mr-out-%d-a%d",
		reply.TaskID,
		reply.AttemptID,
	)

	f, err := os.Create(outputFile)
	if err != nil {
		fmt.Println("Error creating reduce output:", err)
		reportTask(reply, false, nil)
		return
	}

	// Sort keys to make output deterministic.
	keys := make([]string, 0, len(data))
	for key := range data {
		keys = append(keys, key)
	}
	sort.Strings(keys)

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
	outputFiles := []string{outputFile}

	if closeErr != nil || writeFailed {
		if closeErr != nil {
			fmt.Println("Error closing reduce output:", closeErr)
		}
		cleanupFiles(outputFiles)
		reportTask(reply, false, nil)
		return
	}

	if !reportTask(reply, true, outputFiles) {
		cleanupFiles(outputFiles)
	}
}

func reportTask(
	task *RequestTaskReply,
	success bool,
	outputFiles []string,
) bool {
	args := &ReportTaskArgs{
		Type:        task.Type,
		TaskID:      task.TaskID,
		AttemptID:   task.AttemptID,
		Success:     success,
		OutputFiles: outputFiles,
	}

	for {
		reply := new(ReportTaskReply)

		if call("Coordinator.ReportTask", args, reply) {
			if !reply.Accepted {
				fmt.Println("Coordinator rejected task result")
			}
			return reply.Accepted
		}

		// Do not delete files here. The coordinator may have
		// received the report even if its response was lost.
		fmt.Println("Error reporting task; retrying")
		time.Sleep(time.Second)
	}
}

func cleanupFiles(files []string) {
	for _, filename := range files {
		if filename != "" {
			if err := os.Remove(filename); err != nil && !os.IsNotExist(err) {
				fmt.Println("Error removing file:", err)
			}
		}
	}
}

func CallExample() {
	args := ExampleArgs{X: 99}
	reply := ExampleReply{}

	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

func call(rpcname string, args interface{}, reply interface{}) bool {
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		fmt.Println("Error dialing coordinator:", err)
		return false
	}
	defer c.Close()

	if err := c.Call(rpcname, args, reply); err != nil {
		fmt.Println("RPC error:", err)
		return false
	}

	return true
}
