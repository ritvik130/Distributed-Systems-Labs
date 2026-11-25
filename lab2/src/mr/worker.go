package mr

import (
	"bufio"
	"bytes"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sort"
	"strings"
	"sync"
	"time"
)

var coordinatorAddr = envOrDefault(
	"MR_COORDINATOR_RPC_ADDR",
	"localhost:5000",
)

var coordinatorHTTPURL = strings.TrimRight(
	envOrDefault("MR_COORDINATOR_PUBLIC_URL", "http://localhost:8081"),
	"/",
)

type KeyValue struct {
	Key   string
	Value string
}

func envOrDefault(key, fallback string) string {
	value := os.Getenv(key)
	if value == "" {
		return fallback
	}
	return value
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
	// Generate one registration token per Worker process.
	token, err := generateToken()
	if err != nil {
		fmt.Println("Error generating registration token:", err)
		return
	}

	// Start the HTTP server that serves intermediate files.
	address, err := startWorkerServer()
	if err != nil {
		fmt.Println("Error starting Worker server:", err)
		return
	}

	if address == "" {
		fmt.Println("Error retrieving Worker address")
		return
	}

	registerWorkerArgs := &RegisterWorkerArgs{
		RegistrationToken: token,
		Address:           address,
	}

	registerWorkerReply := new(RegisterWorkerReply)

	// Keep trying registration if the Coordinator is unavailable.
	for {
		ok := call(
			coordinatorAddr,
			"Coordinator.RegisterWorker",
			registerWorkerArgs,
			registerWorkerReply,
		)

		if ok {
			break
		}

		fmt.Println("Error registering Worker. Retrying...")
		time.Sleep(time.Second)
	}

	fmt.Println("Registered Worker with ID:", registerWorkerReply.WorkerID)

	// WorkerID is shared between the main goroutine and heartbeat.
	var workerMu sync.Mutex
	workerID := registerWorkerReply.WorkerID

	stopHeartbeat := make(chan struct{})
	go heartbeat(
		&workerID,
		&workerMu,
		registerWorkerArgs,
		stopHeartbeat,
	)

	defer close(stopHeartbeat)

taskLoop:
	for {
		workerMu.Lock()
		currentWorkerID := workerID
		workerMu.Unlock()

		args := &RequestTaskArgs{
			WorkerID: currentWorkerID,
		}

		reply := new(RequestTaskReply)

		ok := call(
			coordinatorAddr,
			"Coordinator.RequestTask",
			args,
			reply,
		)

		if !ok {
			fmt.Println("Error requesting task. Retrying...")
			time.Sleep(time.Second)
			continue
		}

		switch reply.Type {

		case MapTask:
			// Keep retrying the same input download.
			var data []byte

			for {
				data, err = getFile(reply.InputURL)

				if err == nil {
					break
				}

				fmt.Println("Error downloading Map input:", err)
				time.Sleep(time.Second)
			}

			executeMap(
				currentWorkerID,
				data,
				reply.InputFile,
				mapf,
				reply,
			)

		case ReduceTask:
			reduceData := make([][]byte, len(reply.IntermediateLocations))

			// Download each required intermediate file.
			for i, location := range reply.IntermediateLocations {
				var data []byte
				var downloadErr error

				// Retry a few times for transient errors.
				const maxDownloadAttempts = 3

				for attempt := 1; attempt <= maxDownloadAttempts; attempt++ {
					data, downloadErr = getIntermediateFiles(location)

					if downloadErr == nil {
						break
					}

					fmt.Printf(
						"Intermediate download attempt %d failed: %v\n",
						attempt,
						downloadErr,
					)

					if attempt < maxDownloadAttempts {
						time.Sleep(time.Second)
					}
				}

				if downloadErr != nil {
					fmt.Println("Intermediate file unavailable. Reporting to Coordinator...")

					accepted := reportMissingIntermediate(
						currentWorkerID,
						reply,
						location,
					)

					if accepted {
						fmt.Println("Coordinator accepted missing-intermediate report")
					} else {
						fmt.Println("Coordinator rejected or marked report stale")
					}

					// Do not execute a partial Reduce task.
					continue taskLoop
				}

				reduceData[i] = data
			}

			executeReduce(
				currentWorkerID,
				reduceData,
				reducef,
				reply,
			)

		case WaitTask:
			time.Sleep(300 * time.Millisecond)

		case ExitTask:
			fmt.Println("Coordinator says the job is complete. Exiting.")
			return

		default:
			fmt.Println("Unknown task type:", reply.Type)
			time.Sleep(300 * time.Millisecond)
		}
	}
}

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

func reportTask(
	workerID uint64,
	task *RequestTaskReply,
	success bool,
	outputFiles []string,
) bool {
	args := &ReportTaskArgs{
		WorkerID:    workerID,
		Type:        task.Type,
		TaskID:      task.TaskID,
		AttemptID:   task.AttemptID,
		Success:     success,
		OutputFiles: outputFiles,
	}

	for {
		reply := new(ReportTaskReply)

		if call(coordinatorAddr, "Coordinator.ReportTask", args, reply) {
			if !reply.Accepted {
				fmt.Println("Coordinator rejected task result")
			}

			return reply.Accepted
		}

		fmt.Println("Error reporting Map task. Retrying...")
		time.Sleep(time.Second)
	}
}

func reportMissingIntermediate(
	workerID uint64,
	task *RequestTaskReply,
	location FileLocation,
) bool {
	args := &ReportMissingIntermediateArgs{
		WorkerID:        workerID,
		ReduceTaskID:    task.TaskID,
		ReduceAttemptID: task.AttemptID,
		Location:        location,
	}

	for {
		reply := new(ReportMissingIntermediateReply)

		if call(
			coordinatorAddr,
			"Coordinator.ReportMissingIntermediate",
			args,
			reply,
		) {
			return reply.Accepted
		}

		fmt.Println("Error reporting missing intermediate file. Retrying...")
		time.Sleep(time.Second)
	}
}

func cleanupFiles(files []string) {
	for _, filename := range files {
		if filename == "" {
			continue
		}

		if err := os.Remove(filename); err != nil && !os.IsNotExist(err) {
			fmt.Println("Error removing file:", err)
		}
	}
}

func call(
	address string,
	rpcname string,
	args interface{},
	reply interface{},
) bool {
	const timeout = 5 * time.Second

	conn, err := net.DialTimeout("tcp", address, timeout)
	if err != nil {
		fmt.Println("Error dialing Coordinator:", err)
		return false
	}
	defer conn.Close()

	if err := conn.SetDeadline(time.Now().Add(timeout)); err != nil {
		return false
	}

	_, err = fmt.Fprintf(
		conn,
		"CONNECT %s HTTP/1.0\n\n",
		rpc.DefaultRPCPath,
	)
	if err != nil {
		fmt.Println("Error sending RPC handshake:", err)
		return false
	}

	response, err := http.ReadResponse(
		bufio.NewReader(conn),
		&http.Request{Method: "CONNECT"},
	)
	if err != nil {
		fmt.Println("Error receiving RPC handshake:", err)
		return false
	}

	if response.StatusCode != http.StatusOK {
		fmt.Println("RPC handshake rejected:", response.Status)
		return false
	}

	if err := conn.SetDeadline(time.Now().Add(timeout)); err != nil {
		return false
	}

	client := rpc.NewClient(conn)
	defer client.Close()

	if err := client.Call(rpcname, args, reply); err != nil {
		fmt.Println("RPC error:", err)
		return false
	}

	return true
}

func generateToken() (string, error) {
	token := make([]byte, 16)

	if _, err := rand.Read(token); err != nil {
		return "", err
	}

	return hex.EncodeToString(token), nil
}

func getFile(url string) ([]byte, error) {
	client := &http.Client{
		Timeout: 10 * time.Second,
	}

	resp, err := client.Get(url)
	if err != nil {
		return nil, fmt.Errorf("HTTP request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("bad HTTP status: %s", resp.Status)
	}

	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed reading response: %w", err)
	}

	return data, nil
}

func getIntermediateFiles(location FileLocation) ([]byte, error) {
	url := fmt.Sprintf(
		"http://%s/intermediate/%s",
		location.Address,
		location.FileName,
	)

	return getFile(url)
}

func sendFile(
	workerID uint64,
	task *RequestTaskReply,
	outputFile string,
) (bool, error) {
	url := fmt.Sprintf(
		"%s/outputs?task=%d&attempt=%d&worker=%d",
		coordinatorHTTPURL,
		task.TaskID,
		task.AttemptID,
		workerID,
	)

	fileData, err := os.ReadFile(outputFile)
	if err != nil {
		return false, fmt.Errorf("reading Reduce output: %w", err)
	}

	client := &http.Client{
		Timeout: 10 * time.Second,
	}

	for {
		req, err := http.NewRequest(
			http.MethodPost,
			url,
			bytes.NewReader(fileData),
		)
		if err != nil {
			return false, err
		}

		resp, err := client.Do(req)

		if err != nil {
			fmt.Println("Reduce upload network error:", err)
			time.Sleep(time.Second)
			continue
		}

		status := resp.StatusCode
		resp.Body.Close()

		switch status {
		case http.StatusOK:
			return true, nil

		case http.StatusConflict:
			fmt.Println("Another Reduce attempt already won")
			return false, nil

		case http.StatusForbidden:
			fmt.Println("Coordinator rejected stale or invalid Reduce attempt")
			return false, nil

		case http.StatusBadRequest:
			return false, fmt.Errorf("Coordinator rejected upload: HTTP 400")

		default:
			if status >= 500 {
				fmt.Println("Coordinator upload error:", status, "- retrying")
				time.Sleep(time.Second)
				continue
			}

			return false, fmt.Errorf("unexpected upload status: %d", status)
		}
	}
}

func heartbeat(
	workerID *uint64,
	workerMu *sync.Mutex,
	registerWorkerArgs *RegisterWorkerArgs,
	stopHeartbeat <-chan struct{},
) {
	for {
		workerMu.Lock()
		currentWorkerID := *workerID
		workerMu.Unlock()

		heartbeatArgs := &HeartbeatArgs{
			WorkerID: currentWorkerID,
		}

		heartbeatReply := new(HeartbeatReply)

		ok := call(
			coordinatorAddr,
			"Coordinator.Heartbeat",
			heartbeatArgs,
			heartbeatReply,
		)

		if !ok {
			// Coordinator may be temporarily unavailable.
			// Keep the Worker alive and retry.
			fmt.Println("Heartbeat failed; Coordinator may be unavailable")
		} else if !heartbeatReply.Accepted {
			fmt.Println("Coordinator rejected heartbeat. Re-registering...")

			newReply := new(RegisterWorkerReply)

			reRegister := call(
				coordinatorAddr,
				"Coordinator.RegisterWorker",
				registerWorkerArgs,
				newReply,
			)

			if !reRegister {
				fmt.Println("Worker re-registration failed")
			} else {
				workerMu.Lock()
				*workerID = newReply.WorkerID
				workerMu.Unlock()

				fmt.Println("Worker re-registered with ID:", newReply.WorkerID)
			}
		}

		select {
		case <-stopHeartbeat:
			fmt.Println("Heartbeat stopped")
			return

		case <-time.After(time.Second):
		}
	}
}
