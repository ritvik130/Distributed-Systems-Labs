package mr

import (
	"fmt"
	"hash/fnv"
	"os"
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