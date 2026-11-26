package mr

import (
	"bufio"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"net"
	"net/http"
	"net/rpc"
	"sync"
	"time"
)

// call sends an RPC to the Coordinator with bounded connection and call times.
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

	// Establish the HTTP CONNECT tunnel used by Go RPC.
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

	// Use a fresh deadline for the RPC after the handshake.
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

// generateToken creates a token reused when the Worker re-registers.
func generateToken() (string, error) {
	token := make([]byte, 16)

	if _, err := rand.Read(token); err != nil {
		return "", err
	}

	return hex.EncodeToString(token), nil
}

// reportTask retries the same Map report until the Coordinator responds.
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

// reportMissingIntermediate identifies the failed Reduce attempt and missing Map output.
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

// heartbeat reports liveness and task progress independently of the task loop.
func heartbeat(
	workerID *uint64,
	workerMu *sync.Mutex,
	progress *progressState,
	registerWorkerArgs *RegisterWorkerArgs,
	stopHeartbeat <-chan struct{},
) {
	for {

		currentWorkerID := *workerID

		// Copy progress before the RPC so task execution can continue.
		heartbeatArgs := progress.snapshot()
		heartbeatArgs.WorkerID = currentWorkerID

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

			// Reuse the registration token to recover the existing Worker ID.
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
