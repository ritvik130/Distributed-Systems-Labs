package mr

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"os"
	"time"
)

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