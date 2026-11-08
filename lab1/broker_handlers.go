package main

import (
	"fmt"
	"io"
	"net"
	"net/http"
	"strconv"
)

// Register a subscriber and send its new ID back to the client.
func handleSubscribe(conn net.Conn, req *http.Request, broker *Broker) {
	topicName := req.URL.Query().Get("topic")

	if topicName == "" {
		sendError(conn, 400)
		return
	}

	id := broker.Subscribe(topicName)

	body := []byte(fmt.Sprintf("%d\n", id))
	sendResponse(conn, 200, "text/plain", body)
}

// Remove the subscriber identified by topic and ID in the URL.
func handleUnsubscribe(conn net.Conn, req *http.Request, broker *Broker) {
	topicName := req.URL.Query().Get("topic")
	idString := req.URL.Query().Get("id")

	if topicName == "" {
		sendError(conn, 400)
		return
	}

	id, err := strconv.ParseUint(idString, 10, 64)

	if err != nil || id == 0 {
		sendError(conn, 400)
		return
	}

	success := broker.Unsubscribe(topicName, id)

	if !success {
		sendError(conn, 400)
		return
	}

	sendResponse(conn, 200, "text/plain", []byte("Unsubscribed\n"))
}

// Read the POST body and publish it to the requested topic.
func handlePublish(conn net.Conn, req *http.Request, broker *Broker) {
	topicName := req.URL.Query().Get("topic")

	if topicName == "" {
		sendError(conn, 400)
		return
	}

	if req.ContentLength > maxUploadBytes {
		sendError(conn, 400)
		return
	}

	// Read into memory because Publish expects []byte; limit the message size.
	data, err := io.ReadAll(io.LimitReader(req.Body, maxUploadBytes+1))

	if err != nil || int64(len(data)) > maxUploadBytes {
		sendError(conn, 400)
		return
	}

	broker.Publish(topicName, data)

	sendResponse(conn, 200, "text/plain", []byte("Published\n"))
}

// Long-poll for the next message and send back the broker result.
func handleEvents(conn net.Conn, req *http.Request, broker *Broker) {
	topicName := req.URL.Query().Get("topic")
	idString := req.URL.Query().Get("id")

	if topicName == "" {
		sendError(conn, 400)
		return
	}

	id, err := strconv.ParseUint(idString, 10, 64)

	if err != nil || id == 0 {
		sendError(conn, 400)
		return
	}

	// Events may wait until a message arrives, the user unsubscribes, or timeout.
	message, status := broker.Events(topicName, id)

	if status == 200 {
		sendResponse(conn, 200, "text/plain", message.Data)
		return
	}

	// 204 means no message arrived before the timeout.
	if status == 204 {
		sendResponse(conn, 204, "text/plain", nil)
		return
	}

	sendError(conn, 400)
}
