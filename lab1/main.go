// HTTP server for file uploads/downloads and the Pub-Sub broker.
package main

import (
	"bufio"
	"bytes"
	"crypto/rand"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"path"
	"strings"
	"time"
)

// Limits shared by the file server and Pub-Sub handlers.
const (
	maxHandlers    = 10
	maxUploadBytes = 64 << 20         // 64 MiB
	requestTimeout = 30 * time.Second // Total connection deadline
)

// File extensions that our server accepts.
var contentTypes = map[string]string{
	".html": "text/html",
	".txt":  "text/plain",
	".gif":  "image/gif",
	".jpeg": "image/jpeg",
	".jpg":  "image/jpeg",
	".css":  "text/css",
}

// Convert an HTTP status code into the text used in the response line.
func statusText(status int) string {
	switch status {
	case 200:
		return "OK"
	case 204:
		return "No Content"
	case 400:
		return "Bad Request"
	case 404:
		return "Not Found"
	case 501:
		return "Not Implemented"
	default:
		return "Error"
	}
}

// Write the HTTP status and headers. The body is sent separately.
func writeHeaders(conn net.Conn, status int, contentType string, length int64) error {

	_, err := fmt.Fprintf(conn,
		"HTTP/1.1 %d %s\r\n"+
			"Content-Type: %s\r\n"+
			"Content-Length: %d\r\n"+
			"Connection: close\r\n"+
			"\r\n",
		status, statusText(status), contentType, length,
	)
	return err
}

// Send a short response that is already available in memory.
func sendResponse(conn net.Conn, status int, contentType string, body []byte) error {
	if err := writeHeaders(conn, status, contentType, int64(len(body))); err != nil {
		return err
	}
	_, err := io.Copy(conn, bytes.NewReader(body))
	return err
}

// Return an HTTP error with a short text message.
func sendError(conn net.Conn, status int) {
	if err := sendResponse(conn, status, "text/plain", []byte(statusText(status)+"\n")); err != nil {
		log.Printf("cannot send HTTP %d: %v", status, err)
	}
}

// Turn the URL path into a safe relative filename and check its extension.
func requestedFile(urlPath string) (filename, contentType string, ok bool) {
	if !strings.HasPrefix(urlPath, "/") {
		return "", "", false
	}
	filename = strings.TrimPrefix(urlPath, "/")
	if filename == "" {
		return "", "", false
	}

	// Reject paths that try to go up a directory.
	for _, part := range strings.Split(filename, "/") {
		if part == "." || part == ".." {
			return "", "", false
		}
	}

	extension := path.Ext(filename)
	contentType, ok = contentTypes[extension]
	return filename, contentType, ok
}

// Serve a file from public/. An open file stays readable during a concurrent upload.
func handleGet(conn net.Conn, root *os.Root, filename, contentType string) {
	file, err := root.Open(filename)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			sendError(conn, 404)
		} else {
			sendError(conn, 400)
		}
		return
	}
	defer file.Close()

	// We only serve regular files, not directories.
	info, err := file.Stat()
	if err != nil || !info.Mode().IsRegular() {
		sendError(conn, 400)
		return
	}

	if err := writeHeaders(conn, 200, contentType, info.Size()); err != nil {
		log.Printf("GET headers: %v", err)
		return
	}

	// Stream the file to the client instead of loading it all into memory.
	n, err := io.Copy(conn, file)
	if err != nil || n != info.Size() {
		log.Printf("GET %q: transferred %d/%d bytes; error: %v", filename, n, info.Size(), err)
	}
}

// Store an uploaded file. Write to a temporary file before replacing the old one.
func handlePost(conn net.Conn, root *os.Root, req *http.Request, filename string) {
	// Reject uploads that are already known to be too large.
	if req.ContentLength > maxUploadBytes {
		sendError(conn, 400)
		return
	}

	if err := root.MkdirAll(path.Dir(filename), 0o755); err != nil {
		log.Printf("POST mkdir %q: %v", filename, err)
		sendError(conn, 400)
		return
	}

	// Use a unique name so concurrent uploads do not overwrite each other.
	tempName := path.Join(path.Dir(filename), ".upload-"+rand.Text()+".tmp")
	temp, err := root.OpenFile(tempName, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		log.Printf("POST temporary file %q: %v", tempName, err)
		sendError(conn, 400)
		return
	}

	// Remove the temporary file if anything fails before the rename.
	committed := false
	defer func() {
		if !committed {
			root.Remove(tempName)
		}
	}()

	// Read one extra byte to detect oversized uploads, including chunked requests.
	n, copyErr := io.Copy(temp, io.LimitReader(req.Body, maxUploadBytes+1))
	closeErr := temp.Close()
	if copyErr != nil || closeErr != nil || n > maxUploadBytes {
		log.Printf("POST %q: copied %d bytes; copy=%v close=%v", filename, n, copyErr, closeErr)
		sendError(conn, 400)
		return
	}

	// Replace the destination only after the whole upload is stored.
	if err := root.Rename(tempName, filename); err != nil {
		log.Printf("POST rename %q: %v", filename, err)
		sendError(conn, 400)
		return
	}
	committed = true

	if err := sendResponse(conn, 200, "text/plain", []byte("Stored\n")); err != nil {
		log.Printf("POST response: %v", err)
	}
}

// Handle one TCP connection and route its HTTP request.
func handleClient(conn net.Conn, slots chan struct{}, root *os.Root, broker *Broker) {
	// Always close the connection and free its handler slot.
	defer func() {
		conn.Close()
		<-slots
	}()

	// Avoid waiting forever for a client to finish its request.
	_ = conn.SetDeadline(time.Now().Add(requestTimeout))

	req, err := http.ReadRequest(bufio.NewReader(conn))
	if err != nil {
		sendError(conn, 400)
		return
	}

	if req.Method != "GET" && req.Method != "POST" {
		sendError(conn, 501)
		return
	}

	// Log the request before routing it to the correct handler.
	log.Printf("%s %s from %s", req.Method, req.URL.RequestURI(), conn.RemoteAddr())

	// Pub-Sub paths have no file extension, so handle them first.
	switch req.URL.Path {
	case "/subscribe":
		if req.Method != "POST" {
			sendError(conn, 501)
			return
		}
		handleSubscribe(conn, req, broker)
		return

	case "/unsubscribe":
		if req.Method != "POST" {
			sendError(conn, 501)
			return
		}
		handleUnsubscribe(conn, req, broker)
		return

	case "/publish":
		if req.Method != "POST" {
			sendError(conn, 501)
			return
		}
		handlePublish(conn, req, broker)
		return

	case "/events":
		if req.Method != "GET" {
			sendError(conn, 501)
			return
		}
		handleEvents(conn, req, broker)
		return
	}

	filename, contentType, ok := requestedFile(req.URL.Path)
	if !ok {
		sendError(conn, 400)
		return
	}

	switch req.Method {
	case "GET":
		handleGet(conn, root, filename, contentType)
	case "POST":
		handlePost(conn, root, req, filename)
	}
}

// Set up the shared resources and accept incoming connections.
func main() {
	if len(os.Args) != 2 {
		fmt.Fprintln(os.Stderr, "Usage: ./http_server <port>")
		os.Exit(1)
	}
	port := os.Args[1]

	listener, err := net.Listen("tcp", ":"+port)
	if err != nil {
		log.Fatalf("Listen error: %v", err)
	}
	defer listener.Close()

	// All file requests stay inside the public directory.
	root, err := os.OpenRoot("public")
	if err != nil {
		log.Fatalf("Open public directory: %v (create ./public first)", err)
	}
	defer root.Close()

	log.Printf("Server listening on port %s", port)

	// A buffered channel is used as a semaphore for the 10-handler limit.
	slots := make(chan struct{}, maxHandlers)

	// All connections use the same broker, so subscriptions are kept between requests.
	broker := NewBroker()

	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Printf("Accept error: %v", err)
			continue
		}

		// Put a token in the channel. If all 10 slots are busy, wait here.
		slots <- struct{}{}
		go handleClient(conn, slots, root, broker)
	}
}
