// Lab 1: concurrent HTTP file server (Basic Part).
//
// Architecture:
//  1. main() accepts TCP connections and limits active handlers to 10
//  2. handleClient() parses one HTTP request and dispatches by method
//  3. handleGet() reads an existing file; handlePost() publishes a new version
//  4. The server closes each connection after sending one response
//
// File operations are restricted to ./public using os.Root (Go 1.25+)
// We use net for sockets; net/http is used only to parse request objects
// The 30-second deadline and 64-MiB upload limit are local safety choices
// NOT explicit requirements in the assignment.
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

// Server limits. Only maxHandlers=10 is explicitly required by the lab.
const (
	maxHandlers    = 10
	maxUploadBytes = 64 << 20         // 64 MiB; custom limit, may reject larger valid files
	requestTimeout = 30 * time.Second // Total handler deadline, not an idle timeout
)

// Map each allowed file extension to the Content-Type returned by GET.
// Unsupported extensions cause HTTP 400 (as specified by the assignment).
var contentTypes = map[string]string{
	".html": "text/html",
	".txt":  "text/plain",
	".gif":  "image/gif",
	".jpeg": "image/jpeg",
	".jpg":  "image/jpeg",
	".css":  "text/css",
}

// statusText returns the reason phrase used in the HTTP status line.
// Keep this in sync if Pub-Sub adds new response codes (e.g., 204).
func statusText(status int) string {
	switch status {
	case 200:
		return "OK"
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

// writeHeaders sends the status line and HTTP response headers
// length counts BODY bytes only, not headers. The blank CRLF line ends headers
// It does not send any body bytes
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

// sendResponse is for short, in-memory response bodies (error messages,
// successful POST acknowledgement). GET uses writeHeaders + io.Copy instead
// so that large files are streamed without loading them fully into RAM.
func sendResponse(conn net.Conn, status int, contentType string, body []byte) error {
	if err := writeHeaders(conn, status, contentType, int64(len(body))); err != nil {
		return err
	}
	_, err := io.Copy(conn, bytes.NewReader(body))
	return err
}

// sendError replies with a plain-text error; log network write failures.
func sendError(conn net.Conn, status int) {
	if err := sendResponse(conn, status, "text/plain", []byte(statusText(status)+"\n")); err != nil {
		log.Printf("cannot send HTTP %d: %v", status, err)
	}
}

// requestedFile turns a URL path such as "/images/cat.jpg" into the relative
// filename "images/cat.jpg" and its MIME type "image/jpeg"
// Reject paths that are empty, contain traversal/dot segments, or use unsupported
// extensions. os.Root provides a additional security
func requestedFile(urlPath string) (filename, contentType string, ok bool) {
	if !strings.HasPrefix(urlPath, "/") {
		return "", "", false
	}
	// HTTP paths start with '/'; os.Root expects paths relative to public/.
	filename = strings.TrimPrefix(urlPath, "/")
	if filename == "" {
		return "", "", false
	}
	// Reject '..' traversal, '.' segments
	for _, part := range strings.Split(filename, "/") {
		if part == "." || part == ".." {
			return "", "", false
		}
	}
	// Extract .txt/.html/.jpg/... and look it up in the allowed-type map
	extension := path.Ext(filename)
	contentType, ok = contentTypes[extension]
	return filename, contentType, ok
}

// handleGet opens one version of a file and streams it to the client
// If a concurrent POST renames a new version, an already-open GET still
// reads its original file descriptor (on Linux), never half of each version
func handleGet(conn net.Conn, root *os.Root, filename, contentType string) {
	// root is already attached to public/; filename is a RELATIVE path
	file, err := root.Open(filename)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			sendError(conn, 404)
		} else {
			sendError(conn, 400)
		}
		return
	}
	// Close the descriptor regardless of which return path is taken
	defer file.Close()

	// GET serves only regular files, not directories or special files
	info, err := file.Stat()
	if err != nil || !info.Mode().IsRegular() {
		sendError(conn, 400)
		return
	}

	// Send Content-Length before the body (known from the open file's size).
	if err := writeHeaders(conn, 200, contentType, info.Size()); err != nil {
		log.Printf("GET headers: %v", err)
		return
	}
	// Stream bytes directly from disk to the TCP connection
	// io.Copy returns the count and any transfer error
	n, err := io.Copy(conn, file)
	if err != nil || n != info.Size() {
		log.Printf("GET %q: transferred %d/%d bytes; error: %v", filename, n, info.Size(), err)
	}
}

// handlePost stores the request body as a file in public/
// It uploads into a unique temporary file FIRST and renames only after
// writing and closing succeed. On Linux and the same filesystem, rename
// atomically replaces the destination name: GET sees an old or new version
// Concurrent POSTs have separate temp files and the last rename wins
func handlePost(conn net.Conn, root *os.Root, req *http.Request, filename string) {
	// Known-length uploads can be rejected before creating any files
	// ContentLength == -1 means unknown/chunked; the streaming limit below
	// still applies. The 64-MiB limit is an implementation decision.
	if req.ContentLength > maxUploadBytes {
		sendError(conn, 400)
		return
	}
	// For POST /images/cat.jpg, create public/images/ if needed
	// For POST /hello.txt, path.Dir(filename) is "." (the public root)
	if err := root.MkdirAll(path.Dir(filename), 0o755); err != nil {
		log.Printf("POST mkdir %q: %v", filename, err)
		sendError(conn, 400)
		return
	}

	// The temporary file is in the same destination directory/filesystem
	// A random name lets concurrent POSTs write without overwriting each other
	tempName := path.Join(path.Dir(filename), ".upload-"+rand.Text()+".tmp")
	// O_CREATE: create; O_EXCL: fail on collision; O_WRONLY: writing only
	// 0600 makes the unpublished temp file accessible only to its owner
	temp, err := root.OpenFile(tempName, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		log.Printf("POST temporary file %q: %v", tempName, err)
		sendError(conn, 400)
		return
	}

	// A failed/interrupted upload must not replace the existing destination
	// Defer cleanup unless rename succeeds (committed=true)
	committed := false
	defer func() {
		if !committed {
			_ = root.Remove(tempName)
		}
	}()

	// req.Body contains the incoming POST payload
	// Copy at most limit+1 bytes: the extra byte lets us detect oversize
	// even when Content-Length was not supplied (e.g., chunked encoding)
	n, copyErr := io.Copy(temp, io.LimitReader(req.Body, maxUploadBytes+1))
	// Always close before rename, and check both write and close errors
	closeErr := temp.Close()
	if copyErr != nil || closeErr != nil || n > maxUploadBytes {
		log.Printf("POST %q: copied %d bytes; copy=%v close=%v", filename, n, copyErr, closeErr)
		sendError(conn, 400)
		return
	}

	// Publish only a COMPLETELY written temporary file
	// Rename replaces the filename mapping, not bytes in an open fil
	// A pre-existing GET file descriptor stays on the old complete version
	if err := root.Rename(tempName, filename); err != nil {
		log.Printf("POST rename %q: %v", filename, err)
		sendError(conn, 400)
		return
	}
	// Rename succeeded; deferred cleanup must not remove the published file.
	committed = true

	// Acknowledge POST. "Stored\n" is the RESPONSE body; req.Body was
	// the REQUEST body stored in the file.
	if err := sendResponse(conn, 200, "text/plain", []byte("Stored\n")); err != nil {
		log.Printf("POST response: %v", err)
	}
}

// handleClient processes one HTTP request on one accepted TCP connection
// conn belongs to this client; slots and root are shared by all handlers
// The deferred closure releases the semaphore slot on EVERY exit path
func handleClient(conn net.Conn, slots chan struct{}, root *os.Root) {
	defer func() {
		_ = conn.Close()
		<-slots
	}()

	// Protect the finite handler pool against clients that never finish a request
	//  This is a 30-seconds deadline for reads and writes
	_ = conn.SetDeadline(time.Now().Add(requestTimeout))

	// Read the HTTP request from the client.
	// If the request is invalid, return 400 Bad Request
	req, err := http.ReadRequest(bufio.NewReader(conn))
	if err != nil {
		sendError(conn, 400)
		return
	}

	//  HTTP Response 501 for methods other than GET/POST
	if req.Method != "GET" && req.Method != "POST" {
		sendError(conn, 501)
		return
	}

	// Both GET and POST share the same filename + extension validation
	filename, contentType, ok := requestedFile(req.URL.Path)
	if !ok {
		sendError(conn, 400)
		return
	}

	// Log successfully validated file requests. Pub-Sub endpoint routing
	// should be added earlier, BEFORE requestedFile() checks extensions
	log.Printf("%s %s from %s", req.Method, req.URL.Path, conn.RemoteAddr())
	switch req.Method {
	case "GET":
		handleGet(conn, root, filename, contentType)
	case "POST":
		handlePost(conn, root, req, filename)
	}
}

// main sets up shared resources, then continually accepts TCP connections
// The listener and root remain open for the server's lifetime
func main() {
	if len(os.Args) != 2 {
		fmt.Fprintln(os.Stderr, "Usage: ./http_server <port>")
		os.Exit(1)
	}
	// The port is supplied via CLI
	port := os.Args[1]

	// Bind all local interfaces on the requested TCP port
	listener, err := net.Listen("tcp", ":"+port)
	if err != nil {
		log.Fatalf("Listen error: %v", err)
	}
	defer listener.Close()

	// All GET/POST file operations are rooted in ./public
	// OpenRoot prevents escaping via ".." and symlinks outside the root
	root, err := os.OpenRoot("public")
	if err != nil {
		log.Fatalf("Open public directory: %v (create ./public first)", err)
	}
	defer root.Close()

	log.Printf("Server listening on port %s", port)
	// Buffered channel used as a counting semaphore (10 occupied tokens)
	// The empty struct has zero data payload; only capacity matters
	slots := make(chan struct{}, maxHandlers)

	for {
		// Accept blocks until a new client establishes a TCP connection
		conn, err := listener.Accept()
		if err != nil {
			log.Printf("Accept error: %v", err)
			continue
		}
		// Acquire a slot BEFORE spawning a goroutine. If 10 are active,
		// this send blocks until some handler receives from slots on exit
		slots <- struct{}{}
		// Each request is handled concurrently, with shared root and semaphore
		go handleClient(conn, slots, root)
	}
}
