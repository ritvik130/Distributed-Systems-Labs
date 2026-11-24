package mr

import (
    "fmt"
    "io"
    "net"
    "net/http"
    "os"
    "path/filepath"
    "strings"
)

const port = "5001"

func intermediateHandler(w http.ResponseWriter, r *http.Request){
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return 
	}
	filename := strings.TrimPrefix(r.URL.Path, "/intermediate/")
	if filename == "" || filepath.Base(filename) != filename {
    	http.Error(w, "Invalid filename", http.StatusBadRequest)
    	return
	}

	file, err := os.Open(filename)
	if err != nil {
    	http.Error(w, "File not found", http.StatusNotFound)
    	return
	}
	defer file.Close()
	_, err = io.Copy(w, file)
	if err != nil {
    	fmt.Println("Error sending file:", err)
    	return
	}
} 

func getIP(port string) string {

    addrs, err := net.InterfaceAddrs()
    if err != nil {
        return ""
    }

    for _, address := range addrs {
        ipnet, ok := address.(*net.IPNet); 
		if ok && !ipnet.IP.IsLoopback() && ipnet.IP.To4() != nil {
            return net.JoinHostPort(ipnet.IP.String(), port)
        }
    }
    return ""
}

func startWorkerServer() (string, error) {
    mux := http.NewServeMux()
	mux.HandleFunc("/intermediate/", intermediateHandler)

    listener, err := net.Listen("tcp", ":"+port)
    if err != nil {
        return "", fmt.Errorf("Worker HTTP listen error: %w", err)
    }

    go func() {
        err := http.Serve(listener, mux)
		if err != nil {
            fmt.Println("Worker HTTP server error:", err)
        }
    }()

    address := getIP(port)
	if address == "" {
    	listener.Close()
    	return "", fmt.Errorf("could not determine Worker IP address")
	}

	return address, nil
}