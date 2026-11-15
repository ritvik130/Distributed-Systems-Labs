# Lab 1 — Concurrent HTTP Server and Pub-Sub Broker

A Go TCP server that supports HTTP file uploads/downloads and a simple publish-subscribe broker. TCP connections, goroutines, and concurrency control are implemented manually using the `net` package; `net/http` is used to parse incoming HTTP requests.

## Requirements

- Docker and Docker Compose (recommended), **or** Go 1.25+
- Port **8080** available

## Run with Docker Compose

From the project directory:

```bash
docker compose up --build
```

The server listens on `http://localhost:8080`. Press **Ctrl+C** to stop it, or run `docker compose down` from another terminal. Uploaded files are stored in the `lab1-data` Docker volume and are retained when the container is recreated. Pub-Sub subscriptions and queues are held in memory and reset when the server restarts.

To run in the background instead:

```bash
docker compose up -d --build
docker compose logs -f
```

## HTTP file server

| Method | Path | Behavior |
| --- | --- | --- |
| `GET` | `/path/file.txt` | Download a file from `public/` |
| `POST` | `/path/file.txt` | Upload or overwrite a file in `public/` |

Supported extensions: `.html`, `.txt`, `.gif`, `.jpeg`, `.jpg`, `.css`. Uploads are limited to **64 MiB**. Files are written to a temporary file and atomically renamed into place after the upload completes.

Try an upload and download from a second terminal:

```bash
printf 'Hello Lab 1\n' > example.txt
curl -i -X POST --data-binary @example.txt http://localhost:8080/example.txt
curl -i http://localhost:8080/example.txt
```

## Pub-Sub broker

| Method | Endpoint | Behavior |
| --- | --- | --- |
| `POST` | `/subscribe?topic=news` | Subscribe; returns `Subscribed ID: <id>` |
| `POST` | `/publish?topic=news` | Publish the request body to the topic |
| `GET` | `/events?topic=news&id=<id>` | Wait for the next message for this subscriber |
| `POST` | `/unsubscribe?topic=news&id=<id>` | Remove a subscriber |

Manual test:

**Terminal A — subscribe:**

```bash
curl -i -X POST 'http://localhost:8080/subscribe?topic=news'
```

Note the returned subscriber ID (for example, `1`). Then, in **Terminal A**, start waiting for an event:

```bash
curl -i 'http://localhost:8080/events?topic=news&id=1'
```

**Terminal B — publish while Terminal A is waiting:**

```bash
curl -i -X POST --data-binary 'Hello subscribers!' \
  'http://localhost:8080/publish?topic=news'
```

Terminal A should receive `Hello subscribers!`. Replace `1` with the actual returned ID. Unsubscribe when finished:

```bash
curl -i -X POST 'http://localhost:8080/unsubscribe?topic=news&id=1'
```

`GET /events` uses long polling: it returns a message when available, `204 No Content` after approximately 20 seconds without one, or an error if the subscription is invalid/cancelled.

## Concurrency

- At most **10 active HTTP handlers** use a buffered channel as a semaphore.
- Each TCP connection is handled by a goroutine.
- Waiting long-poll requests temporarily release their handler slot, allowing `POST /publish` to proceed even when 10 subscribers are waiting.
- Broker state is protected with a mutex. Every topic maintains message sequence numbers.
- Each subscriber has a queue of up to **10 messages**. If a queue is full, publishing skips that subscriber instead of blocking.

### Optional: demonstrate the 10-handler limit

This test needs `nc` (Netcat). It starts 10 unfinished POST uploads, then measures how long an 11th GET must wait:

```bash
for i in $(seq 1 10); do
  (printf "POST /hold$i.txt HTTP/1.1\r\nHost: localhost\r\nContent-Length: 1\r\nConnection: close\r\n\r\n"; sleep 10) \
    | nc -N localhost 8080 >/dev/null 2>&1 &
done
sleep 1
time curl -i http://localhost:8080/missing.txt
```

The GET should wait roughly **9 seconds** for a free handler slot; the resulting `404 Not Found` is expected because `missing.txt` does not exist.

## Bash test scripts

Two small Bash scripts are included for repeatable concurrency checks. Keep the server running with `docker compose up --build`, then run the tests from another terminal in the project directory.

```bash
bash test_concurrency_limit.sh
bash test_pubsub_events.sh
```

- **`test_concurrency_limit.sh` — HTTP handler limit:** Opens 10 incomplete POST uploads using Netcat (`nc`), keeps them open for 10 seconds, and times an 11th GET. The GET should wait approximately 9 seconds before receiving a response. A `404 Not Found` for `/demo.txt` is expected if that file does not exist. This script shows the timing for manual inspection; it does not print an automatic PASS/FAIL result.
- **`test_pubsub_events.sh` — Pub-Sub long polling:** Creates 10 subscribers on one topic, starts 10 simultaneous `/events` requests, then publishes a message while they are waiting. It checks that all 10 subscribers receive `Hello everyone!`, prints PASS/FAIL, and unsubscribes the test clients.

The scripts use **Bash** and **curl**; the HTTP concurrency test also requires **Netcat (`nc`)**. For testing a remote EC2 instance, edit the `SERVER="localhost"` variable at the top of each script to the instance's current public IP. The Pub-Sub script also has a `PORT="8080"` setting; the HTTP concurrency script uses port `8080` directly. Ensure the server is reachable on that port.

The manual `curl` and `nc` examples above can also be run individually during a demonstration.

## Running without Docker (optional)

```bash
mkdir -p public
go build -o http_server main.go broker.go broker_handlers.go
./http_server 8080
```
