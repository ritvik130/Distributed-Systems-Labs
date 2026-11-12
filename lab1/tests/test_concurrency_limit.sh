#!/usr/bin/env bash

# localhost or IP for EC2
SERVER="localhost"

for i in $(seq 1 10); do
    (printf "POST /hold$i.txt HTTP/1.1\r\nHost: $SERVER\r\nContent-Length: 1\r\nConnection: close\r\n\r\n"; sleep 10) | nc -N "$SERVER" 8080 >/dev/null 2>&1 &
done

sleep 1

time curl -i "http://$SERVER:8080/demo.txt"