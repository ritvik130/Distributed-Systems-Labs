#!/usr/bin/env bash
set -euo pipefail

# localhost or IP for EC2
SERVER="localhost"
PORT="8080"

TOPIC="demo-$$"
IDS=()
PIDS=()

# Create 10 subscribers on the same topic.
echo "Creating 10 subscribers (topic: $TOPIC)..."
for i in $(seq 1 10); do
    response=$(curl -fsS --max-time 5 -X POST "http://$SERVER:$PORT/subscribe?topic=$TOPIC")
    id=$(echo "$response" | awk '{print $3}')
    IDS+=("$id")
    echo "$response"
done

# Start 10 long-poll requests at the same time.
echo
echo "Starting 10 waiting /events requests..."
for id in "${IDS[@]}"; do
    (
        response=$(curl -fsS --max-time 25 "http://$SERVER:$PORT/events?topic=$TOPIC&id=$id")
        if [[ "$response" == "Hello everyone!" ]]; then
            echo "[PASS] Subscriber $id received: $response"
        else
            echo "[FAIL] Subscriber $id received: $response"
            exit 1
        fi
    ) &
    PIDS+=("$!")
done

sleep 5

echo
echo "Publishing while all 10 subscribers are waiting..."
start=$SECONDS
curl -fsS --max-time 5 -i -X POST \
    "http://$SERVER:$PORT/publish?topic=$TOPIC" \
    --data-binary "Hello everyone!"
echo "Publish finished in $((SECONDS - start)) second(s)."
echo

failed=0
for pid in "${PIDS[@]}"; do
    wait "$pid" || failed=1
done

# Clean up the test subscriptions.
for id in "${IDS[@]}"; do
    curl -fsS --max-time 5 -X POST \
        "http://$SERVER:$PORT/unsubscribe?topic=$TOPIC&id=$id" >/dev/null || true
done

if (( failed )); then
    echo "RESULT: FAIL — some subscribers did not receive the message."
    exit 1
fi

echo "RESULT: PASS — all 10 subscribers received the message."