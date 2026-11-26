# Lab 2 — MapReduce (Advanced)



This implementation runs a Coordinator and multiple Workers on separate machines without a shared filesystem. It was demonstrated locally and on AWS EC2. The Basic implementation is maintained separately.

## Implementation

- **2.1 — Distribution and worker failures:** Workers request tasks and report results using Go RPC over TCP. Input files are downloaded from the Coordinator over HTTP. Intermediate files (`mr-mapID-attemptID-reduceID`) are stored on the Workers that produce them and fetched by other Workers when needed. Final output files (`mr-out-X`) are uploaded to the Coordinator. Heartbeats are sent approximately every second; Workers are considered unreachable after five seconds without a heartbeat. Interrupted tasks are reassigned, and missing intermediate results trigger recomputation of the affected Map tasks only.
- **2.2 — Coordinator failure:** The Coordinator saves tasks, attempts and Worker registrations to a local JSON file using atomic file replacement and fsync. Workers retry failed requests and uploads. After a Coordinator restart, the job resumes from the saved state, including results from previously assigned attempts.
- **2.3 — Straggler mitigation:** Workers report their current execution stage in heartbeats. The Coordinator prioritizes recovery, then unassigned tasks, then speculative execution. The adaptive threshold is `max(4s, 2 × median completed task duration)` for the same task type. When an idle Worker is available, a task with one live attempt that has spent enough time in an eligible stage can receive a speculative copy. The first valid successful attempt wins. This applies to both Map and Reduce tasks.
- **Worker shutdown:** After all final Reduce results have been committed, heartbeat replies carry `JobDone`. Workers receiving this signal exit, including those still executing losing speculative attempts. `Done()` keeps the Coordinator available for a four-second grace period, followed by the starter program's original one-second wait.

The `demo/wc_demo.go` plugin uses the MIT WordCount application with controlled delays to simulate slow tasks. `demo/status.py` displays task information from the Coordinator's persistent state.

## 1. Setup

Use three Linux machines (one Coordinator and two Workers). Our AWS setup uses Amazon Linux 2023, x86-64, Go 1.27.3 and `t3.small` instances. Use the same repository revision and Go version on all machines.

For AWS, allow TCP ports 8081 (Coordinator RPC/HTTP) and 5001 (Worker HTTP) between participating instances. Allow TCP 22 from the regional EC2 Instance Connect source for browser-based SSH. No Docker or shared storage is required.

On each EC2 instance, install the required tools and Go if not already installed:

```bash
sudo dnf install -y git gcc python3 tar gzip
cd /tmp
curl -fLO https://go.dev/dl/go1.27.3.linux-amd64.tar.gz
sudo rm -rf /usr/local/go
sudo tar -C /usr/local -xzf go1.27.3.linux-amd64.tar.gz
echo 'export PATH=/usr/local/go/bin:$PATH' >> ~/.bashrc
source ~/.bashrc
go version
```

Clone the repository (skip this if using the submitted source archive):

```bash
cd ~
git clone -b main https://github.com/ritvik130/Distributed-Systems-Labs.git
cd Distributed-Systems-Labs/lab2/src
```

For an existing checkout, use `git pull --ff-only origin main` and check `git log -1 --oneline`.

## 2. Build

From `lab2/src`, on all three machines:

```bash
mkdir -p demo/coordinator demo/worker1 demo/worker2
go build -o demo/mrcoordinator main/mrcoordinator.go
go build -o demo/mrworker main/mrworker.go
go build -buildmode=plugin -o demo/wc.so demo/wc_demo.go
```

## 3. Configuration

### Terminal 1 — Coordinator

From `lab2/src`, prepare the input files once:

```bash
cp main/pg-being_ernest.txt demo/coordinator/a.txt
cp main/pg-dorian_gray.txt demo/coordinator/b.txt
cp main/pg-frankenstein.txt demo/coordinator/c.txt
cd demo/coordinator
```

For a larger workload, optionally repeat the original text to reproduce the input sizes used in our AWS demo. Run once from `demo/coordinator`:

```bash
python3 - <<'PY'
from itertools import cycle, islice
for name, count in [('a.txt', 552211), ('b.txt', 676705), ('c.txt', 681118)]:
    with open(name) as f:
        lines = f.read().splitlines()
    with open(name, 'w') as f:
        f.writelines(line + '\n' for line in islice(cycle(lines), count))
PY
```

Keep the same inputs throughout all tests. Replace `COORDINATOR_IP` with the Coordinator's actual public IPv4 address:

```bash
export MR_COORDINATOR_HTTP_ADDR=:8081
export MR_COORDINATOR_PUBLIC_URL=http://COORDINATOR_IP:8081
export MR_COORDINATOR_STATE_FILE=coordinator-state.json
```

### Terminals 2 and 3 — Workers

From `lab2/src`, use `cd demo/worker1` on Worker 1 and `cd demo/worker2` on Worker 2.

On both Workers, set the same Coordinator IP:

```bash
export MR_COORDINATOR_RPC_ADDR=COORDINATOR_IP:8081
export MR_COORDINATOR_PUBLIC_URL=http://COORDINATOR_IP:8081
export MR_WORKER_PORT=5001
export MR_WORKER_PUBLIC_ADDR="$(curl -4 -fsS https://checkip.amazonaws.com):5001"
```

Both Workers can use port 5001 because they run on different machines. Reapply the environment variables for new terminal sessions or changed IP addresses.

For local testing, use the same three working directories on one machine, with `127.0.0.1` in place of public IPs. Set Worker 1's port/address to `5001` / `127.0.0.1:5001` and Worker 2's to `5002` / `127.0.0.1:5002`.

## 4. Manual testing

### Cleanup before each new test

Stop all previous demo processes before removing generated files.

On the Coordinator, from `demo/coordinator`:

```bash
rm -f coordinator-state.json mr-out-*
```

On both Workers, from their respective working directories:

```bash
rm -f mr-* reduce-*
```

Do not clean files when restarting the Coordinator during Test 3.

### Test 1 — Normal distributed execution

**Goal:** verify distributed execution, file placement and no unnecessary attempts.

| Order | Terminal | Command |
|---|---|---|
| 1 | Worker 1 | `../mrworker ../wc.so` |
| 2 | Worker 2 | `../mrworker ../wc.so` |
| 3 | Coordinator | `../mrcoordinator a.txt b.txt c.txt` |

Expected: all three Map and ten Reduce tasks finish with one attempt each. Both Workers can participate in the computation. Intermediate files remain on the Workers and final output files are on the Coordinator.

On the Coordinator:

```bash
python3 ../status.py
cat mr-out-* | sort | head -20
cat mr-out-* | sort > ../normal-output.txt
```

Use `ls` in each terminal to inspect file placement.

### Test 2 — Worker crash recovery

**Goal:** recover an interrupted Map and previously completed Map output that becomes unavailable.

1. Clean all three working directories as described above.
2. Coordinator: `../mrcoordinator a.txt b.txt c.txt`
3. Worker 1: `SLOW_MAP=1 ../mrworker ../wc.so`
4. Wait for `Simulating a slow Map: b.txt`, then press Ctrl+C on Worker 1.
5. Immediately start Worker 2: `../mrworker ../wc.so`

Expected: the Coordinator detects Worker 1 as unreachable. Worker 2 reports the missing intermediate file and recomputes the affected Map. Map attempt counts are `[2, 2, 1]`; unrelated Map work is preserved. The final output matches Test 1.

### Test 3 — Coordinator crash and restart

**Goal:** resume the job from persistent state and accept a result from an original attempt.

1. Clean all three working directories.
2. Coordinator: `../mrcoordinator a.txt b.txt c.txt`
3. Worker 1: `SLOW_MAP=1 ../mrworker ../wc.so`
4. Wait for `Simulating a slow Map: b.txt`, then press Ctrl+C on the Coordinator only.
5. On the Coordinator, inspect the saved state with `python3 ../status.py`.
6. Keep Worker 1 running until `Error reporting Map task. Retrying...` appears.
7. Restart the Coordinator with `../mrcoordinator a.txt b.txt c.txt` in the same directory. Do not remove the state file or intermediate files.

Expected: the Worker continues after reconnecting, the original Map attempt is accepted and all 13 tasks finish without extra attempts. Worker 2 is not needed for this test.

### Test 4 — Slow Map / speculative execution

**Goal:** use an idle Worker to execute a speculative copy of a slow Map.

1. Clean all three working directories.
2. Coordinator: `../mrcoordinator a.txt b.txt c.txt`
3. Worker 1: `SLOW_MAP=1 ../mrworker ../wc.so`
4. Wait for `Simulating a slow Map: b.txt`.
5. Worker 2: `../mrworker ../wc.so`

Keep both Workers running. Worker 2 finishes the remaining unassigned Map before receiving a speculative copy of Map 1 (`b.txt`). Expected: Map 1 has two attempts, attempt 2 wins, and other tasks execute once. A late result is rejected; a still-running losing Worker can receive the heartbeat shutdown signal when the job finishes.

### Test 5 — Slow Reduce / speculative execution

**Goal:** demonstrate Reduce speculation and automatic shutdown.

1. Clean all three working directories.
2. Coordinator: `../mrcoordinator a.txt b.txt c.txt`
3. Worker 1: `SLOW_REDUCE=1 ../mrworker ../wc.so`
4. Wait for `Simulating a slow Reduce: it`.
5. Worker 2: `../mrworker ../wc.so`

The key `it` belongs to Reduce partition 0 with ten reducers. Worker 2 completes the remaining Reduce tasks before receiving the speculative copy. Expected: Reduce 0 has two attempts, attempt 2 wins, and other tasks execute once. A still-running losing Worker exits on receipt of `JobDone`, printing `Job completed. Shutting down Worker.`

### Verify results after each test

On the Coordinator:

```bash
python3 ../status.py
cat mr-out-* | sort | head -20
cat mr-out-* | sort > ../current-output.txt
diff ../normal-output.txt ../current-output.txt
```

An empty `diff` confirms that the complete output matches Test 1. In `status.py`, `Issued` is the number of attempts assigned and `Winner` is the accepted AttemptID (not the WorkerID).
