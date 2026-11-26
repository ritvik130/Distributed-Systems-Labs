# Lab 2 — MapReduce (Basic)

The Basic implementation runs one Coordinator and multiple Workers on a single Linux machine with a shared local filesystem. The Advanced implementation is maintained separately.

## Implementation

- **Coordinator (`mr/coordinator.go`):** assigns Map and Reduce tasks using Go RPC over a Unix-domain socket. Reduce tasks become available only after all Map tasks complete. Tasks that have been in progress for at least 10 seconds are eligible for reassignment. A mutex protects shared task state, and attempt IDs allow the Coordinator to accept the first valid successful result without overwriting it with a losing attempt.
- **Worker (`mr/worker.go`):** requests tasks, executes the application plugin, and reports results. Map output is partitioned by key hash and written to JSON files named `mr-mapID-attemptID-reduceID`. Reduce tasks read the accepted Map partitions, group values by key, and write attempt-specific output files. The Coordinator renames the accepted output to `mr-out-X`. Workers wait when no tasks are available and exit upon receiving `ExitTask`.
- **RPC (`mr/rpc.go`):** defines task requests, result reports, and the Map, Reduce, Wait and Exit task types.

## Testing

Requirements: Linux, Go (tested with Go 1.27.3), GCC, and Bash. No AWS, Docker or additional Python scripts are required.

From `lab2/src/main`:

```bash
bash test-mr.sh
bash test-mr-many.sh 3
```

The scripts build the Coordinator, Worker and required plugins automatically. Run them sequentially, not in parallel.

The following tests passed locally: WordCount, Indexer, Map parallelism, Reduce parallelism, job count, early exit and crash recovery. All three repeated testing trials also passed.

Expected final messages:

```text
*** PASSED ALL TESTS
*** PASSED ALL 3 TESTING TRIALS
```

## Manual execution (optional)

From `lab2/src/main`, with no previous MapReduce processes running:

```bash
go build -buildmode=plugin -o wc.so ../mrapps/wc.go
rm -f mr-out-*
go run mrcoordinator.go pg-*.txt
```

In two other terminals, from the same directory, run:

```bash
go run mrworker.go wc.so
```

After the job finishes, inspect the combined results:

```bash
cat mr-out-* | sort | head -20
```
