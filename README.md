# Concurrency Lab: Thread Pool with Live Terminal Visualizer

Operating Systems course project. Submitted to: **[Professor's name]**

| Member | Registration | Responsibility |
| --- | --- | --- |
| Muhammad Sheheryar | 2024-cs-075 | Pool and synchronization |
| Muhammad Rafay Butt | 2024-cs-007 | Demos and benchmarks |
| Muhammad Abubakkar Sadaqat | 2024-cs-013 | Terminal visualizer and report |

## Small project structure

Four files: `lab.c` contains the core, terminal visualizer and demos; `Makefile` builds it; `verify.py` checks correctness; this document explains it. No browser, web framework or extra terminal library is needed. Generated binaries and logs are additional outputs.

## Run on Ubuntu or WSL Ubuntu

Install GCC, Make and Python 3 if needed (`sudo apt install build-essential python3`). Inside this directory:

```sh
make
./lab pool 4 24 1000
make test
make tsan
./lab race
./lab deadlock
./lab wait
./lab bench > benchmark.csv
```

The pool command opens a live display directly in your Linux terminal, similar to a small `top` screen. It refreshes every 100 ms and leaves the final summary on screen. Use a terminal at least 85 columns wide and 24 rows tall. When redirected to a file or used in a dumb terminal, it prints one final snapshot without screen-control codes.

Example snapshot (illustrative; ordering and timing vary):

```text
CONCURRENCY LAB | PID 1234 | 1.2 seconds
Workers: 4   Queued: 16/16   Completed: 4/24

WORKER   STATE       JOB       FINISHED
0        BUSY        4         1
1        BUSY        5         1
2        BUSY        6         1
3        BUSY        7         1

Waiting jobs: 8 9 10 11 12 13 14 15 16 17 18 19 20 21 22 23
```

PID is the actual program process ID. WORKER is our logical thread number, not a Linux thread ID. STARTING means the worker has not yet reported activity; IDLE means no assigned job; BUSY means processing an assigned job; STOPPED means the worker exited. These are application states, not kernel scheduling states. Jobs currently simulate work by sleeping, so a BUSY worker need not be using CPU. To inspect actual Linux thread states, run `ps -T -p PID` or `top -H -p PID` in another terminal using the displayed PID.

The C program still saves `events.jsonl` for automated correctness checks. You do not need to open it to see the visualizer.

`./lab pool [workers] [jobs] [task milliseconds] [log path]` accepts 1–8 workers, 0–10000 jobs and 0–10000 ms per task. Example: `./lab pool 8 500 0 stress.jsonl`. Queue capacity is 16 waiting tasks, plus up to eight running tasks. Long runs deliberately block the producer when the queue fills.

## How the core works

1. Create a fixed set of workers once.
2. The producer waits on a counting semaphore for a free queue slot, then inserts a job under the mutex and signals the work condition.
3. A worker waits in a condition-variable predicate loop, removes one job under the mutex and returns its queue slot with `sem_post`.
4. It releases the mutex before doing work, then locks it to record completion.
5. After all submissions, the producer sets the closing flag and broadcasts. Workers drain queued jobs before exiting. Join every worker before destroying synchronization objects or closing the log.

The same mutex protects queue indices, queue count, completion count, closing, worker display state and log order. A monitor thread takes a consistent snapshot under that mutex, releases it, then prints the snapshot. It is joined before cleanup. The benchmark disables this monitor to keep display overhead out of measurements. Worker arguments remain alive until all joins finish. The semaphore enforces capacity; the condition variable handles an empty queue; their jobs differ. This intentionally small implementation supports one producer and shutdown only after submission finishes. It is a teaching program, not a general concurrent submit/shutdown library. OS synchronization failures terminate the process with an error.

## Demos and interpretation

- **Race:** separate atomic load/store loses updates because the compound increment is not indivisible. Atomic fetch-add fixes it. Lost updates are scheduling-dependent and might not appear on every run. This is a logical race demonstration using defined C behavior; it deliberately avoids an undefined non-atomic data race. ThreadSanitizer should not flag this example.
- **Deadlock:** barriers make both threads hold opposite first locks. Each tries the second lock and reports failure. A second barrier preserves that state until both attempts finish. `trylock` demonstrates the circular-wait situation without hanging the program. The fix acquires A then B in every worker.
- **Busy wait:** compare spinning on an atomic flag with waiting on a condition variable for a 200 ms signal. Wall time should be similar; CPU time should usually fall with blocking. Actual measurements depend on the machine.
- **Producer-consumer:** the pool itself is the bounded producer-consumer simulation. Workers are consumers; the main thread is the producer.

## Correctness and validation

 The authoring environment was Windows without GCC and with WSL access denied, so Linux compilation, integration tests and ThreadSanitizer were not executed here. Do not describe them as passed until you run them successfully. ThreadSanitizer availability depends on your Linux runtime/toolchain.

## Performance: fair claims

The benchmark prints CSV for 1, 2, 4 and 8 pool workers and thread-per-task, with three trials each. Each run handles the same 128 simulated 5 ms waiting jobs, includes creation/join overhead and disables event logging. The thread-per-task case uses 128 concurrent threads; it therefore has a different concurrency limit and can finish faster for waiting jobs. These numbers are not evidence that a pool always wins or that CPU parallelism scales linearly. No performance numbers are invented here.

For the report, plot workers against mean elapsed time from `benchmark.csv`, with trial range/error bars. Record CPU model, available cores, OS and GCC version. Benchmark CSV is a separate output for plotting in a spreadsheet or report. The live terminal view shows queue contents and counts.

## Useful enhancements, in order

1. Add a CPU-bound workload with a verifiable checksum; compare both designs at equal maximum concurrency. This makes the performance argument stronger.
2. Add repeated randomized producer delays and multiple producers, then define and test submit-vs-shutdown behavior before exposing a reusable API.
3. Add latency metrics (submit-to-start and submit-to-completion), throughput and peak queue usage from the existing log.
4. Add benchmark CSV plotting and exportable figures for the final report.
5. Add keyboard controls (pause display, change refresh speed) only if required. Keep the current ANSI terminal output for the simplest implementation.

## Suggested final report / presentation

Explain the invisible-concurrency problem, show the queue/semaphore/condition design, show a live 4-worker terminal run, demonstrate the three synchronization examples, present actual benchmark results with limitations, then discuss tests and improvements. Include screenshots and your machine details. This README is a report outline; final experimental results and professor information still need to be filled in.
