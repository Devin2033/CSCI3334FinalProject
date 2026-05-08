Concurrent Task Dispatcher Final Project

Steps to build and run from a new codespace:
(cd finalProject)
(cargo build)
(cargo run)

Commands to run a single experiment:
(cargo run -- fifo)
(cargo run -- optimized)

The program uses a fixed random seed (42), so results are reproducible across runs.

---
Design Report:  [Design Report.pdf](https://github.com/user-attachments/files/27505106/Design.Report.pdf)

Design Summary
The system is organized into three concurrent blocks plus shared state:

Block 1 — Dispatcher (main thread). Generates 1000 tasks (70% IO, 30% CPU) 
and pushes them into a shared Mutex<VecDeque> queue at 20ms intervals.

Block 2 — Worker pool (8 threads). Each worker locks the queue, picks a task, 
releases the lock, then simulates execution with thread::sleep(200ms). 
Workers update a shared CPU usage counter when they start and finish each task.

Block 3 — Monitor thread. Snapshots CPU usage, active workers, and queue length every 10ms. 
These snapshots produce the final averaged metrics.

Shared state is bundled in a SharedState struct wrapped in Arc, containing the task 
queue (Mutex<VecDeque>), CPU usage counter (Mutex<f64>), 
active worker count (AtomicU32), and a shutdown flag (AtomicBool).

Shutdown: The dispatcher sets shutdown = true after all tasks are sent. 
Workers exit when the flag is set and the queue is empty. 
Workers are joined first, then the monitor.

---

Tool Use Disclosure
Tools used: Claude - was used as an assistant throughout the project.
Used for syntax, function/tool explination and best coding practice advice. 

Advice Accepted:
-Used a shutdown flag (AtomicBool) instead of channels for clean termination.
-Workers check the flag when the queue is empty and exit gracefully
-Cleaner fit for a shared-queue design than mpsc channels

Advice rejected: 
-Claude suggested using a CPU-Aware policy with a dispatcher-side holding buffer 
that would withhold CPU tasks from the queue entirely. 
-This made the optimized experiment significantly slower because workers sat idle while the dispatcher held tasks back. 
-I moved the scheduling decision into the workers themselves.
They scan the queue and pick the best available task which kept utilization high.
