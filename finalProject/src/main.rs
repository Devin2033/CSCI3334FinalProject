#![allow(dead_code)]
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use std::collections::VecDeque;
use std::sync::{Arc, Mutex, atomic::{AtomicBool, AtomicU32, Ordering}};
use std::thread;
use std::time::{Duration, Instant};

//Task Model
#[derive(Debug, Clone)]
enum TaskKind {
    Cpu,
    Io,
}

#[derive(Debug, Clone)]
struct Task {
    id: u32,
    kind: TaskKind,
    duration: Duration,
    cpu_cost: f64,
    arrival_time: Instant,
}

//Scheduling Policy
#[derive(Debug, Clone, PartialEq)]
enum Policy {
    Fifo,
    CpuAware,
}

//Shared State
struct SharedState {
    queue: Mutex<VecDeque<Task>>,
    cpu_usage: Mutex<f64>,
    active_workers: AtomicU32,
    completed: AtomicU32,
    shutdown: AtomicBool,
    policy: Policy,
    cpu_cap: f64,
}

//Metrics
#[derive(Debug, Clone)]
struct Snapshot {
    time_ms: u128,
    cpu_usage: f64,
    active_workers: u32,
    queue_len: usize,
}

struct CompletionRecord {
    _id: u32,
    kind: TaskKind,
    wait_time: Duration,
    turnaround_time: Duration,
}

//Workload Generation
fn generate_tasks(count: u32, io_ratio: f64, seed: u64) -> Vec<Task> {
    let mut rng = StdRng::seed_from_u64(seed);
    let now = Instant::now();
    let mut tasks = Vec::with_capacity(count as usize);

    for i in 0..count {
        let is_io = rng.gen_range(0.0..1.0_f64) < io_ratio;
        let (kind, cpu_cost) = if is_io {
            (TaskKind::Io, 0.10)
        } else {
            (TaskKind::Cpu, 0.35)
        };
        tasks.push(Task {
            id: i,
            kind,
            duration: Duration::from_millis(200),
            cpu_cost,
            arrival_time: now,
        });
    }
    tasks
}

//Worker Logic
fn worker_loop(
    _id: u32,
    state: Arc<SharedState>,
    completions: Arc<Mutex<Vec<CompletionRecord>>>,
) {
    loop {
        let task = {
            let mut q = state.queue.lock().unwrap();
            match state.policy {
                Policy::Fifo => {
                    q.pop_front()
                }
                Policy::CpuAware => {
                    let cpu = *state.cpu_usage.lock().unwrap();
                    let mut io_idx: Option<usize> = None;
                    let mut cpu_idx: Option<usize> = None;

                    for (i, task) in q.iter().enumerate() {
                        match task.kind {
                            TaskKind::Io if io_idx.is_none() => {
                                io_idx = Some(i);
                            }
                            TaskKind::Cpu if cpu_idx.is_none() => {
                                cpu_idx = Some(i);
                            }
                            _ => {}
                        }
                        if io_idx.is_some() && cpu_idx.is_some() { break; }
                    }

                    //Strategy: when CPU usage is high, prefer IO tasks
                    //(they only cost 10% cpu vs 35%), but always take
                    //something if available, idle workers waste time.
                    let chosen = if cpu + 0.35 <= state.cpu_cap + 0.001 {
                        //CPU headroom exists, prefer CPU tasks to drain them
                        //and prevent CPU task starvation
                        cpu_idx.or(io_idx)
                    } else {
                        //CPU is loaded, prefer cheap IO tasks, but still
                        //take a CPU task rather than sit idle
                        io_idx.or(cpu_idx)
                    };

                    match chosen {
                        Some(i) => Some(q.remove(i).unwrap()),
                        None => None,
                    }
                }
            }
        }; //queue lock released

        match task {
            Some(task) => {
                let start = Instant::now();
                let wait_time = start - task.arrival_time;

                {
                    let mut cpu = state.cpu_usage.lock().unwrap();
                    *cpu += task.cpu_cost;
                }
                state.active_workers.fetch_add(1, Ordering::Relaxed);

                thread::sleep(task.duration);

                {
                    let mut cpu = state.cpu_usage.lock().unwrap();
                    *cpu -= task.cpu_cost;
                    if *cpu < 0.0 { *cpu = 0.0; }
                }
                state.active_workers.fetch_sub(1, Ordering::Relaxed);
                state.completed.fetch_add(1, Ordering::Relaxed);

                completions.lock().unwrap().push(CompletionRecord {
                    _id: task.id,
                    kind: task.kind,
                    wait_time,
                    turnaround_time: Instant::now() - task.arrival_time,
                });
            }
            None => {
                if state.shutdown.load(Ordering::Relaxed) {
                    let empty = state.queue.lock().unwrap().is_empty();
                    if empty { break; }
                }
                thread::sleep(Duration::from_millis(1));
            }
        }
    }
}

//Monitor Logic
fn monitor_loop(
    state: Arc<SharedState>,
    start_time: Instant,
    snapshots: Arc<Mutex<Vec<Snapshot>>>,
) {
    loop {
        thread::sleep(Duration::from_millis(10));

        let cpu = *state.cpu_usage.lock().unwrap();
        let active = state.active_workers.load(Ordering::Relaxed);
        let qlen = state.queue.lock().unwrap().len();
        let elapsed = start_time.elapsed().as_millis();

        snapshots.lock().unwrap().push(Snapshot {
            time_ms: elapsed,
            cpu_usage: cpu,
            active_workers: active,
            queue_len: qlen,
        });

        if state.shutdown.load(Ordering::Relaxed) && qlen == 0 && active == 0 {
            break;
        }
    }
}

//Print Results
fn print_results(
    completions: &[CompletionRecord],
    snapshots: &[Snapshot],
    makespan: Duration,
    num_workers: u32,
    policy: &Policy,
) {
    let total = completions.len();
    let io_done = completions.iter().filter(|c| matches!(c.kind, TaskKind::Io)).count();
    let cpu_done = completions.iter().filter(|c| matches!(c.kind, TaskKind::Cpu)).count();

    let avg_wait: f64 = completions.iter()
        .map(|c| c.wait_time.as_millis() as f64)
        .sum::<f64>() / total as f64;

    let avg_turnaround: f64 = completions.iter()
        .map(|c| c.turnaround_time.as_millis() as f64)
        .sum::<f64>() / total as f64;

    let max_wait = completions.iter()
        .map(|c| c.wait_time.as_millis())
        .max()
        .unwrap_or(0);

    let avg_cpu: f64 = if !snapshots.is_empty() {
        snapshots.iter().map(|s| s.cpu_usage).sum::<f64>() / snapshots.len() as f64
    } else { 0.0 };

    let avg_active: f64 = if !snapshots.is_empty() {
        snapshots.iter().map(|s| s.active_workers as f64).sum::<f64>() / snapshots.len() as f64
    } else { 0.0 };

    let avg_qlen: f64 = if !snapshots.is_empty() {
        snapshots.iter().map(|s| s.queue_len as f64).sum::<f64>() / snapshots.len() as f64
    } else { 0.0 };

    println!("\n========== RESULTS ({:?}) ==========", policy);
    println!("Total tasks completed: {}", total);
    println!("  IO tasks:  {}", io_done);
    println!("  CPU tasks: {}", cpu_done);
    println!("Makespan:              {:>8}ms", makespan.as_millis());
    println!("Avg wait time:         {:>8.1}ms", avg_wait);
    println!("Avg turnaround:        {:>8.1}ms", avg_turnaround);
    println!("Max wait time:         {:>8}ms", max_wait);
    println!("Avg CPU usage:         {:>8.1}%", avg_cpu * 100.0);
    println!("Avg active workers:    {:>8.2} / {}", avg_active, num_workers);
    println!("Avg queue length:      {:>8.1}", avg_qlen);
    println!("==========================================\n");
}

//Run Experiment
fn run_experiment(
    policy: Policy, num_workers: u32, task_count: u32,
    io_ratio: f64, arrival_interval: Duration,
    cpu_cap: f64, seed: u64,
) {
    println!("=== Experiment: {:?} ===", policy);
    println!("Workers: {} | Tasks: {} | IO ratio: {:.0}% | CPU cap: {:.0}%",
        num_workers, task_count, io_ratio * 100.0, cpu_cap * 100.0);

    let tasks = generate_tasks(task_count, io_ratio, seed);

    let state = Arc::new(SharedState {
        queue: Mutex::new(VecDeque::new()),
        cpu_usage: Mutex::new(0.0),
        active_workers: AtomicU32::new(0),
        completed: AtomicU32::new(0),
        shutdown: AtomicBool::new(false),
        policy: policy.clone(),
        cpu_cap,
    });

    let completions: Arc<Mutex<Vec<CompletionRecord>>> = Arc::new(Mutex::new(Vec::new()));
    let snapshots: Arc<Mutex<Vec<Snapshot>>> = Arc::new(Mutex::new(Vec::new()));
    let start_time = Instant::now();

    //Spawn monitor
    let mon_state = Arc::clone(&state);
    let mon_snaps = Arc::clone(&snapshots);
    let monitor = thread::spawn(move || {
        monitor_loop(mon_state, start_time, mon_snaps);
    });

    //Spawn workers
    let mut workers = Vec::new();
    for i in 0..num_workers {
        let s = Arc::clone(&state);
        let c = Arc::clone(&completions);
        workers.push(thread::spawn(move || worker_loop(i, s, c)));
    }

    //Dispatcher: feed tasks at arrival intervals
    for mut task in tasks {
        task.arrival_time = Instant::now();
        state.queue.lock().unwrap().push_back(task);
        thread::sleep(arrival_interval);
    }

    println!("All tasks dispatched. Waiting for workers...");
    state.shutdown.store(true, Ordering::Relaxed);

    for w in workers { w.join().unwrap(); }
    monitor.join().unwrap();

    let makespan = start_time.elapsed();
    let comps = completions.lock().unwrap();
    let snaps = snapshots.lock().unwrap();
    print_results(&comps, &snaps, makespan, num_workers, &policy);
}

//Main Function
fn main() {
    let args: Vec<String> = std::env::args().collect();
    let mode = args.get(1).map(|s| s.as_str()).unwrap_or("both");

    let num_workers = 8;
    let task_count = 1000;
    let io_ratio = 0.70;
    let arrival_interval = Duration::from_millis(20);
    let cpu_cap = 0.95;
    let seed = 42;

    match mode {
        "fifo" => {
            run_experiment(Policy::Fifo, num_workers, task_count,
                           io_ratio, arrival_interval, cpu_cap, seed);
        }
        "optimized" => {
            run_experiment(Policy::CpuAware, num_workers, task_count,
                           io_ratio, arrival_interval, cpu_cap, seed);
        }
        _ => {
            run_experiment(Policy::Fifo, num_workers, task_count,
                           io_ratio, arrival_interval, cpu_cap, seed);
            run_experiment(Policy::CpuAware, num_workers, task_count,
                           io_ratio, arrival_interval, cpu_cap, seed);
        }
    }
}