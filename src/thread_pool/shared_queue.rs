use std::thread;

use crossbeam::channel::{self, Receiver, Sender};
use log::{debug, error};

use super::ThreadPool;
use crate::Result;

// Note for Rust training course: the thread pool is not implemented using
// `catch_unwind` because it would require the task to be `UnwindSafe`.

/// A thread pool using a shared queue inside.
///
/// If a spawned task panics, the old thread will be destroyed and a new one will be
/// created. It fails silently when any failure to create the thread at the OS level
/// is captured after the thread pool is created. So, the thread number in the pool
/// can decrease to zero, then spawning a task to the thread pool will panic.
pub struct SharedQueueThreadPool {
    tx: Sender<Box<dyn FnOnce() + Send + 'static>>,
}

impl ThreadPool for SharedQueueThreadPool {
    fn new(threads: usize) -> Result<Self> {
        let (tx, rx) = channel::unbounded();
        for _ in 0..threads {
            let rx = TaskReceiver(rx.clone());
            thread::Builder::new().spawn(move || run_task(rx))?;
        }
        Ok(SharedQueueThreadPool { tx })
    }

    fn spawn<F>(&self, job: F)
    where
        F: FnOnce() + Send + 'static,
    {
        self.tx
            .send(Box::new(job))
            .expect("The thread pool has no thread.");
    }
}

#[derive(Clone)]
struct TaskReceiver(Receiver<Box<dyn FnOnce() + Send + 'static>>);
impl Drop for TaskReceiver {
    fn drop(&mut self) {
        if thread::panicking() {
            let rx = self.clone();
            if let Err(err) = thread::Builder::new().spawn(move || run_task(rx)) {
                error!("Failed to spawn a thread: {}", err);
            }
        }
    }
}

fn run_task(rx: TaskReceiver) {
    loop {
        match rx.0.recv() {
            Ok(job) => job(),
            Err(_) => {
                debug!("Thread exits because the thread pool is destroyed.");
                return;
            }
        }
    }
}
