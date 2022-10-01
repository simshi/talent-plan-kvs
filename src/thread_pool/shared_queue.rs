use std::thread;

use super::ThreadPool;
use crate::Result;

/// It is actually not a thread pool. It spawns a new thread every time
/// the `spawn` method is called.
pub struct SharedQueueThreadPool;

impl ThreadPool for SharedQueueThreadPool {
    fn new(_threads: u32) -> Result<Self> {
        Ok(SharedQueueThreadPool)
    }

    fn spawn<F>(&self, job: F)
    where
        F: FnOnce() -> () + Send + 'static,
    {
        thread::spawn(job);
    }
}
