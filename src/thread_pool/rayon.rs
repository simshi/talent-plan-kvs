use super::ThreadPool;
use crate::{KvsError, Result};

/// Wrapper of rayon::ThreadPool
pub struct RayonThreadPool(rayon::ThreadPool);

impl ThreadPool for RayonThreadPool {
    fn new(threads: usize) -> Result<Self> {
        let pool = rayon::ThreadPoolBuilder::new()
            .num_threads(threads as usize)
            .build()
            .map_err(|e| KvsError::StringError(e.to_string()))?;

        Ok(RayonThreadPool(pool))
    }

    fn spawn<F>(&self, job: F)
    where
        F: FnOnce() -> () + Send + 'static,
    {
        self.0.spawn(job);
    }
}
