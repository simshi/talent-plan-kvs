#![deny(missing_docs)]
//! A simple key/value store.

pub use client::KvsClient;
pub use engines::{KvStore, KvsEngine, SledKvsEngine};
pub use error::{KvsError, Result};
pub use server::KvsServer;
// pub use thread_pool::{NaiveThreadPool, RayonThreadPool, SharedQueueThreadPool};

mod client;
mod engines;
mod error;
mod protocol;
mod server;
pub mod thread_pool;
