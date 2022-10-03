//! This module provides various key value storage engines.
use std::path::PathBuf;

use crate::Result;

/// Trait for a key value storage engine.
pub trait KvsEngine: Clone + Send + 'static {
    /// Open the KvStore at a given path. Return the KvStore.
    ///
    /// This will create a new directory if the given one does not exist.
    ///
    /// # Errors
    /// It propagates I/O or deserialization errors during the log replay.fn open(path: impl Into<PathBuf>) -> Result<Self>;
    fn open(path: impl Into<PathBuf>) -> Result<Self>;

    /// Sets the value of a string key to a string.
    ///
    /// If the key already exists, the previous value will be overwritten.
    fn set(&self, key: String, value: String) -> Result<()>;

    /// Gets the string value of a given string key.
    ///
    /// Returns `None` if the given key does not exist.
    fn get(&self, key: String) -> Result<Option<String>>;

    /// Removes a given key.
    ///
    /// # Errors
    ///
    /// It returns `KvsError::KeyNotFound` if the given key is not found.
    fn remove(&self, key: String) -> Result<()>;
}

mod kvs;
mod sled;

pub use self::kvs::KvStore;
pub use self::sled::SledKvsEngine;
