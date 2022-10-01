use std::cell::RefCell;
use std::collections::BTreeMap;
use std::ffi::OsStr;
use std::fs::{self, File, OpenOptions};
use std::io::prelude::*;
use std::io::{self, BufReader, BufWriter, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock};

use log::{debug, error};
use serde::{Deserialize, Serialize};
use serde_json::Deserializer;

use crate::{KvsEngine, KvsError, Result};

// compact if more than `threshold` bytes can be saved
const COMPACTION_THRESHOLD: u64 = 1024 * 1024;

// TODO: use concurrent version later, SkipMap?
type ReaderMap = BTreeMap<u64, BufReader<File>>;
type IndexMap = BTreeMap<String, CommandPos>;
// interior mutability
// +========+    +========+    +========+
// |KvStore1|    |KvStore2|    |KvStore3|
// +--------+    +--------+    +--------+
// |path    |    |path    |    |path    |
// |reader  |    |reader  |    |reader  |
// +========+    +========+    +========+
//    |    \      /     \       /    |
//    +-----\----+-------\-----+     |
//    |      \            \          |
//    |       +------------+---------+   all readers
//    |                    |                |
//    v                    v                v
// +=============+     +==========+    +=========+
// |KvStoreWriter| --> |KvIndexMap|    |first_gen|
// +-------------+     +==========+    +====^====+
// | path        |                          |
// | reader -----|--------------------------+
// +=============+

/// The `KvStore` stores string key/value pairs.
///
/// Key/value pairs are persisted to disk in log files. Log files are named after
/// monotonically increasing generation numbers with a `log` extension name.
/// A `BTreeMap` in memory stores the keys and the value locations for fast query.
///
/// Example:
///
/// ```rust
/// # use kvs::{KvStore, Result};
/// # fn try_main() -> Result<()> {
/// use std::env::current_dir;
/// use kvs::KvsEngine;
/// let mut store = KvStore::open(current_dir()?)?;
/// store.set("key".to_owned(), "value".to_owned())?;
/// let val = store.get("key".to_owned())?;
/// assert_eq!(val, Some("value".to_owned()));
/// # Ok(())
/// # }
/// ```
pub struct KvStore {
    // path for the log files.
    path: PathBuf,
    // index to file position, operations work with its' reference
    index: Arc<RwLock<IndexMap>>,
    // first generation availble, changes due to compaction
    first_gen: Arc<AtomicU64>,

    // map gen to file reader
    reader: KvStoreReader,
    // Interior KvsStoreWriter works as a singleton
    writer: Arc<Mutex<KvStoreWriter>>,
    // current_gen: u64,
    // // log writer
    // writer: Arc<Mutex<BufWriterWithPos<File>>>,
    // // the number of bytes representing "stale" commands that could be
    // // deleted during a compaction.
    // stale_bytes: u64,
}
impl Clone for KvStore {
    fn clone(&self) -> Self {
        Self {
            path: self.path.clone(),
            index: self.index.clone(),
            first_gen: self.first_gen.clone(),
            reader: self.reader.clone(),
            writer: self.writer.clone(),
        }
    }
}

impl KvStore {
    /// Open the KvStore at a given path. Return the KvStore.
    ///
    /// This will create a new directory if the given one does not exist.
    ///
    /// # Errors
    /// It propagates I/O or deserialization errors during the log replay.
    pub fn open(path: impl Into<PathBuf>) -> Result<KvStore> {
        let path = path.into();
        fs::create_dir_all(&path)?;
        debug!("open:{:?}", path);

        // let mut index = BTreeMap::new();
        let index = Arc::new(RwLock::new(IndexMap::new()));
        // build all exist logs into readers
        let mut readers = ReaderMap::new();

        let gen_list = sorted_gen_list(&path)?;
        let mut stale_bytes = 0;

        for &gen in &gen_list {
            let filepath = log_file_path(&path, gen);
            let mut reader = BufReader::new(File::open(&filepath)?);
            stale_bytes += load_log(gen, &mut reader, &*index)?;
            readers.insert(gen, reader);
        }
        debug!("found readers: {}", readers.len());

        let current_gen = gen_list.last().unwrap_or(&0) + 1;
        let first_gen = Arc::new(AtomicU64::new(current_gen));
        let reader = KvStoreReader {
            path: path.clone(),
            first_gen: first_gen.clone(),
            readers: RefCell::new(readers),
        };

        let writer = new_log_file(&path, current_gen)?;
        let writer = KvStoreWriter {
            path: path.clone(),
            current_gen,
            reader: reader.clone(),
            writer,
            stale_bytes,
            index: index.clone(),
        };

        debug!("open done");
        Ok(KvStore {
            path,
            index,
            first_gen,
            reader,
            writer: Arc::new(Mutex::new(writer)),
        })
    }
}

impl KvsEngine for KvStore {
    /// Sets the value of a string key to a string.
    ///
    /// If the key already exists, the previous value will be overwritten.
    ///
    /// # Errors
    /// It propagates I/O or serialization errors during writing the log.
    fn set(&self, key: String, value: String) -> Result<()> {
        self.writer.lock().unwrap().set(key, value)
    }

    /// Gets the string value of a given string key.
    ///
    /// Returns `None` if the given key does not exist.
    ///
    /// # Errors
    /// It returns `KvsError::UnexpectedCommandType` if the given command type unexpected.
    fn get(&self, key: String) -> Result<Option<String>> {
        if let Some(cmd_pos) = self.index.read().unwrap().get(&key) {
            // println!("get: {:?}, pos:{:?}", key, &cmd_pos);
            let cmd_pos = *cmd_pos;
            self.reader.read_and(&cmd_pos, |rdr| {
                if let Command::Set { value, .. } = serde_json::from_reader(rdr)? {
                    Ok(Some(value))
                } else {
                    Err(KvsError::UnexpectedCommandType)
                }
            })
            // let reader = self
            //     .readers
            //     .borrow_mut()
            //     .get_mut(&cmd_pos.gen)
            //     .expect("Cannot find log reader");

            // reader.seek(SeekFrom::Start(cmd_pos.pos))?;
            // let cmd_rdr = reader.take(cmd_pos.len);
            // if let Command::Set { value, .. } = serde_json::from_reader(cmd_rdr)? {
            //     Ok(Some(value))
            // } else {
            //     Err(KvsError::UnexpectedCommandType)
            // }
        } else {
            Ok(None)
        }
    }

    /// Remove a given key.
    ///
    /// # Errors
    /// It returns `KvsError::KeyNotFound` if the given key is not found.
    /// It propagates I/O or serialization errors during writing the log.
    fn remove(&self, key: String) -> Result<()> {
        self.writer.lock().unwrap().remove(key)
    }
}

// after KvStoreWriter compacts, `first_gen` updates, readers must be updated
// accordingly, so let's extract this logic as KvStoreReader, and it's lazy on
// opening files, `Send + !Sync`
struct KvStoreReader {
    path: PathBuf,
    first_gen: Arc<AtomicU64>,
    // map gen to file reader, use interior mutability due to accessing from
    // multiple places in same thread (we're `Send` but not `Sync`)
    readers: RefCell<BTreeMap<u64, BufReader<File>>>,
}
impl Clone for KvStoreReader {
    fn clone(&self) -> Self {
        Self {
            path: self.path.clone(),
            first_gen: self.first_gen.clone(),
            readers: RefCell::new(ReaderMap::new()), // each instance maintains a unique map of readers
        }
    }
}
impl KvStoreReader {
    /// Close file handles with generation number less than first_gen.
    ///
    /// `first_gen` is updated to the latest compaction gen after a compaction finishes.
    /// The compaction generation contains the sum of all operations before it and the
    /// in-memory index contains no entries with generation number less than it.
    /// So we can safely close those file handles and the stale files can be deleted.
    fn close_stale_files(&self) {
        let mut readers = self.readers.borrow_mut();
        while !readers.is_empty() {
            let gen = *readers.keys().next().unwrap();
            if gen >= self.first_gen.load(Ordering::SeqCst) {
                break;
            }
            readers.remove(&gen);
        }
    }

    /// Read the log file at the given `CommandPos`.
    fn read_and<F, R>(&self, cmd_pos: &CommandPos, f: F) -> Result<R>
    where
        F: FnOnce(io::Take<&mut BufReader<File>>) -> Result<R>,
    {
        self.close_stale_files();

        let mut readers = self.readers.borrow_mut();
        if !readers.contains_key(&cmd_pos.gen) {
            let reader = BufReader::new(File::open(log_file_path(&self.path, cmd_pos.gen))?);
            readers.insert(cmd_pos.gen, reader);
        }

        let reader = readers.get_mut(&cmd_pos.gen).unwrap();
        reader.seek(SeekFrom::Start(cmd_pos.pos))?;
        let cmd_reader = reader.take(cmd_pos.len);

        f(cmd_reader)
    }

    // fn read
}

// KvStoreWriter singleton
struct KvStoreWriter {
    path: PathBuf,
    current_gen: u64,
    // read helper
    reader: KvStoreReader,
    writer: BufWriterWithPos<File>,
    // writer: Arc<Mutex<BufWriterWithPos<File>>>,
    // the number of bytes representing "stale" commands that could be
    // deleted during a compaction.
    stale_bytes: u64,

    // index reference to KvsStore
    index: Arc<RwLock<BTreeMap<String, CommandPos>>>,
}
impl KvStoreWriter {
    fn set(&mut self, key: String, value: String) -> Result<()> {
        let cmd = Command::set(key, value);
        let pos = self.writer.pos;
        serde_json::to_writer(&mut self.writer, &cmd)?;
        self.writer.flush()?;
        let len = self.writer.pos - pos;

        if let Command::Set { key, .. } = cmd {
            // println!("insert key {:?} value {:?}", key, value);
            if let Some(CommandPos { len, .. }) = self
                .index
                .write()
                .unwrap()
                .insert(key, (self.current_gen, pos, len).into())
            {
                // overwritten case
                self.stale_bytes += len;
            }
        }

        if self.stale_bytes > COMPACTION_THRESHOLD {
            self.compact()?;
        }
        // println!("set: {:?}", serde_json::to_string(&cmd).unwrap());
        Ok(())
    }

    fn remove(&mut self, key: String) -> Result<()> {
        // don't remove the key immediately, make sure writer successful first!
        if !self.index.read().unwrap().contains_key(&key) {
            // println!("not find key: {:?}", key);
            return Err(KvsError::KeyNotFound);
        }

        // println!("find key: {:?}", &key);
        let cmd = Command::remove(key);
        serde_json::to_writer(&mut self.writer, &cmd)?;
        self.writer.flush()?;

        // flushed, now we're safe to remove the key
        if let Command::Remove { key } = cmd {
            if let Some(CommandPos { len, .. }) = self.index.write().unwrap().remove(&key) {
                self.stale_bytes += len;
            }
        }
        Ok(())
    }

    /// Collect space by writing entries to a new log file then remove old log
    /// files, staled bytes then gone.
    fn compact(&mut self) -> Result<()> {
        // current_gen + 1 for the compaction log.
        let compaction_gen = self.current_gen + 1;
        self.current_gen += 2;
        self.writer = new_log_file(&self.path, self.current_gen)?;

        // write all KV to a new log file.
        let mut compaction_writer = new_log_file(&self.path, compaction_gen)?;
        let mut new_pos = 0;
        for cmd_pos in self.index.write().unwrap().values_mut() {
            let len = self.reader.read_and(cmd_pos, |mut rdr| {
                Ok(io::copy(&mut rdr, &mut compaction_writer)?)
            })?;
            *cmd_pos = (compaction_gen, new_pos, len).into();
            new_pos = compaction_writer.pos;
        }
        compaction_writer.flush()?;

        // after update `first_gen`, all `KvStoreReader`s will sense it and
        // close files' handles of stale generations
        self.reader
            .first_gen
            .store(compaction_gen, Ordering::SeqCst);
        self.reader.close_stale_files();

        // remove stale log files
        // Note that actually these files are not deleted immediately because `KvStoreReader`s
        // still keep open file handles. When `KvStoreReader` is used next time, it will clear
        // its stale file handles. On Unix, the files will be deleted after all the handles
        // are closed. On Windows, the deletions below will fail and stale files are expected
        // to be deleted in the next compaction.
        let stale_gens = sorted_gen_list(&self.path)?
            .into_iter()
            .filter(|gen| gen < &compaction_gen);

        for stale_gen in stale_gens {
            // `remove_file` might return error on windows, but would succeed
            // eventually at later compactions
            let file_path = log_file_path(&self.path, stale_gen);
            if let Err(err) = fs::remove_file(&file_path) {
                error!("Failed to remove file: {}", err);
            }
        }

        // fresh as new born
        self.stale_bytes = 0;

        Ok(())
    }
}

/// Struct representing a command.
#[derive(Debug, Serialize, Deserialize)]
enum Command {
    Set { key: String, value: String },
    Remove { key: String },
}
impl Command {
    fn set(key: String, value: String) -> Self {
        Command::Set { key, value }
    }

    fn remove(key: String) -> Self {
        Command::Remove { key }
    }
}

/// Represents the position and length of a (json)serialized command in the log.
#[derive(Clone, Copy, Debug)]
struct CommandPos {
    gen: u64,
    pos: u64,
    len: u64,
}
impl From<(u64, u64, u64)> for CommandPos {
    fn from((gen, pos, len): (u64, u64, u64)) -> Self {
        CommandPos { gen, pos, len }
    }
}

// trace pos to reduce several calls to seek for performance
// struct BufReaderWithPos<R: Read + Seek> {
//     inner: BufReader<R>,
//     pos: u64,
// }
// impl<R: Read + Seek> BufReaderWithPos<R> {
//     fn new(mut inner: R) -> Result<Self> {
//         let pos = inner.seek(SeekFrom::Current(0))?;
//         Ok(BufReaderWithPos {
//             inner: BufReader::new(inner),
//             pos,
//         })
//     }
// }
// impl<R: Read + Seek> Read for BufReaderWithPos<R> {
//     fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
//         let len = self.inner.read(buf)?;
//         self.pos += len as u64;

//         Ok(len)
//     }
// }
// impl<R: Read + Seek> Seek for BufReaderWithPos<R> {
//     fn seek(&mut self, pos: SeekFrom) -> io::Result<u64> {
//         self.pos = self.inner.seek(pos)?;
//         Ok(self.pos)
//     }
// }

// trace pos/len because `serde_json::to_write()` doesn't return written size
struct BufWriterWithPos<W: Write + Seek> {
    inner: BufWriter<W>,
    pos: u64,
}
impl<W: Write + Seek> BufWriterWithPos<W> {
    fn new(mut inner: W) -> Result<Self> {
        let pos = inner.seek(SeekFrom::Current(0))?;
        Ok(BufWriterWithPos {
            inner: BufWriter::new(inner),
            pos,
        })
    }
}

impl<W: Write + Seek> Write for BufWriterWithPos<W> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let len = self.inner.write(buf)?;
        self.pos += len as u64;

        Ok(len)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.inner.flush()
    }
}
impl<W: Write + Seek> Seek for BufWriterWithPos<W> {
    fn seek(&mut self, pos: SeekFrom) -> io::Result<u64> {
        self.pos = self.inner.seek(pos)?;
        Ok(self.pos)
    }
}

fn log_file_path(dir: &Path, gen: u64) -> PathBuf {
    dir.join(format!("{}.log", gen))
}

/// Create a new log file with given generation number and add the reader to the readers map.
///
/// Returns the writer to the log.
fn new_log_file(path: &Path, gen: u64) -> Result<BufWriterWithPos<File>> {
    let filepath = log_file_path(path, gen);
    let file = OpenOptions::new()
        .create(true)
        .write(true)
        .append(true)
        .open(&filepath)?;

    Ok(BufWriterWithPos::new(file)?)
}

/// Returns sorted generation numbers in the given directory.
fn sorted_gen_list(path: &Path) -> Result<Vec<u64>> {
    let mut gen_list: Vec<u64> = fs::read_dir(&path)?
        .flat_map(|res| -> Result<_> { Ok(res?.path()) })
        .filter(|path| path.is_file() && path.extension() == Some("log".as_ref()))
        .flat_map(|path| {
            path.file_name()
                .and_then(OsStr::to_str)
                .map(|s| s.trim_end_matches(".log"))
                .map(str::parse::<u64>)
        })
        .flatten()
        .collect();

    gen_list.sort_unstable();
    Ok(gen_list)
}

/// Load the whole log file and store value locations in the index map.
///
/// Returns how many bytes can be saved after a compaction.
fn load_log(
    gen: u64,
    reader: &mut BufReader<File>,
    index: &RwLock<IndexMap>, //&mut BTreeMap<String, CommandPos>
) -> Result<u64> {
    // To make sure we read from the beginning of the file.
    let mut pos = reader.seek(SeekFrom::Start(0))?;
    let mut stream = Deserializer::from_reader(reader).into_iter::<Command>();
    let mut staled_bytes = 0; // number of bytes that can be saved after a compaction.

    while let Some(cmd) = stream.next() {
        let new_pos = stream.byte_offset() as u64;
        match cmd? {
            Command::Set { key, .. } => {
                let cmd_pos = CommandPos {
                    gen,
                    pos,
                    len: new_pos - pos,
                };
                if let Some(old_cmd) = index.write().unwrap().insert(key, cmd_pos) {
                    staled_bytes += old_cmd.len;
                }
            }
            Command::Remove { key } => {
                if let Some(old_cmd) = index.write().unwrap().remove(&key) {
                    staled_bytes += old_cmd.len;
                }
                // the "remove" command itself can be deleted in the next compaction.
                // so we add its length to `uncompacted`.
                staled_bytes += new_pos - pos;
            }
        }
        pos = new_pos;
    }

    Ok(staled_bytes)
}
