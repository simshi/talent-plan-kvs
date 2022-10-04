# Talent Plan Key-Value Store
  Talent Plan course, implementing a Key-Value Store in Rust.

## Note
### Multiple threads
1. Using BufReaderWithPos make performance a little bit worse, maybe file::seek is efficient enough, meantime if-branchs are extra price?
2. KvStore::index_map would be used both by readers and the writer(singleton), RwLock would be a little bit inefficient(writer is locked during index_map is writting-locked for updating), lock-free map is prefered. Using `DashMap` now.
3. Possible optimization: `engine` is cloned for each incoming connection, it leads to create `ReaderMap` for every request, which is not necessarily, considerating using `syncpool`.
    - before optimization, clones engine for each connection:
    ```rust
      for stream in listener.incoming() {
          let engine = self.engine.clone();
          self.pool.spawn(move || {
              serve(engine, stream);
              // ...
          })
      }
    ```
    - draft the optimization, replace `KvsEngine::clone()` with `Arc<SyncPool>::clone()`, **25% performance improved in thread-pool-size=1, no change in other size, interesting...**
    ```rust
      pub struct KvsServer<E: KvsEngine, P: ThreadPool> {
          // ... other fields
          engine_pool: Arc<SyncPool<E>>,
      }
      for stream in listener.incoming() {
          let engine_pool = self.engine_pool.clone();
          self.pool.spawn(move || {
              serve(engine_pool, stream);
              // ...
          })
      }
    ```