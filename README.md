# telent-plan-kvs

## Note
1. Using BufReaderWithPos make performance a little bit worse, maybe file::seek is efficient enough, meantime if-branchs are extra price?
2. KvStore::index_map would be used both by readers and the writer(singleton), RwLock would be a little bit inefficient(writer is locked during index_map is writting-locked for updating), lock-free map is prefered.