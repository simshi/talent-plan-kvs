use std::io::{BufReader, BufWriter, Write};
use std::net::{TcpListener, TcpStream, ToSocketAddrs};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use crossbeam::queue::SegQueue;
use log::{debug, error};
use serde_json::Deserializer;

use crate::protocol::*;
use crate::thread_pool::ThreadPool;
use crate::{KvsEngine, Result};

/// The server of a key value store.
pub struct KvsServer<E: KvsEngine, P: ThreadPool> {
    pool: P,
    stop: Arc<AtomicBool>,
    engine_pool: Arc<SyncPool<E>>,
}

// naive syncpool
struct SyncPool<E: KvsEngine> {
    engine: Mutex<E>,
    queue: SegQueue<E>,
}
impl<E: KvsEngine> SyncPool<E> {
    fn new(engine: E) -> Self {
        Self {
            engine: Mutex::new(engine),
            queue: SegQueue::new(),
        }
    }

    fn get(&self) -> E {
        // loop for safety simutaneous getting due to pop-push is not atomic
        // e.g. thread2 rob the one created by thread1:
        //    thread1     |   thread2
        // ===============+===============
        // 1. pop: None   |
        // 2. push(cloned)|
        // 3.             | pop: Some(<from thread1 just pushed>)
        // 4. pop: None   |
        // 5. push again  |
        loop {
            if let Some(engine) = self.queue.pop() {
                return engine;
            }
            self.queue.push(self.engine.lock().unwrap().clone());
        }
    }
    fn put(&self, engine: E) {
        self.queue.push(engine)
    }
}

impl<E: KvsEngine, P: ThreadPool> KvsServer<E, P> {
    /// Create a `KvsServer` with a given storage engine.
    pub fn new(engine: E, pool: P, stop: Arc<AtomicBool>) -> Self {
        KvsServer {
            pool,
            stop,
            engine_pool: Arc::new(SyncPool::new(engine)),
        }
    }

    /// Run the server listening on the given address
    pub fn run<A: ToSocketAddrs>(self, addr: A) -> Result<()> {
        let listener = TcpListener::bind(addr)?;
        for stream in listener.incoming() {
            if self.stop.load(Ordering::SeqCst) {
                break;
            }
            let engine_pool = self.engine_pool.clone();
            self.pool.spawn(move || match stream {
                Ok(stream) => {
                    if let Err(e) = serve(engine_pool, stream) {
                        error!("Error on serve client: {}", e);
                    }
                }
                Err(e) => error!("Connection failed: {}", e),
            })
        }
        Ok(())
    }
}

fn serve<E>(engine_pool: Arc<SyncPool<E>>, tcp: TcpStream) -> Result<()>
where
    E: KvsEngine,
{
    let peer_addr = tcp.peer_addr()?;
    let reader = BufReader::new(&tcp);
    let mut writer = BufWriter::new(&tcp);
    let req_rdr = Deserializer::from_reader(reader).into_iter::<Request>();

    macro_rules! send_resp {
        ($resp:expr) => {{
            let resp = $resp;
            serde_json::to_writer(&mut writer, &resp)?;
            writer.flush()?;
            debug!("Response sent to {}: {:?}", peer_addr, resp);
        };};
    }

    let engine = engine_pool.get();
    for req in req_rdr {
        let req = req?;
        debug!("Receive request from {}: {:?}", peer_addr, req);

        match req {
            Request::Get { key } => {
                send_resp!(match engine.get(key) {
                    Ok(value) => GetResponse::Ok(value),
                    Err(e) => GetResponse::Err(e.to_string()),
                });
            }
            Request::Set { key, value } => {
                send_resp!(match engine.set(key, value) {
                    Ok(_) => SetResponse::Ok(()),
                    Err(e) => SetResponse::Err(e.to_string()),
                });
            }
            Request::Remove { key } => {
                send_resp!(match engine.remove(key) {
                    Ok(_) => RemoveResponse::Ok(()),
                    Err(e) => RemoveResponse::Err(e.to_string()),
                });
            }
        }
    }
    engine_pool.put(engine);

    Ok(())
}
