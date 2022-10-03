use std::{
    sync::{atomic::AtomicBool, atomic::Ordering, Arc},
    thread,
    time::Duration,
};

use criterion::{criterion_group, criterion_main, Bencher, BenchmarkId, Criterion};
use crossbeam::sync::WaitGroup;
use log::warn;
use tempfile::TempDir;

use kvs::{
    thread_pool::{RayonThreadPool, SharedQueueThreadPool, ThreadPool},
    KvStore, KvsClient, KvsEngine, KvsServer, SledKvsEngine,
};

const ENTRY_COUNT: usize = 100;
const THREAD_COUNT: [usize; 4] = [1, 2, 4, 8];
const ADDRESS: &str = "127.0.0.1:4001";

fn write_bench<E: KvsEngine, P: ThreadPool>(b: &mut Bencher, pool_size: usize) {
    let is_stop = Arc::new(AtomicBool::new(false));
    let is_stop_c = is_stop.clone();
    let handle = thread::spawn(move || {
        let dir = TempDir::new().unwrap();
        let eng = E::open(dir.path()).unwrap();
        let server_pool = P::new(pool_size).unwrap();
        let server = KvsServer::new(eng, server_pool, is_stop_c);

        server.run(&ADDRESS.to_owned()).unwrap();
    });

    let value = "value".to_owned();
    let keys: Vec<String> = (0..ENTRY_COUNT).map(|x| format!("key{}", x)).collect();
    let client_pool = RayonThreadPool::new(ENTRY_COUNT).unwrap();

    thread::sleep(Duration::from_secs(1));
    b.iter(|| {
        let wg = WaitGroup::new();
        for i in 0..ENTRY_COUNT {
            let key = keys[i].clone();
            let value = value.clone();
            let wg = wg.clone();
            client_pool.spawn(move || {
                let mut client = KvsClient::connect(ADDRESS).unwrap();
                client.set(key, value).unwrap();
                drop(wg);
            });
        }
        wg.wait();
    });

    is_stop.store(true, Ordering::SeqCst);
    let _ = KvsClient::connect(ADDRESS);
    if let Err(err) = handle.join() {
        warn!("exit server failed because {:?}", err);
    }
}

fn write_benches(c: &mut Criterion) {
    let mut group = c.benchmark_group("write");
    for size in THREAD_COUNT.iter() {
        group.bench_with_input(
            BenchmarkId::new("sledengine", format!("rayon-{}", size)),
            size,
            |b, pool_size| write_bench::<SledKvsEngine, RayonThreadPool>(b, *pool_size),
        );
        group.bench_with_input(
            BenchmarkId::new("kvstore", format!("rayon-{}", size)),
            size,
            |b, pool_size| write_bench::<KvStore, RayonThreadPool>(b, *pool_size),
        );
        group.bench_with_input(
            BenchmarkId::new("kvstore", format!("queued-{}", size)),
            size,
            |b, pool_size| write_bench::<KvStore, SharedQueueThreadPool>(b, *pool_size),
        );
    }
    group.finish();
}

fn read_bench<E: KvsEngine, P: ThreadPool>(b: &mut Bencher, pool_size: usize) {
    let is_stop = Arc::new(AtomicBool::new(false));
    let is_stop_c = is_stop.clone();
    let handle = thread::spawn(move || {
        let dir = TempDir::new().unwrap();
        let eng = E::open(dir.path()).unwrap();
        let server_pool = P::new(pool_size).unwrap();
        let server = KvsServer::new(eng, server_pool, is_stop_c);

        server.run(&ADDRESS.to_owned()).unwrap();
    });

    let value = "value".to_owned();
    let keys: Vec<String> = (0..ENTRY_COUNT).map(|x| format!("key{}", x)).collect();
    let client_pool = RayonThreadPool::new(ENTRY_COUNT).unwrap();

    thread::sleep(Duration::from_secs(1));
    for i in 0..ENTRY_COUNT {
        let mut write_client = KvsClient::connect(ADDRESS).unwrap();
        let key = keys[i].clone();
        let value = value.clone();
        write_client.set(key, value).unwrap();
    }

    b.iter(|| {
        let wg = WaitGroup::new();
        for i in 0..ENTRY_COUNT {
            let key = keys[i].clone();
            let wg = wg.clone();
            client_pool.spawn(move || {
                let mut client = KvsClient::connect(ADDRESS).unwrap();
                client.get(key).unwrap();
                drop(wg);
            });
        }
        wg.wait();
    });

    is_stop.store(true, Ordering::SeqCst);
    let _ = KvsClient::connect(ADDRESS);
    if let Err(err) = handle.join() {
        warn!("exit server failed because {:?}", err);
    }
}

fn read_benches(c: &mut Criterion) {
    let mut group = c.benchmark_group("read");
    for size in THREAD_COUNT.iter() {
        group.bench_with_input(
            BenchmarkId::new("sledengine", format!("rayon-{}", size)),
            size,
            |b, pool_size| read_bench::<KvStore, RayonThreadPool>(b, *pool_size),
        );
        group.bench_with_input(
            BenchmarkId::new("kvstore", format!("rayon-{}", size)),
            size,
            |b, pool_size| read_bench::<KvStore, RayonThreadPool>(b, *pool_size),
        );
        group.bench_with_input(
            BenchmarkId::new("kvstore", format!("queued-{}", size)),
            size,
            |b, pool_size| read_bench::<KvStore, SharedQueueThreadPool>(b, *pool_size),
        );
    }
    group.finish();
}

criterion_group! {
    name = benches;
    config = Criterion::default().sample_size(10).measurement_time(Duration::from_secs(10));
    targets = write_benches, read_benches
}
criterion_main!(benches);
