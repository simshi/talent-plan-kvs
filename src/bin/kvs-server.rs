use std::env::current_dir;
use std::fs;
use std::net::SocketAddr;
use std::process::exit;
use std::str::FromStr; // for SocketAddr::from_str

use clap::{Parser, ValueEnum};
use kvs::thread_pool::{NaiveThreadPool, ThreadPool};
use log::LevelFilter;
use log::{error, info, warn};

use kvs::{KvStore, KvsEngine, KvsServer, Result, SledKvsEngine};

const DEFAULT_LISTENING_ADDRESS: &str = "127.0.0.1:4000";
const DEFAULT_ENGINE: Engine = Engine::kvs;

#[allow(non_camel_case_types)]
#[derive(Debug, Copy, Clone, PartialEq, Eq, ValueEnum)]
// #[clap(rename_all = lower)]
enum Engine {
    kvs,
    sled,
}

impl FromStr for Engine {
    type Err = String;
    fn from_str(s: &str) -> std::result::Result<Engine, Self::Err> {
        match s {
            "kvs" => Ok(Engine::kvs),
            "sled" => Ok(Engine::sled),
            _ => Err(format!("'{}' is not a Engine", s)),
        }
    }
}

#[derive(Parser)]
#[clap(author, version, about, long_about=None)]
struct Cli {
    #[clap(short, long, value_parser, default_value_t = SocketAddr::from_str(DEFAULT_LISTENING_ADDRESS).unwrap())]
    addr: SocketAddr,

    #[clap(arg_enum, short, long, value_parser)]
    engine: Option<Engine>,
}

fn main() -> Result<()> {
    env_logger::builder().filter_level(LevelFilter::Info).init();
    let cli = Cli::parse();

    // TODO: grooming below logic
    let engine = if let Some(current_engine) = current_engine()? {
        if let Some(engine) = cli.engine {
            if current_engine != engine {
                error!("Engine miss match: current engine = {:?}", current_engine);
                exit(1);
            }
            engine
        } else {
            DEFAULT_ENGINE
        }
    } else {
        cli.engine.unwrap_or(DEFAULT_ENGINE)
    };

    info!("kvs-server {}", env!("CARGO_PKG_VERSION"));
    info!("Storage engine: {:?}", engine);
    info!("Listening on {}", cli.addr);

    // write engine to engine file
    fs::write(current_dir()?.join("engine"), format!("{:?}", engine))?;

    let pool = NaiveThreadPool::new(num_cpus::get() as u32)?;

    match engine {
        Engine::kvs => run_with_engine(KvStore::open(current_dir()?)?, pool, cli.addr),
        Engine::sled => run_with_engine(
            SledKvsEngine::new(sled::open(current_dir()?)?),
            pool,
            cli.addr,
        ),
    }
}

fn run_with_engine<E: KvsEngine, P: ThreadPool>(
    engine: E,
    pool: P,
    addr: SocketAddr,
) -> Result<()> {
    let server = KvsServer::new(engine, pool);
    server.run(addr)
}

fn current_engine() -> Result<Option<Engine>> {
    let engine = current_dir()?.join("engine");
    if !engine.exists() {
        return Ok(None);
    }

    match fs::read_to_string(engine)?.parse() {
        Ok(engine) => Ok(Some(engine)),
        Err(e) => {
            warn!("The content of engine file is invalid: {}", e);
            Ok(None)
        }
    }
}
