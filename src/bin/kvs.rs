use std::env::current_dir;
use std::process::exit;

use clap::{Parser, Subcommand};

use kvs::{KvStore, KvsError, Result};

#[derive(Parser)]
#[clap(author, version, about, long_about=None)]
struct Cli {
    #[clap(subcommand)]
    command: Option<Commands>,
}

#[derive(Subcommand)]
enum Commands {
    Get {
        #[clap(value_parser)]
        key: String,
    },
    Set {
        #[clap(value_parser)]
        key: String,
        value: String,
    },
    RM {
        #[clap(value_parser)]
        key: String,
    },
}

fn main() -> Result<()> {
    let cli = Cli::parse();
    match &cli.command {
        Some(Commands::Get { key }) => {
            let mut store = KvStore::open(current_dir()?)?;
            if let Some(value) = store.get(key.to_string())? {
                println!("{}", value);
            } else {
                println!("Key not found");
            }
        }
        Some(Commands::Set { key, value }) => {
            let mut store = KvStore::open(current_dir()?)?;
            store.set(key.to_string(), value.to_string())?;
        }
        Some(Commands::RM { key }) => {
            let mut store = KvStore::open(current_dir()?)?;
            match store.remove(key.to_string()) {
                Ok(()) => {}
                Err(KvsError::KeyNotFound) => {
                    println!("Key not found");
                    exit(1);
                }
                Err(err) => return Err(err),
            }
        }
        None => exit(1),
    }

    Ok(())
}
