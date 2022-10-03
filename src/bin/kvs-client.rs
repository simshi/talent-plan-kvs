use std::net::SocketAddr;
use std::process::exit;
use std::str::FromStr;

use clap::{Parser, Subcommand};

use kvs::{KvsClient, Result};

const DEFAULT_LISTENING_ADDRESS: &str = "127.0.0.1:4000";
const ADDRESS_FORMAT: &str = "IP:PORT";

#[derive(Parser)]
#[clap(author, version, about, long_about=None)]
struct Cli {
    #[clap(subcommand)]
    command: Option<Commands>,
}

#[derive(Subcommand)]
enum Commands {
    Get {
        #[clap(value_parser, value_name=ADDRESS_FORMAT, short, long, help = "Sets the server address", default_value_t= SocketAddr::from_str(DEFAULT_LISTENING_ADDRESS).unwrap())]
        addr: SocketAddr,

        #[clap(value_parser)]
        key: String,
    },
    Set {
        #[clap(value_parser, value_name=ADDRESS_FORMAT, short, long, help = "Sets the server address", default_value_t= SocketAddr::from_str(DEFAULT_LISTENING_ADDRESS).unwrap())]
        addr: SocketAddr,

        #[clap(value_parser)]
        key: String,
        value: String,
    },
    RM {
        #[clap(value_parser, value_name=ADDRESS_FORMAT, short, long, help = "Sets the server address", default_value_t= SocketAddr::from_str(DEFAULT_LISTENING_ADDRESS).unwrap())]
        addr: SocketAddr,

        #[clap(value_parser)]
        key: String,
    },
}

fn main() -> Result<()> {
    let cli = Cli::parse();
    match &cli.command {
        Some(Commands::Get { addr, key }) => {
            let mut client = KvsClient::connect(addr)?;
            if let Some(value) = client.get(key.to_string())? {
                println!("{}", value);
            } else {
                println!("Key not found");
            }
        }
        Some(Commands::Set { addr, key, value }) => {
            let mut client = KvsClient::connect(addr)?;
            client.set(key.to_string(), value.to_string())?;
        }
        Some(Commands::RM { addr, key }) => {
            let mut client = KvsClient::connect(addr)?;
            client.remove(key.to_string())?;
        }
        None => exit(1),
    }

    Ok(())
}
