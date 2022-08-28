use std::process::exit;

use clap::{Parser, Subcommand};

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

fn main() {
    let cli = Cli::parse();
    match &cli.command {
        Some(Commands::Get { key: _ }) => {
            eprintln!("unimplemented");
            exit(1)
        }
        Some(Commands::Set { key: _, value: _ }) => {
            //eprintln!("set {} {}", key, value);
            eprintln!("unimplemented");
            exit(1)
        }
        Some(Commands::RM { key: _ }) => {
            //eprintln!("rm {}", key);
            eprintln!("unimplemented");
            exit(1)
        }
        None => exit(1),
    }
}
