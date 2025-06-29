use clap::{Parser, Subcommand};
use networked_kv_store::KvStore;
use networked_kv_store::KvsError;
use networked_kv_store::Result;

#[derive(Subcommand)]
enum Command {
    /// get key
    Get { key: String },
    /// set key value
    Set { key: String, value: String },
    /// remove key
    Rm { key: String },
}

#[derive(Parser)]
#[command(name = "kvs", version = env!("CARGO_PKG_VERSION"), author = env!("CARGO_PKG_AUTHORS"), about = "A simple key-value store")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

fn main() -> Result<()> {
    let cli = Cli::parse();

    match cli.command {
        Command::Get { key } => {
            let store = KvStore::new()?;

            if let Some(value) = store.get(key) {
                println!("{}", value);
            } else {
                println!("Key not found");
                std::process::exit(0);
            }
        }
        Command::Set { key, value } => {
            let mut store = KvStore::new()?;

            store.set(key, value)?;
            std::process::exit(0);
        }
        Command::Rm { key } => {
            let mut store = KvStore::new()?;
            match store.remove(key) {
                Ok(_) => std::process::exit(0),
                Err(e) => match e {
                    KvsError::KeyNotFound => {
                        println!("Key not found");
                        std::process::exit(1);
                    }
                    _ => {
                        eprintln!("Error: {}", e);
                        std::process::exit(1);
                    }
                },
            }
        }
    }
    Ok(())
}
