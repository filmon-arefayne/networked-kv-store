use clap::{Parser, Subcommand};
use networked_kv_store::KvStore;

#[derive(Subcommand)]
enum Command {
    /// get key
    Get { key: String },
    /// set key value
    Set { key: String, value: String },
    /// remove key
    Remove { key: String },
}

#[derive(Parser)]
#[command(name = "kvs", version = "1.0", about = "A simple key-value store")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

fn main() {
    let mut kv = KvStore::new();
    let cli = Cli::parse();
    match cli.command {
        Command::Get { key } => {
            // Handle get command
            if let Some(value) = kv.get(&key) {
                println!("Found value: {}", value);
            } else {
                println!("Key not found");
            }
        }
        Command::Set { key, value } => {
            // Handle set command
            println!("Setting key: {} with value: {}", key, value);
            kv.set(key, value);
        }
        Command::Remove { key } => {
            // Handle remove command
            println!("Removing key: {}", key);
            kv.remove(&key);
        }
    }
}
