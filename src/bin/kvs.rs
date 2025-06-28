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
#[command(name = "kvs", version = env!("CARGO_PKG_VERSION"), author = env!("CARGO_PKG_AUTHORS"), about = "A simple key-value store")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

fn main() -> Result<(), std::io::Error> {
    let cli = Cli::parse();
    let mut store = KvStore::open(std::env::current_dir()?.as_path())?;

    match cli.command {
        Command::Get { key } => {
            if let Some(value) = store.get(key) {
                println!("{}", value);
            } else {
                println!("Key not found");
                std::process::exit(0); // Test expects success for missing key
            }
        }
        Command::Set { key, value } => {
            store.set(key, value)?;
            std::process::exit(0); // Ensure we exit with success
        }
        Command::Remove { key } => match store.remove(key) {
            Ok(_) => std::process::exit(0), // Success
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                println!("Key not found");
                std::process::exit(1);
            }
            Err(e) => return Err(e),
        },
    }
    Ok(())
}
