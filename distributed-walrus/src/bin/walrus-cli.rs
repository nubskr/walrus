use anyhow::Result;
use clap::{Parser, Subcommand};
use distributed_walrus::cli_client::CliClient;
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::signal;
use tokio::sync::mpsc;
use tracing_subscriber::{fmt, EnvFilter};

/// Lightweight CLI for talking to a distributed-walrus cluster.
#[derive(Parser, Debug)]
#[command(
    author,
    version,
    about,
    subcommand_required = false,
    arg_required_else_help = false
)]
struct Args {
    /// Address of the client listener (e.g. 127.0.0.1:8080).
    #[arg(long, default_value = "127.0.0.1:9091")]
    addr: String,

    #[command(subcommand)]
    command: Option<Command>,
}

#[derive(Subcommand, Debug)]
enum Command {
    /// Interactive shell (default when no subcommand is given).
    Repl,
    /// Register a topic if it does not exist.
    Register { topic: String },
    /// Append a message to a topic.
    Put { topic: String, data: String },
    /// Read a single message from a topic (advances shared cursor).
    Get { topic: String },
    /// Dump topic state as JSON.
    State { topic: String },
    /// Show Raft metrics for the node handling the request.
    Metrics,
}

#[tokio::main]
async fn main() -> Result<()> {
    let _ = fmt::Subscriber::builder()
        .with_env_filter(EnvFilter::from_default_env())
        .with_writer(std::io::stderr)
        .try_init();
    let args = Args::parse();
    let addr = args.addr.clone();
    let client = CliClient::new(addr.clone());
    println!("walrus> connected target: {}", addr);
    match args.command.unwrap_or(Command::Repl) {
        Command::Repl => run_repl(client).await?,
        Command::Register { topic } => client.register(&topic).await?,
        Command::Put { topic, data } => client.put(&topic, &data).await?,
        Command::Get { topic } => match client.get(&topic).await? {
            Some(val) => println!("{}", val),
            None => println!("EMPTY"),
        },
        Command::State { topic } => println!("{}", client.state(&topic).await?),
        Command::Metrics => println!("{}", client.metrics().await?),
    };
    Ok(())
}

async fn run_repl(client: CliClient) -> Result<()> {
    println!("walrus> type commands (REGISTER/PUT/GET/STATE/METRICS). 'exit' or Ctrl+C to quit.");
    let (tx, mut rx) = mpsc::unbounded_channel::<String>();

    // Reader task to handle stdin asynchronously.
    tokio::spawn({
        let tx = tx.clone();
        async move {
            let stdin = tokio::io::stdin();
            let mut lines = BufReader::new(stdin).lines();
            while let Ok(Some(line)) = lines.next_line().await {
                let _ = tx.send(line);
            }
        }
    });

    loop {
        tokio::select! {
            _ = signal::ctrl_c() => break,
            Some(line) = rx.recv() => {
                let trimmed = line.trim();
                if trimmed.is_empty() {
                    continue;
                }
                if matches!(trimmed.to_lowercase().as_str(), "exit" | "quit" | "q") {
                    break;
                }
                match client.send_raw(trimmed).await {
                    Ok(resp) => println!("{resp}"),
                    Err(e) => eprintln!("ERR {e}"),
                }
            }
            else => break,
        }
    }
    Ok(())
}
