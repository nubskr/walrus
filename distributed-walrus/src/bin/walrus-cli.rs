use anyhow::Result;
use clap::{Parser, Subcommand};
use distributed_walrus::cli_client::CliClient;
use rustyline::error::ReadlineError;
use rustyline::history::DefaultHistory;
use rustyline::Editor;
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

const PROMPT: &str = "🦭 > ";
const RESET: &str = "\x1b[0m";
const BANNER_COLOR: &str = "\x1b[38;5;80m"; // teal

#[tokio::main]
async fn main() -> Result<()> {
    let _ = fmt::Subscriber::builder()
        .with_env_filter(EnvFilter::from_default_env())
        .with_writer(std::io::stderr)
        .try_init();
    let args = Args::parse();
    let addr = args.addr.clone();
    let client = CliClient::new(addr.clone());
    println!("→ connected target: {}", addr);
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
    print_banner();
    println!("type commands (REGISTER/PUT/GET/STATE/METRICS). 'exit' or Ctrl+C to quit.");

    let mut editor = Editor::<(), DefaultHistory>::new()?;

    loop {
        match editor.readline(PROMPT) {
            Ok(line) => {
                let trimmed = line.trim();
                if trimmed.is_empty() {
                    continue;
                }
                if matches!(trimmed.to_lowercase().as_str(), "exit" | "quit" | "q") {
                    break;
                }

                if editor.add_history_entry(trimmed).is_err() {
                    eprintln!("ERR failed to store command in history");
                }

                match client.send_raw(trimmed).await {
                    Ok(resp) => println!("{resp}"),
                    Err(e) => eprintln!("ERR {e}"),
                }
            }
            Err(ReadlineError::Interrupted | ReadlineError::Eof) => {
                println!();
                break;
            }
            Err(e) => {
                eprintln!("ERR failed to read input: {e}");
                break;
            }
        }
    }
    Ok(())
}

fn print_banner() {
    for line in WALRUS_ASCII.lines() {
        println!("{BANNER_COLOR}{line}{RESET}");
    }
    println!();
}

const WALRUS_ASCII: &str = r#"
██╗    ██╗ █████╗ ██╗     ██████╗ ██╗   ██╗███████╗
██║    ██║██╔══██╗██║     ██╔══██╗██║   ██║██╔════╝
██║ █╗ ██║███████║██║     ██████╔╝██║   ██║█████╗  
██║███╗██║██╔══██║██║     ██╔══██╗██║   ██║╚════██║
╚███╔███╔╝██║  ██║███████╗██║  ██║╚██████╔╝███████║
 ╚══╝╚══╝ ╚═╝  ╚═╝╚══════╝╚═╝  ╚═╝ ╚═════╝ ╚══════╝
                                                    
"#;
