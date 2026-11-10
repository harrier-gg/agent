//! `harrier`: agent and CLI in one binary.
//! M0: `harrier agent run` connects to the panel and performs the handshake.

mod agent;

use clap::{Parser, Subcommand};
use tracing_subscriber::EnvFilter;

#[derive(Parser)]
#[command(name = "harrier", version, about = "Harrier game server manager")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Agent commands
    Agent {
        #[command(subcommand)]
        command: AgentCommand,
    },
}

#[derive(Subcommand)]
enum AgentCommand {
    /// Start the agent and connect it to the panel
    Run {
        /// WebSocket URL of the panel
        #[arg(
            long,
            env = "HARRIER_PANEL_URL",
            default_value = "ws://localhost:8080/agent/ws"
        )]
        panel: String,
    },
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();

    match Cli::parse().command {
        Command::Agent {
            command: AgentCommand::Run { panel },
        } => agent::run(&panel).await,
    }
}
