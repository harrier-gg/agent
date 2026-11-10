//! `harrier`: Agent und CLI in einer Binary.
//! M0: `harrier agent run` verbindet sich mit dem Panel und macht den Handshake.

mod agent;

use clap::{Parser, Subcommand};
use tracing_subscriber::EnvFilter;

#[derive(Parser)]
#[command(name = "harrier", version, about = "Harrier Game-Server-Manager")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Agent-Befehle
    Agent {
        #[command(subcommand)]
        command: AgentCommand,
    },
}

#[derive(Subcommand)]
enum AgentCommand {
    /// Agent starten und mit dem Panel verbinden
    Run {
        /// WebSocket-URL des Panels
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
