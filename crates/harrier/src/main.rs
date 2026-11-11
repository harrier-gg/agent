//! `harrier`: agent and CLI in one binary.
//! `harrier agent run` connects to the panel and authenticates with the node's agent token.

mod agent;
mod query;
mod runtime;

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
    /// Start the agent and connect it to the panel.
    ///
    /// The agent token is read from HARRIER_AGENT_TOKEN (not from an argument, because arguments
    /// are visible to other users via `ps`).
    Run {
        /// WebSocket URL of the panel
        #[arg(
            long,
            env = "HARRIER_PANEL_URL",
            default_value = "ws://localhost:8080/agent/ws"
        )]
        panel: String,

        /// Allow sending the token over unencrypted ws:// to another machine, e.g. a panel in
        /// the home network without HTTPS. Anyone on that network can read the token.
        #[arg(long, env = "HARRIER_AGENT_INSECURE")]
        insecure: bool,

        /// Directory for the servers' data (worlds, configs, mods); one subdirectory per server.
        /// Default: ~/.local/share/harrier/servers
        #[arg(long, env = "HARRIER_DATA_DIR")]
        data_dir: Option<std::path::PathBuf>,
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
            command:
                AgentCommand::Run {
                    panel,
                    insecure,
                    data_dir,
                },
        } => {
            let token = agent_token()?;
            check_panel_url(&panel, insecure)?;
            let data_dir = match data_dir {
                Some(dir) => dir,
                None => std::path::PathBuf::from(std::env::var("HOME")?)
                    .join(".local/share/harrier/servers"),
            };
            agent::run(&panel, &token, data_dir).await
        }
    }
}

/// Reads the agent token from the environment (ADR 0004).
fn agent_token() -> anyhow::Result<String> {
    let token = std::env::var("HARRIER_AGENT_TOKEN").unwrap_or_default();
    if !token.starts_with("hrr_") {
        anyhow::bail!(
            "HARRIER_AGENT_TOKEN is missing or not an agent token (it starts with \"hrr_\"). \
             Register the node in the panel to get one."
        );
    }
    Ok(token)
}

/// The token must not travel unencrypted over a network: `wss://` is required, except for the
/// own machine or with an explicit `--insecure`.
fn check_panel_url(panel: &str, insecure: bool) -> anyhow::Result<()> {
    let uri: tokio_tungstenite::tungstenite::http::Uri = panel
        .parse()
        .map_err(|_| anyhow::anyhow!("Invalid panel URL: {panel}"))?;
    match uri.scheme_str() {
        Some("wss") => Ok(()),
        Some("ws") if is_loopback(uri.host().unwrap_or_default()) => Ok(()),
        Some("ws") if insecure => {
            tracing::warn!(
                "Sending the agent token unencrypted to {panel} (--insecure). Anyone on this network can read it."
            );
            Ok(())
        }
        Some("ws") => anyhow::bail!(
            "Refusing to send the agent token unencrypted to {panel}. Use wss:// (HTTPS on the panel), \
             or pass --insecure if the panel is in a trusted home network without HTTPS."
        ),
        _ => anyhow::bail!("The panel URL must start with wss:// or ws:// ({panel})"),
    }
}

fn is_loopback(host: &str) -> bool {
    let host = host.trim_start_matches('[').trim_end_matches(']');
    host.eq_ignore_ascii_case("localhost")
        || host
            .parse::<std::net::IpAddr>()
            .is_ok_and(|ip| ip.is_loopback())
}

#[cfg(test)]
mod tests {
    use super::check_panel_url;

    #[test]
    fn wss_is_always_allowed() {
        assert!(check_panel_url("wss://panel.example.com/agent/ws", false).is_ok());
    }

    #[test]
    fn ws_is_allowed_for_the_own_machine() {
        for url in [
            "ws://localhost:8080/agent/ws",
            "ws://127.0.0.1:8080/agent/ws",
            "ws://[::1]:8080/agent/ws",
        ] {
            assert!(check_panel_url(url, false).is_ok(), "{url}");
        }
    }

    #[test]
    fn ws_to_another_machine_needs_insecure() {
        let url = "ws://192.168.1.10:8080/agent/ws";
        assert!(check_panel_url(url, false).is_err());
        assert!(check_panel_url(url, true).is_ok());
        assert!(check_panel_url("ws://localhost.evil.example/agent/ws", false).is_err());
    }

    #[test]
    fn other_schemes_are_rejected() {
        assert!(check_panel_url("http://localhost:8080/agent/ws", true).is_err());
    }
}
