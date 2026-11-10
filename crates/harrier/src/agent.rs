use anyhow::Context;
use futures_util::{SinkExt, StreamExt};
use harrier_protocol::{AgentMessage, Hello, PROTOCOL_VERSION, PanelMessage};
use std::time::Duration;
use tokio_tungstenite::{connect_async, tungstenite::Message};
use tracing::{error, info, warn};

const MAX_BACKOFF: Duration = Duration::from_secs(30);

/// Connects to the panel and keeps the connection. When it drops, the agent reconnects with a
/// growing pause. A rejection (e.g. wrong protocol version) stops the agent, because retrying
/// would not change anything.
pub async fn run(panel: &str) -> anyhow::Result<()> {
    let mut backoff = Duration::from_secs(1);
    loop {
        tokio::select! {
            result = session(panel) => match result {
                Ok(Outcome::Rejected(reason)) => {
                    error!("The panel rejected this agent: {reason}");
                    std::process::exit(2);
                }
                Ok(Outcome::Closed) => {
                    warn!("Connection to the panel closed");
                    backoff = Duration::from_secs(1);
                }
                Err(e) => warn!("Connection failed: {e:#}"),
            },
            _ = tokio::signal::ctrl_c() => {
                info!("Stopping agent");
                return Ok(());
            }
        }
        info!("Retrying in {}s", backoff.as_secs());
        tokio::time::sleep(backoff).await;
        backoff = (backoff * 2).min(MAX_BACKOFF);
    }
}

enum Outcome {
    Closed,
    Rejected(String),
}

async fn session(panel: &str) -> anyhow::Result<Outcome> {
    let (mut ws, _) = connect_async(panel)
        .await
        .with_context(|| format!("Connecting to {panel}"))?;

    let hello = AgentMessage::Hello(Hello {
        protocol: PROTOCOL_VERSION,
        agent_version: env!("CARGO_PKG_VERSION").into(),
        os: std::env::consts::OS.into(),
        arch: std::env::consts::ARCH.into(),
        hostname: hostname(),
    });
    ws.send(Message::text(serde_json::to_string(&hello)?))
        .await?;

    while let Some(msg) = ws.next().await {
        let Message::Text(text) = msg? else { continue };
        match serde_json::from_str::<PanelMessage>(&text)? {
            PanelMessage::Welcome(w) => {
                info!(
                    "Connected to the panel as {} (protocol {})",
                    w.agent_id, w.protocol
                )
            }
            PanelMessage::Reject(r) => {
                return Ok(Outcome::Rejected(format!(
                    "{} (panel supports protocol {}–{}, agent speaks {PROTOCOL_VERSION})",
                    r.reason, r.min_protocol, r.max_protocol
                )));
            }
        }
    }
    Ok(Outcome::Closed)
}

fn hostname() -> String {
    std::env::var("HOSTNAME")
        .ok()
        .or_else(|| {
            std::process::Command::new("hostname")
                .output()
                .ok()
                .map(|o| String::from_utf8_lossy(&o.stdout).trim().to_string())
        })
        .filter(|h| !h.is_empty())
        .unwrap_or_else(|| "unknown".into())
}
