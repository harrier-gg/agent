use anyhow::Context;
use futures_util::{SinkExt, StreamExt};
use harrier_protocol::{AgentMessage, Hello, PROTOCOL_VERSION, PanelMessage};
use std::time::Duration;
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::http::{HeaderValue, StatusCode, header::AUTHORIZATION};
use tokio_tungstenite::tungstenite::{Error as WsError, Message};
use tracing::{error, info, warn};

const MAX_BACKOFF: Duration = Duration::from_secs(30);
/// Close code the panel uses when another connection with the same token took over (ADR 0004).
const CLOSE_REPLACED: u16 = 4002;

/// Connects to the panel and keeps the connection. When it drops, the agent reconnects with a
/// growing pause. A rejection (invalid token, wrong protocol version, replaced by another agent)
/// stops the agent, because retrying would not change anything.
///
/// The token authenticates the node (ADR 0004). It is sent in the `Authorization` header of the
/// WebSocket upgrade and never logged.
pub async fn run(panel: &str, token: &str) -> anyhow::Result<()> {
    let mut backoff = Duration::from_secs(1);
    loop {
        tokio::select! {
            result = session(panel, token) => match result {
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

async fn session(panel: &str, token: &str) -> anyhow::Result<Outcome> {
    let mut request = panel
        .into_client_request()
        .with_context(|| format!("Invalid panel URL {panel}"))?;
    let mut bearer = HeaderValue::from_str(&format!("Bearer {token}"))?;
    bearer.set_sensitive(true);
    request.headers_mut().insert(AUTHORIZATION, bearer);

    let mut ws = match connect_async(request).await {
        Ok((ws, _)) => ws,
        Err(WsError::Http(response)) if response.status() == StatusCode::UNAUTHORIZED => {
            return Ok(Outcome::Rejected(
                "the agent token is invalid or the node was removed. Register the node again and set HARRIER_AGENT_TOKEN".into(),
            ));
        }
        Err(e) => return Err(e).with_context(|| format!("Connecting to {panel}")),
    };

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
        let text = match msg? {
            Message::Text(text) => text,
            Message::Close(Some(frame)) if u16::from(frame.code) == CLOSE_REPLACED => {
                return Ok(Outcome::Rejected(
                    "another agent connected with the same token. Each node needs its own token"
                        .into(),
                ));
            }
            _ => continue,
        };
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
