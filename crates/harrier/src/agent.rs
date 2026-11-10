use anyhow::Context;
use futures_util::{SinkExt, StreamExt};
use harrier_protocol::{AgentMessage, Hello, PROTOCOL_VERSION, PanelMessage};
use std::time::Duration;
use tokio_tungstenite::{connect_async, tungstenite::Message};
use tracing::{error, info, warn};

const MAX_BACKOFF: Duration = Duration::from_secs(30);

/// Verbindet sich mit dem Panel und hält die Verbindung. Bei Abbruch wird mit wachsender
/// Pause neu verbunden. Eine Ablehnung (z. B. falsche Protokollversion) beendet den Agent,
/// weil ein erneuter Versuch nichts ändern würde.
pub async fn run(panel: &str) -> anyhow::Result<()> {
    let mut backoff = Duration::from_secs(1);
    loop {
        tokio::select! {
            result = session(panel) => match result {
                Ok(Outcome::Rejected(reason)) => {
                    error!("Panel hat den Agent abgelehnt: {reason}");
                    std::process::exit(2);
                }
                Ok(Outcome::Closed) => {
                    warn!("Verbindung zum Panel getrennt");
                    backoff = Duration::from_secs(1);
                }
                Err(e) => warn!("Verbindung fehlgeschlagen: {e:#}"),
            },
            _ = tokio::signal::ctrl_c() => {
                info!("Beende Agent");
                return Ok(());
            }
        }
        info!("Neuer Versuch in {}s", backoff.as_secs());
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
        .with_context(|| format!("Verbinden mit {panel}"))?;

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
                    "Mit Panel verbunden als {} (Protokoll {})",
                    w.agent_id, w.protocol
                )
            }
            PanelMessage::Reject(r) => {
                return Ok(Outcome::Rejected(format!(
                    "{} (Panel unterstützt Protokoll {}–{}, Agent spricht {PROTOCOL_VERSION})",
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
        .unwrap_or_else(|| "unbekannt".into())
}
