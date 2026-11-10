//! Nachrichten zwischen Agent und Panel (Gateway), als JSON über WebSocket.
//!
//! Diese Typen sind die Quelle der Wahrheit. `just protocol` erzeugt daraus TypeScript-Typen
//! (`bindings/`), die das Backend übernimmt.
//!
//! Versionierung (ADR 0002): Inkompatible Änderungen erhöhen [`PROTOCOL_VERSION`]. Das Panel
//! unterstützt immer die aktuelle und die vorherige Version.

use serde::{Deserialize, Serialize};
use ts_rs::TS;

/// Protokollversion, die dieser Agent spricht.
pub const PROTOCOL_VERSION: u32 = 1;

/// Nachrichten vom Agent an das Panel.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
#[serde(tag = "type", rename_all = "camelCase")]
#[ts(export)]
pub enum AgentMessage {
    /// Erste Nachricht nach dem Verbinden.
    Hello(Hello),
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
#[serde(rename_all = "camelCase")]
#[ts(export)]
pub struct Hello {
    pub protocol: u32,
    pub agent_version: String,
    pub os: String,
    pub arch: String,
    pub hostname: String,
}

/// Nachrichten vom Panel an den Agent.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
#[serde(tag = "type", rename_all = "camelCase")]
#[ts(export)]
pub enum PanelMessage {
    /// Handshake angenommen. `protocol` ist die Version, die ab jetzt gesprochen wird.
    Welcome(Welcome),
    /// Handshake abgelehnt, die Verbindung wird danach geschlossen.
    Reject(Reject),
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
#[serde(rename_all = "camelCase")]
#[ts(export)]
pub struct Welcome {
    pub protocol: u32,
    pub agent_id: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
#[serde(rename_all = "camelCase")]
#[ts(export)]
pub struct Reject {
    pub reason: String,
    pub min_protocol: u32,
    pub max_protocol: u32,
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn hello_hat_das_vereinbarte_json_format() {
        let msg = AgentMessage::Hello(Hello {
            protocol: 1,
            agent_version: "0.1.0".into(),
            os: "linux".into(),
            arch: "x86_64".into(),
            hostname: "box".into(),
        });
        assert_eq!(
            serde_json::to_value(&msg).unwrap(),
            json!({"type": "hello", "protocol": 1, "agentVersion": "0.1.0", "os": "linux", "arch": "x86_64", "hostname": "box"})
        );
    }

    #[test]
    fn antworten_des_panels_werden_gelesen() {
        let welcome: PanelMessage =
            serde_json::from_value(json!({"type": "welcome", "protocol": 1, "agentId": "a1"}))
                .unwrap();
        assert_eq!(
            welcome,
            PanelMessage::Welcome(Welcome {
                protocol: 1,
                agent_id: "a1".into()
            })
        );

        let reject: PanelMessage = serde_json::from_value(
            json!({"type": "reject", "reason": "zu alt", "minProtocol": 2, "maxProtocol": 3}),
        )
        .unwrap();
        assert!(matches!(reject, PanelMessage::Reject(r) if r.min_protocol == 2));
    }
}
