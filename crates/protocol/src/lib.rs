//! Messages between agent and panel (gateway), as JSON over WebSocket.
//!
//! These types are the source of truth. `just protocol` generates TypeScript types from them
//! (`bindings/`), which the backend takes over.
//!
//! Versioning (ADR 0002): incompatible changes increase [`PROTOCOL_VERSION`]. The panel
//! always supports the current and the previous version.

use serde::{Deserialize, Serialize};
use ts_rs::TS;

/// Protocol version this agent speaks.
pub const PROTOCOL_VERSION: u32 = 1;

/// Messages from the agent to the panel.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
#[serde(tag = "type", rename_all = "camelCase")]
#[ts(export)]
pub enum AgentMessage {
    /// First message after connecting.
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

/// Messages from the panel to the agent.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
#[serde(tag = "type", rename_all = "camelCase")]
#[ts(export)]
pub enum PanelMessage {
    /// Handshake accepted. `protocol` is the version spoken from now on.
    Welcome(Welcome),
    /// Handshake rejected; the connection is closed afterwards.
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
    fn hello_has_the_agreed_json_format() {
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
    fn panel_replies_are_parsed() {
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
            json!({"type": "reject", "reason": "too old", "minProtocol": 2, "maxProtocol": 3}),
        )
        .unwrap();
        assert!(matches!(reject, PanelMessage::Reject(r) if r.min_protocol == 2));
    }
}
