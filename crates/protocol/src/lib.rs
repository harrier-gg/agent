//! Messages between agent and panel (gateway), as JSON over WebSocket.
//!
//! These types are the source of truth. `just protocol` generates TypeScript types from them
//! (`bindings/`), which the backend takes over.
//!
//! Versioning (ADR 0002): incompatible changes increase [`PROTOCOL_VERSION`]. The panel
//! always supports the current and the previous version.

use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
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
    /// A server changed its status (also sent for every known server after connecting).
    ServerState(ServerState),
    /// Raw console output of a server (may contain ANSI escape codes).
    ConsoleOutput(ConsoleOutput),
    /// Resource usage of a running server, sent every few seconds.
    ServerStats(ServerStats),
    /// Who is online, for servers whose spec has a `query` (ADR 0009). Sent every few seconds.
    ServerPlayers(ServerPlayers),
    /// Reply to `readFile` or `writeFile` (ADR 0011).
    FileResult(FileResult),
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
    /// Create (or recreate) and start a server from a runtime spec.
    StartServer(StartServer),
    /// Stop a server gracefully (falls back to killing it after `stopTimeoutSecs`).
    StopServer(ServerRef),
    /// Write one line to a server's console.
    SendCommand(SendCommand),
    /// Read a text file in a server's data directory (ADR 0011). Answered with `fileResult`.
    ReadFile(ReadFile),
    /// Replace a text file in a server's data directory (ADR 0011). Answered with `fileResult`.
    WriteFile(WriteFile),
    /// List a directory; `path` "" is the data directory itself (ADR 0015). Answered with
    /// `fileResult` and `entries`.
    ListDir(PathRequest),
    /// Delete a file, or a directory with everything in it (ADR 0015).
    DeletePath(PathRequest),
    /// Create a directory and missing parents (ADR 0015).
    MakeDir(PathRequest),
    /// Rename or move inside the data directory; fails if the target exists (ADR 0015).
    MovePath(MovePath),
    /// Stream a file to the panel over the transfer channel `/agent/transfer/<transferId>`.
    SendFile(Transfer),
    /// Receive a file from the panel over the transfer channel and put it at `path`.
    ReceiveFile(Transfer),
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
#[serde(rename_all = "camelCase")]
#[ts(export)]
pub struct PathRequest {
    pub request_id: String,
    pub server_id: String,
    pub path: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
#[serde(rename_all = "camelCase")]
#[ts(export)]
pub struct MovePath {
    pub request_id: String,
    pub server_id: String,
    pub from: String,
    pub to: String,
}

/// A file transfer (ADR 0015). The agent opens `<panel>/agent/transfer/<transferId>` with its
/// token and streams binary frames; it closes normally when done, or with code 1011 and a reason.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
#[serde(rename_all = "camelCase")]
#[ts(export)]
pub struct Transfer {
    pub transfer_id: String,
    pub server_id: String,
    pub path: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
#[serde(rename_all = "camelCase")]
#[ts(export)]
pub struct FileEntry {
    pub name: String,
    pub kind: EntryKind,
    #[ts(type = "number")]
    pub size: u64,
    /// Last change, seconds since 1970.
    #[ts(type = "number | null")]
    pub modified: Option<i64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(rename_all = "lowercase")]
#[ts(export)]
pub enum EntryKind {
    File,
    Dir,
    /// Symlinks and special files; shown, but not opened.
    Other,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
#[serde(rename_all = "camelCase")]
#[ts(export)]
pub struct ReadFile {
    pub request_id: String,
    pub server_id: String,
    /// Relative to the server's data directory, e.g. `server.properties`.
    pub path: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
#[serde(rename_all = "camelCase")]
#[ts(export)]
pub struct WriteFile {
    pub request_id: String,
    pub server_id: String,
    pub path: String,
    pub content: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
#[serde(rename_all = "camelCase")]
#[ts(export)]
pub struct FileResult {
    pub request_id: String,
    /// The file's text for `readFile`; `null` if it does not exist (yet) or after `writeFile`.
    #[serde(default)]
    pub content: Option<String>,
    /// Why the request failed (English); `null` on success.
    #[serde(default)]
    pub error: Option<String>,
    /// The directory's entries for `listDir`, directories first.
    #[serde(default)]
    pub entries: Option<Vec<FileEntry>>,
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

/// Everything the agent needs to run a server. Game-neutral (ADR 0005): the panel turns a
/// template and the user's settings into this spec; the agent knows no games.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
#[serde(rename_all = "camelCase")]
#[ts(export)]
pub struct RuntimeSpec {
    pub image: String,
    #[ts(type = "Record<string, string>")]
    pub env: BTreeMap<String, String>,
    pub ports: Vec<PortMapping>,
    pub memory_mb: u32,
    /// Regular expression on the console output that marks the server as running.
    /// Without it, the server counts as running as soon as the process started.
    #[serde(default)]
    pub ready_pattern: Option<String>,
    pub stop_timeout_secs: u32,
    /// How to ask the running server who is online. Without it, there is no player list.
    #[serde(default)]
    pub query: Option<QuerySpec>,
    /// Put the server to sleep when nobody is online (ADR 0013). Needs `query`.
    #[serde(default)]
    pub sleep: Option<SleepSpec>,
}

/// When to sleep and how to wake (ADR 0013).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
#[serde(rename_all = "camelCase")]
#[ts(export)]
pub struct SleepSpec {
    /// Minutes with nobody online before the server is stopped (at least 1).
    pub idle_minutes: u32,
    /// How players wake it by connecting. Without it, only the panel wakes it.
    #[serde(default)]
    pub wake: Option<WakeSpec>,
}

/// While asleep, the agent answers on the game port itself.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
#[serde(rename_all = "camelCase")]
#[ts(export)]
pub struct WakeSpec {
    pub protocol: WakeProtocol,
    /// Host port players connect to.
    pub port: u16,
    /// Shown in the game's server list while asleep.
    pub status_text: String,
    /// Shown to a player whose connection wakes the server.
    pub join_text: String,
}

/// Network protocols the agent can answer while a server sleeps, not games.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(rename_all = "lowercase")]
#[ts(export)]
pub enum WakeProtocol {
    /// Minecraft: Java Edition (server list ping and login).
    Minecraft,
    /// A protocol of a newer panel; this agent only wakes from the panel.
    #[serde(other)]
    #[ts(skip)]
    Unsupported,
}

/// A query protocol and the host port it answers on (ADR 0009).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
#[serde(rename_all = "camelCase")]
#[ts(export)]
pub struct QuerySpec {
    pub protocol: QueryProtocol,
    pub port: u16,
}

/// Network protocols for asking game servers, not games.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(rename_all = "lowercase")]
#[ts(export)]
pub enum QueryProtocol {
    /// Minecraft: Java Edition Server List Ping.
    Minecraft,
    /// A protocol of a newer panel; this agent does not ask.
    #[serde(other)]
    #[ts(skip)]
    Unsupported,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
#[serde(rename_all = "camelCase")]
#[ts(export)]
pub struct PortMapping {
    pub container_port: u16,
    pub host_port: u16,
    pub protocol: PortProtocol,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(rename_all = "lowercase")]
#[ts(export)]
pub enum PortProtocol {
    Tcp,
    Udp,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
#[serde(rename_all = "camelCase")]
#[ts(export)]
pub struct StartServer {
    pub server_id: String,
    pub spec: RuntimeSpec,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
#[serde(rename_all = "camelCase")]
#[ts(export)]
pub struct ServerRef {
    pub server_id: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
#[serde(rename_all = "camelCase")]
#[ts(export)]
pub struct SendCommand {
    pub server_id: String,
    pub command: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(rename_all = "camelCase")]
#[ts(export)]
pub enum ServerStatus {
    /// Downloading the image or files.
    Installing,
    Starting,
    Running,
    Stopping,
    Stopped,
    /// The process exited without being asked to stop.
    Crashed,
    /// Stopped because nobody was online; wakes on connect or from the panel (ADR 0013).
    Sleeping,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
#[serde(rename_all = "camelCase")]
#[ts(export)]
pub struct ServerState {
    pub server_id: String,
    pub status: ServerStatus,
    /// Human-readable detail, e.g. an error or download progress (English).
    #[serde(default)]
    pub message: Option<String>,
    /// Set while the agent waits to restart a crashed server (ADR 0010).
    #[serde(default)]
    pub restart: Option<Restart>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
#[serde(rename_all = "camelCase")]
#[ts(export)]
pub struct Restart {
    /// 1 for the first restart after a crash.
    pub attempt: u32,
    pub max_attempts: u32,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
#[serde(rename_all = "camelCase")]
#[ts(export)]
pub struct ConsoleOutput {
    pub server_id: String,
    pub data: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
#[serde(rename_all = "camelCase")]
#[ts(export)]
pub struct ServerStats {
    pub server_id: String,
    pub cpu_percent: f64,
    #[ts(type = "number")]
    pub memory_bytes: u64,
    #[ts(type = "number")]
    pub memory_limit_bytes: u64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
#[serde(rename_all = "camelCase")]
#[ts(export)]
pub struct ServerPlayers {
    pub server_id: String,
    pub online: u32,
    pub max: u32,
    /// Names of online players; may be fewer than `online` (Minecraft sends at most 12).
    pub names: Vec<String>,
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

    #[test]
    fn start_server_has_the_agreed_json_format() {
        let msg: PanelMessage = serde_json::from_value(json!({
            "type": "startServer",
            "serverId": "s1",
            "spec": {
                "image": "itzg/minecraft-server:java21",
                "env": {"TYPE": "PAPER", "EULA": "TRUE"},
                "ports": [{"containerPort": 25565, "hostPort": 25566, "protocol": "tcp"}],
                "memoryMb": 2048,
                "readyPattern": "Done \\(",
                "stopTimeoutSecs": 30
            }
        }))
        .unwrap();
        let PanelMessage::StartServer(start) = msg else {
            panic!("expected startServer")
        };
        assert_eq!(start.spec.ports[0].protocol, PortProtocol::Tcp);
        assert_eq!(start.spec.env["TYPE"], "PAPER");
        assert_eq!(start.spec.query, None);
    }

    #[test]
    fn unknown_query_protocols_do_not_break_starting() {
        let spec: QuerySpec =
            serde_json::from_value(json!({"protocol": "a2s", "port": 27015})).unwrap();
        assert_eq!(spec.protocol, QueryProtocol::Unsupported);
    }

    #[test]
    fn server_state_omits_nothing_and_message_is_optional() {
        let state: AgentMessage = serde_json::from_value(
            json!({"type": "serverState", "serverId": "s1", "status": "running"}),
        )
        .unwrap();
        assert_eq!(
            serde_json::to_value(&state).unwrap(),
            json!({"type": "serverState", "serverId": "s1", "status": "running", "message": null, "restart": null})
        );
    }
}
