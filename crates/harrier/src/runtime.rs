//! Runs game servers as Docker containers from game-neutral runtime specs (ADR 0005).
//! The agent knows no games: image, env, ports and limits all come from the panel.

use crate::query;
use anyhow::Context;
use bollard::Docker;
use bollard::models::{ContainerCreateBody, HostConfig, PortBinding};
use bollard::query_parameters::{
    AttachContainerOptions, CreateContainerOptions, CreateImageOptions, ListContainersOptions,
    RemoveContainerOptions, StatsOptions, StopContainerOptions, WaitContainerOptions,
};
use futures_util::StreamExt;
use harrier_protocol::{
    AgentMessage, ConsoleOutput, PortProtocol, QueryProtocol, QuerySpec, RuntimeSpec,
    ServerPlayers, ServerState, ServerStats, ServerStatus,
};
use regex::Regex;
use std::collections::HashMap;
// ponytail: Unix only (container user = owner of the data directory); Windows agents come with
// `runtime: native` (ADR 0005)
use std::os::unix::fs::MetadataExt;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use tokio::io::{AsyncWrite, AsyncWriteExt};
use tokio::sync::mpsc;
use tracing::{info, warn};

const LABEL: &str = "gg.harrier.server";
/// The spec's `query`, kept on the container so a restarted agent keeps asking (ADR 0009).
const QUERY_LABEL: &str = "gg.harrier.query";
const PLAYERS_INTERVAL: std::time::Duration = std::time::Duration::from_secs(10);
const DEFAULT_STOP_TIMEOUT_SECS: i32 = 30;

type Stdin = Pin<Box<dyn AsyncWrite + Send>>;

struct Running {
    stdin: tokio::sync::Mutex<Stdin>,
    stopping: AtomicBool,
    stop_timeout_secs: i32,
}

/// Owns the Docker connection and all servers of this node. Outlives panel connections:
/// containers keep running while the agent reconnects.
pub struct Runtime {
    docker: Docker,
    data_dir: PathBuf,
    events: mpsc::Sender<AgentMessage>,
    servers: Mutex<HashMap<String, Arc<Running>>>,
}

fn container_name(server_id: &str) -> String {
    format!("harrier-{server_id}")
}

impl Runtime {
    pub fn new(data_dir: PathBuf, events: mpsc::Sender<AgentMessage>) -> anyhow::Result<Arc<Self>> {
        let docker = Docker::connect_with_defaults().context("Connecting to Docker")?;
        Ok(Arc::new(Self {
            docker,
            data_dir,
            events,
            servers: Mutex::new(HashMap::new()),
        }))
    }

    /// Sends an event to the panel. While disconnected the channel fills up and events are
    /// dropped; after reconnecting, `report_all` sends the current state again.
    fn emit(&self, message: AgentMessage) {
        let _ = self.events.try_send(message);
    }

    fn state(&self, server_id: &str, status: ServerStatus, message: Option<String>) {
        self.emit(AgentMessage::ServerState(ServerState {
            server_id: server_id.into(),
            status,
            message,
        }));
    }

    /// Reports every Harrier container and re-attaches to running ones the agent lost track of
    /// (e.g. after an agent restart).
    pub async fn report_all(self: &Arc<Self>) -> anyhow::Result<()> {
        let filters = HashMap::from([("label".to_string(), vec![LABEL.to_string()])]);
        let containers = self
            .docker
            .list_containers(Some(ListContainersOptions {
                all: true,
                filters: Some(filters),
                ..Default::default()
            }))
            .await?;

        for container in containers {
            let Some(server_id) = container
                .labels
                .as_ref()
                .and_then(|l| l.get(LABEL))
                .cloned()
            else {
                continue;
            };
            let running = matches!(
                container.state,
                Some(bollard::models::ContainerSummaryStateEnum::RUNNING)
            );
            if !running {
                self.state(&server_id, ServerStatus::Stopped, None);
                continue;
            }
            let query = container
                .labels
                .as_ref()
                .and_then(|l| l.get(QUERY_LABEL))
                .and_then(|q| serde_json::from_str::<QuerySpec>(q).ok());
            let known = self.servers.lock().unwrap().contains_key(&server_id);
            if known {
                self.state(&server_id, ServerStatus::Running, None);
            } else {
                let this = self.clone();
                tokio::spawn(async move {
                    if let Err(e) = this
                        .supervise(&server_id, None, DEFAULT_STOP_TIMEOUT_SECS, query)
                        .await
                    {
                        warn!("Re-attaching to {server_id} failed: {e:#}");
                    }
                });
            }
        }
        Ok(())
    }

    /// Pulls the image, (re)creates the container and starts it. Errors end up as `crashed`
    /// with a message, so the panel always learns what happened.
    pub async fn start(self: Arc<Self>, server_id: String, spec: RuntimeSpec) {
        if let Err(e) = self.clone().try_start(&server_id, spec).await {
            warn!("Starting {server_id} failed: {e:#}");
            self.state(&server_id, ServerStatus::Crashed, Some(format!("{e:#}")));
        }
    }

    async fn try_start(self: Arc<Self>, server_id: &str, spec: RuntimeSpec) -> anyhow::Result<()> {
        if self.servers.lock().unwrap().contains_key(server_id) {
            anyhow::bail!("The server is already running");
        }
        let name = container_name(server_id);

        self.state(
            server_id,
            ServerStatus::Installing,
            Some(format!("Pulling {}", spec.image)),
        );
        let mut pull = self.docker.create_image(
            Some(CreateImageOptions {
                from_image: Some(spec.image.clone()),
                ..Default::default()
            }),
            None,
            None,
        );
        while let Some(progress) = pull.next().await {
            progress.with_context(|| format!("Pulling {}", spec.image))?;
        }

        // Recreate the container so it always matches the current spec; data lives outside it.
        let _ = self
            .docker
            .remove_container(
                &name,
                Some(RemoveContainerOptions {
                    force: true,
                    ..Default::default()
                }),
            )
            .await;

        let data = self.data_dir.join(server_id);
        std::fs::create_dir_all(&data).with_context(|| format!("Creating {}", data.display()))?;
        let owner = std::fs::metadata(&data)?;

        let mut port_bindings = HashMap::new();
        let mut exposed = Vec::new();
        for port in &spec.ports {
            let protocol = match port.protocol {
                PortProtocol::Tcp => "tcp",
                PortProtocol::Udp => "udp",
            };
            let key = format!("{}/{protocol}", port.container_port);
            exposed.push(key.clone());
            port_bindings.insert(
                key,
                Some(vec![PortBinding {
                    host_ip: None,
                    host_port: Some(port.host_port.to_string()),
                }]),
            );
        }

        let mut labels = HashMap::from([(LABEL.to_string(), server_id.to_string())]);
        if let Some(query) = &spec.query {
            labels.insert(QUERY_LABEL.to_string(), serde_json::to_string(query)?);
        }
        let body = ContainerCreateBody {
            image: Some(spec.image.clone()),
            env: Some(spec.env.iter().map(|(k, v)| format!("{k}={v}")).collect()),
            exposed_ports: Some(exposed),
            labels: Some(labels),
            // The server runs as the owner of its data directory, never as root.
            user: Some(format!("{}:{}", owner.uid(), owner.gid())),
            tty: Some(true),
            open_stdin: Some(true),
            attach_stdin: Some(true),
            host_config: Some(HostConfig {
                binds: Some(vec![format!("{}:/data", data.display())]),
                port_bindings: Some(port_bindings),
                memory: Some(i64::from(spec.memory_mb) * 1024 * 1024),
                pids_limit: Some(1024),
                cap_drop: Some(vec!["ALL".into()]),
                security_opt: Some(vec!["no-new-privileges".into()]),
                init: Some(true),
                ..Default::default()
            }),
            ..Default::default()
        };
        self.docker
            .create_container(
                Some(CreateContainerOptions {
                    name: Some(name.clone()),
                    ..Default::default()
                }),
                body,
            )
            .await
            .context("Creating the container")?;

        self.state(server_id, ServerStatus::Starting, None);
        self.docker
            .start_container(&name, None)
            .await
            .context("Starting the container")?;

        let ready = spec.ready_pattern.as_deref().map(Regex::new).transpose()?;
        let timeout = i32::try_from(spec.stop_timeout_secs).unwrap_or(DEFAULT_STOP_TIMEOUT_SECS);
        let this = self.clone();
        let id = server_id.to_string();
        tokio::spawn(async move {
            if let Err(e) = this.supervise(&id, ready, timeout, spec.query).await {
                warn!("Supervising {id} failed: {e:#}");
            }
        });
        Ok(())
    }

    /// Streams console output and stats until the container exits, then reports the result.
    async fn supervise(
        self: Arc<Self>,
        server_id: &str,
        ready: Option<Regex>,
        stop_timeout_secs: i32,
        query: Option<QuerySpec>,
    ) -> anyhow::Result<()> {
        let name = container_name(server_id);
        let attach = self
            .docker
            .attach_container(
                &name,
                Some(AttachContainerOptions {
                    stdin: true,
                    stdout: true,
                    stderr: true,
                    stream: true,
                    ..Default::default()
                }),
            )
            .await?;

        let running = Arc::new(Running {
            stdin: tokio::sync::Mutex::new(attach.input),
            stopping: AtomicBool::new(false),
            stop_timeout_secs,
        });
        self.servers
            .lock()
            .unwrap()
            .insert(server_id.to_string(), running.clone());
        if ready.is_none() {
            self.state(server_id, ServerStatus::Running, None);
        }

        let stats = tokio::spawn(self.clone().stream_stats(server_id.to_string()));
        let players =
            query.map(|q| tokio::spawn(self.clone().poll_players(server_id.to_string(), q)));

        let mut output = attach.output;
        let mut ready = ready;
        let mut tail = String::new(); // last partial line, so the ready pattern also matches across chunks
        while let Some(chunk) = output.next().await {
            let Ok(chunk) = chunk else { break };
            let data = String::from_utf8_lossy(&chunk.into_bytes()).into_owned();
            if let Some(pattern) = &ready {
                tail.push_str(&data);
                if pattern.is_match(&tail) {
                    self.state(server_id, ServerStatus::Running, None);
                    ready = None;
                }
                if let Some(pos) = tail.rfind('\n') {
                    tail.drain(..=pos);
                }
            }
            self.emit(AgentMessage::ConsoleOutput(ConsoleOutput {
                server_id: server_id.to_string(),
                data,
            }));
        }

        let exit = self
            .docker
            .wait_container(&name, None::<WaitContainerOptions>)
            .next()
            .await;
        stats.abort();
        if let Some(players) = players {
            players.abort();
        }
        self.servers.lock().unwrap().remove(server_id);

        let code = match exit {
            Some(Ok(r)) => r.status_code,
            // bollard reports a non-zero exit code as an error
            Some(Err(bollard::errors::Error::DockerContainerWaitError { code, .. })) => code,
            _ => -1,
        };
        if running.stopping.load(Ordering::SeqCst) || code == 0 {
            info!("Server {server_id} stopped");
            self.state(server_id, ServerStatus::Stopped, None);
        } else {
            warn!("Server {server_id} exited with code {code}");
            self.state(
                server_id,
                ServerStatus::Crashed,
                Some(format!("Exited with code {code}")),
            );
        }
        Ok(())
    }

    async fn stream_stats(self: Arc<Self>, server_id: String) {
        let mut stats = self.docker.stats(
            &container_name(&server_id),
            Some(StatsOptions {
                stream: true,
                one_shot: false,
            }),
        );
        while let Some(Ok(s)) = stats.next().await {
            let (Some(cpu), Some(pre), Some(memory)) =
                (s.cpu_stats, s.precpu_stats, s.memory_stats)
            else {
                continue;
            };
            let total = |c: &bollard::models::ContainerCpuStats| {
                c.cpu_usage
                    .as_ref()
                    .and_then(|u| u.total_usage)
                    .unwrap_or(0)
            };
            let cpu_delta = total(&cpu).saturating_sub(total(&pre)) as f64;
            let system_delta =
                cpu.system_cpu_usage
                    .unwrap_or(0)
                    .saturating_sub(pre.system_cpu_usage.unwrap_or(0)) as f64;
            let cpus = f64::from(cpu.online_cpus.unwrap_or(1));
            let cpu_percent = if system_delta > 0.0 {
                cpu_delta / system_delta * cpus * 100.0
            } else {
                0.0
            };
            // Like `docker stats`: page cache does not count as used memory.
            let cache = memory
                .stats
                .as_ref()
                .and_then(|m| m.get("inactive_file"))
                .copied()
                .unwrap_or(0);
            self.emit(AgentMessage::ServerStats(ServerStats {
                server_id: server_id.clone(),
                cpu_percent: (cpu_percent * 10.0).round() / 10.0,
                memory_bytes: memory.usage.unwrap_or(0).saturating_sub(cache),
                memory_limit_bytes: memory.limit.unwrap_or(0),
            }));
        }
    }

    /// Asks the server who is online. Failures are expected while it starts and are not reported.
    async fn poll_players(self: Arc<Self>, server_id: String, query: QuerySpec) {
        let mut tick = tokio::time::interval(PLAYERS_INTERVAL);
        loop {
            tick.tick().await;
            let result = match query.protocol {
                QueryProtocol::Minecraft => query::minecraft(query.port).await,
                QueryProtocol::Unsupported => return,
            };
            if let Ok(players) = result {
                self.emit(AgentMessage::ServerPlayers(ServerPlayers {
                    server_id: server_id.clone(),
                    online: players.online,
                    max: players.max,
                    names: players.names,
                }));
            }
        }
    }

    /// Stops gracefully: Docker sends SIGTERM and kills the process after the timeout.
    pub async fn stop(self: Arc<Self>, server_id: String) {
        let running = self.servers.lock().unwrap().get(&server_id).cloned();
        let Some(running) = running else {
            self.state(&server_id, ServerStatus::Stopped, None);
            return;
        };
        running.stopping.store(true, Ordering::SeqCst);
        self.state(&server_id, ServerStatus::Stopping, None);
        let options = StopContainerOptions {
            t: Some(running.stop_timeout_secs),
            ..Default::default()
        };
        if let Err(e) = self
            .docker
            .stop_container(&container_name(&server_id), Some(options))
            .await
        {
            warn!("Stopping {server_id} failed: {e:#}");
        }
    }

    /// Writes one line to the server's console. Line breaks inside the command are removed,
    /// so one request can never send several commands.
    pub async fn send_command(self: Arc<Self>, server_id: String, command: String) {
        let running = self.servers.lock().unwrap().get(&server_id).cloned();
        let Some(running) = running else {
            warn!("Command for {server_id} ignored: the server is not running");
            return;
        };
        let line: String = command.chars().filter(|c| !c.is_control()).collect();
        let mut stdin = running.stdin.lock().await;
        if let Err(e) = stdin.write_all(format!("{line}\n").as_bytes()).await {
            warn!("Writing to the console of {server_id} failed: {e}");
        }
    }
}
