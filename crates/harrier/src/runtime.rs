//! Runs game servers as Docker containers from game-neutral runtime specs (ADR 0005).
//! The agent knows no games: image, env, ports and limits all come from the panel.

use crate::{files, query, transfer, wake};
use anyhow::Context;
use bollard::Docker;
use bollard::models::{ContainerCreateBody, HostConfig, PortBinding};
use bollard::query_parameters::{
    AttachContainerOptions, CreateContainerOptions, CreateImageOptions, ListContainersOptions,
    RemoveContainerOptions, StatsOptions, StopContainerOptions, WaitContainerOptions,
};
use futures_util::StreamExt;
use harrier_protocol::{
    AgentMessage, ConsoleOutput, FileEntry, FileResult, MovePath, PathRequest, PortProtocol,
    QueryProtocol, QuerySpec, ReadFile, Restart, RuntimeSpec, ServerPlayers, ServerState,
    ServerStats, ServerStatus, SleepSpec, Transfer, WakeProtocol, WriteFile,
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
use std::time::{Duration, Instant};
use tokio::io::{AsyncWrite, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::sync::{Notify, mpsc};
use tracing::{info, warn};

const LABEL: &str = "gg.harrier.server";
/// The spec's `query`, kept on the container so a restarted agent keeps asking (ADR 0009).
const QUERY_LABEL: &str = "gg.harrier.query";
const PLAYERS_INTERVAL: Duration = Duration::from_secs(10);
/// The spec's `sleep` and `readyPattern`, kept on the container so a restarted agent can keep a
/// server asleep and report correctly when it wakes (ADR 0013).
const SLEEP_LABEL: &str = "gg.harrier.sleep";
const READY_LABEL: &str = "gg.harrier.ready";
/// Marker files of sleeping servers, next to (never inside) the servers' data directories.
const SLEEPING_DIR: &str = ".harrier-sleeping";
const DEFAULT_STOP_TIMEOUT_SECS: i32 = 30;
/// Waits before restarting a crashed server; its length is the number of attempts (ADR 0010).
const RESTART_DELAYS_SECS: [u64; 3] = [10, 30, 60];
/// A server that ran this long without crashing counts restarts from zero again.
const STABLE_AFTER: Duration = Duration::from_secs(600);

/// How long to wait before restarting a crashed server, or `None` to leave it crashed.
/// A server that crashed before it became ready most likely has a problem a restart won't fix.
fn restart_delay(became_ready: bool, attempts: usize) -> Option<Duration> {
    if !became_ready {
        return None;
    }
    RESTART_DELAYS_SECS
        .get(attempts)
        .map(|s| Duration::from_secs(*s))
}

type Stdin = Pin<Box<dyn AsyncWrite + Send>>;

struct Running {
    /// Replaced after every restart.
    stdin: tokio::sync::Mutex<Option<Stdin>>,
    stopping: AtomicBool,
    /// Stopped (or being stopped) because nobody was online (ADR 0013).
    sleeping: AtomicBool,
    /// Ends a pending restart or a sleep: the server was stopped or started from the panel.
    wake: Notify,
    stop_timeout_secs: i32,
}

/// Owns the Docker connection and all servers of this node. Outlives panel connections:
/// containers keep running while the agent reconnects.
pub struct Runtime {
    docker: Docker,
    data_dir: PathBuf,
    events: mpsc::Sender<AgentMessage>,
    servers: Mutex<HashMap<String, Arc<Running>>>,
    /// Panel URL and agent token, for transfer channels (ADR 0015).
    panel: String,
    token: String,
}

fn container_name(server_id: &str) -> String {
    format!("harrier-{server_id}")
}

impl Runtime {
    pub fn new(
        data_dir: PathBuf,
        events: mpsc::Sender<AgentMessage>,
        panel: &str,
        token: &str,
    ) -> anyhow::Result<Arc<Self>> {
        let docker = Docker::connect_with_defaults().context("Connecting to Docker")?;
        Ok(Arc::new(Self {
            docker,
            data_dir,
            events,
            servers: Mutex::new(HashMap::new()),
            panel: panel.to_string(),
            token: token.to_string(),
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
            restart: None,
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
            let known = self.servers.lock().unwrap().get(&server_id).cloned();
            let label = |key: &str| container.labels.as_ref().and_then(|l| l.get(key)).cloned();
            let query = label(QUERY_LABEL).and_then(|q| serde_json::from_str::<QuerySpec>(&q).ok());
            let sleep = label(SLEEP_LABEL).and_then(|s| serde_json::from_str::<SleepSpec>(&s).ok());
            let asleep = !running && self.sleep_marker(&server_id).exists();
            if let Some(known) = known {
                // Known servers report themselves; only states the panel may have missed are sent.
                if running {
                    self.state(&server_id, ServerStatus::Running, None);
                } else if known.sleeping.load(Ordering::SeqCst) {
                    self.state(&server_id, ServerStatus::Sleeping, None);
                }
                continue;
            }
            if !running && !asleep {
                self.state(&server_id, ServerStatus::Stopped, None);
                continue;
            }
            // A running server has printed its ready line long ago; a sleeping one will again.
            let ready = if asleep {
                label(READY_LABEL).and_then(|r| Regex::new(&r).ok())
            } else {
                None
            };
            let this = self.clone();
            tokio::spawn(async move {
                if let Err(e) = this
                    .supervise(
                        &server_id,
                        ready,
                        DEFAULT_STOP_TIMEOUT_SECS,
                        query,
                        sleep,
                        asleep,
                    )
                    .await
                {
                    warn!("Re-attaching to {server_id} failed: {e:#}");
                }
            });
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
        let existing = self.servers.lock().unwrap().get(server_id).cloned();
        if let Some(running) = existing {
            // Starting a sleeping server wakes it: the same container starts again (ADR 0013).
            if running.sleeping.load(Ordering::SeqCst) {
                running.wake.notify_one();
                return Ok(());
            }
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
        if let Some(sleep) = &spec.sleep {
            labels.insert(SLEEP_LABEL.to_string(), serde_json::to_string(sleep)?);
        }
        if let Some(ready) = &spec.ready_pattern {
            labels.insert(READY_LABEL.to_string(), ready.clone());
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
            if let Err(e) = this
                .supervise(&id, ready, timeout, spec.query, spec.sleep, false)
                .await
            {
                warn!("Supervising {id} failed: {e:#}");
            }
        });
        Ok(())
    }

    /// Streams console output and stats until the container exits, then reports the result.
    /// Restarts crashed servers (ADR 0010) until they stop, keep crashing or are stopped.
    async fn supervise(
        self: Arc<Self>,
        server_id: &str,
        ready: Option<Regex>,
        stop_timeout_secs: i32,
        query: Option<QuerySpec>,
        sleep: Option<SleepSpec>,
        asleep: bool,
    ) -> anyhow::Result<()> {
        let running = Arc::new(Running {
            stdin: tokio::sync::Mutex::new(None),
            stopping: AtomicBool::new(false),
            sleeping: AtomicBool::new(asleep),
            wake: Notify::new(),
            stop_timeout_secs,
        });
        self.servers
            .lock()
            .unwrap()
            .insert(server_id.to_string(), running.clone());
        let result = self
            .run_until_done(server_id, &running, ready, query, sleep, asleep)
            .await;
        self.servers.lock().unwrap().remove(server_id);
        if let Err(e) = &result {
            self.state(server_id, ServerStatus::Crashed, Some(format!("{e:#}")));
        }
        result
    }

    async fn run_until_done(
        self: &Arc<Self>,
        server_id: &str,
        running: &Arc<Running>,
        ready: Option<Regex>,
        query: Option<QuerySpec>,
        sleep: Option<SleepSpec>,
        mut asleep: bool,
    ) -> anyhow::Result<()> {
        let name = container_name(server_id);
        let idle = sleep
            .as_ref()
            .map(|s| Duration::from_secs(u64::from(s.idle_minutes.max(1)) * 60));
        let mut attempts = 0;
        loop {
            if asleep {
                if !self
                    .sleep_until_woken(server_id, running, sleep.as_ref())
                    .await
                {
                    self.state(server_id, ServerStatus::Stopped, None);
                    return Ok(());
                }
                info!("Waking {server_id}");
                running.sleeping.store(false, Ordering::SeqCst);
                attempts = 0;
                self.state(server_id, ServerStatus::Starting, None);
                self.docker
                    .start_container(&name, None)
                    .await
                    .context("Waking the container")?;
            }
            asleep = false;

            let since = Instant::now();
            let became_ready = self
                .run_once(server_id, running, ready.clone(), query.clone(), idle)
                .await?;

            let exit = self
                .docker
                .wait_container(&name, None::<WaitContainerOptions>)
                .next()
                .await;
            let code = match exit {
                Some(Ok(r)) => r.status_code,
                // bollard reports a non-zero exit code as an error
                Some(Err(bollard::errors::Error::DockerContainerWaitError { code, .. })) => code,
                _ => -1,
            };
            if running.stopping.load(Ordering::SeqCst) {
                info!("Server {server_id} stopped");
                self.state(server_id, ServerStatus::Stopped, None);
                return Ok(());
            }
            if running.sleeping.load(Ordering::SeqCst) {
                asleep = true;
                continue;
            }
            if code == 0 {
                info!("Server {server_id} stopped");
                self.state(server_id, ServerStatus::Stopped, None);
                return Ok(());
            }

            let oom = self
                .docker
                .inspect_container(&name, None)
                .await
                .ok()
                .and_then(|c| c.state)
                .and_then(|s| s.oom_killed)
                .unwrap_or(false);
            let reason = if oom {
                "Out of memory: killed at its memory limit".to_string()
            } else {
                format!("Exited with code {code}")
            };
            warn!("Server {server_id}: {reason}");
            if since.elapsed() >= STABLE_AFTER {
                attempts = 0;
            }
            let Some(delay) = restart_delay(became_ready, attempts) else {
                let message = if attempts > 0 {
                    format!("{reason}; not restarted again after {attempts} attempts")
                } else {
                    reason
                };
                self.state(server_id, ServerStatus::Crashed, Some(message));
                return Ok(());
            };

            attempts += 1;
            self.emit(AgentMessage::ServerState(ServerState {
                server_id: server_id.into(),
                status: ServerStatus::Crashed,
                message: Some(reason),
                restart: Some(Restart {
                    attempt: attempts as u32,
                    max_attempts: RESTART_DELAYS_SECS.len() as u32,
                }),
            }));
            tokio::select! {
                () = tokio::time::sleep(delay) => {}
                () = running.wake.notified() => {}
            }
            if running.stopping.load(Ordering::SeqCst) {
                self.state(server_id, ServerStatus::Stopped, None);
                return Ok(());
            }
            info!("Restarting {server_id} (attempt {attempts})");
            self.state(server_id, ServerStatus::Starting, None);
            self.docker
                .start_container(&name, None)
                .await
                .context("Restarting the container")?;
        }
    }

    /// Attaches to the started container and streams its output, stats and players until it
    /// exits. Returns whether the server became ready.
    async fn run_once(
        self: &Arc<Self>,
        server_id: &str,
        running: &Arc<Running>,
        mut ready: Option<Regex>,
        query: Option<QuerySpec>,
        idle: Option<Duration>,
    ) -> anyhow::Result<bool> {
        let attach = self
            .docker
            .attach_container(
                &container_name(server_id),
                Some(AttachContainerOptions {
                    stdin: true,
                    stdout: true,
                    stderr: true,
                    stream: true,
                    ..Default::default()
                }),
            )
            .await?;
        *running.stdin.lock().await = Some(attach.input);
        if ready.is_none() {
            self.state(server_id, ServerStatus::Running, None);
        }

        let stats = tokio::spawn(self.clone().stream_stats(server_id.to_string()));
        let players = query.map(|q| {
            tokio::spawn(
                self.clone()
                    .poll_players(server_id.to_string(), q, running.clone(), idle),
            )
        });

        let mut output = attach.output;
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
        stats.abort();
        if let Some(players) = players {
            players.abort();
        }
        Ok(ready.is_none())
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
    /// With `idle`, a server nobody was on for that long is put to sleep (ADR 0013).
    async fn poll_players(
        self: Arc<Self>,
        server_id: String,
        query: QuerySpec,
        running: Arc<Running>,
        idle: Option<Duration>,
    ) {
        let mut tick = tokio::time::interval(PLAYERS_INTERVAL);
        let mut empty_since: Option<Instant> = None;
        loop {
            tick.tick().await;
            let result = match query.protocol {
                QueryProtocol::Minecraft => query::minecraft(query.port).await,
                QueryProtocol::Unsupported => return,
            };
            let Ok(players) = result else { continue };
            let online = players.online;
            self.emit(AgentMessage::ServerPlayers(ServerPlayers {
                server_id: server_id.clone(),
                online,
                max: players.max,
                names: players.names,
            }));
            let Some(idle) = idle else { continue };
            if online > 0 {
                empty_since = None;
            } else if empty_since.get_or_insert_with(Instant::now).elapsed() >= idle
                && !running.stopping.load(Ordering::SeqCst)
                && !running.sleeping.swap(true, Ordering::SeqCst)
            {
                info!("Nobody online on {server_id}, going to sleep");
                self.state(
                    &server_id,
                    ServerStatus::Stopping,
                    Some("Going to sleep: nobody online".into()),
                );
                // In its own task: this one ends when the container's output does.
                let this = self.clone();
                let timeout = running.stop_timeout_secs;
                tokio::spawn(async move {
                    let options = StopContainerOptions {
                        t: Some(timeout),
                        ..Default::default()
                    };
                    let name = container_name(&server_id);
                    if let Err(e) = this.docker.stop_container(&name, Some(options)).await {
                        warn!("Putting {server_id} to sleep failed: {e:#}");
                    }
                });
                return;
            }
        }
    }

    fn sleep_marker(&self, server_id: &str) -> PathBuf {
        self.data_dir.join(SLEEPING_DIR).join(server_id)
    }

    /// Waits while the server sleeps (ADR 0013): answers on its game port if the spec says how,
    /// until a player joins or the panel starts or stops it. Returns false if it was stopped.
    async fn sleep_until_woken(
        &self,
        server_id: &str,
        running: &Running,
        sleep: Option<&SleepSpec>,
    ) -> bool {
        let marker = self.sleep_marker(server_id);
        if let Some(dir) = marker.parent() {
            let _ = std::fs::create_dir_all(dir);
        }
        let _ = std::fs::write(&marker, "");

        let wake = sleep
            .and_then(|s| s.wake.as_ref())
            .filter(|w| w.protocol == WakeProtocol::Minecraft);
        let listener = match wake {
            Some(w) => match TcpListener::bind(("0.0.0.0", w.port)).await {
                Ok(listener) => Some(listener),
                Err(e) => {
                    warn!("Waking {server_id} on connect is not available: {e}");
                    None
                }
            },
            None => None,
        };
        let message = (wake.is_some() && listener.is_none())
            .then(|| "Waking on connect is not available: the game port is in use".to_string());
        info!("Server {server_id} sleeps");
        self.state(server_id, ServerStatus::Sleeping, message);

        match (&listener, wake) {
            (Some(listener), Some(w)) => tokio::select! {
                () = running.wake.notified() => {}
                () = wake::minecraft(listener, &w.status_text, &w.join_text) => {
                    info!("A player woke {server_id}");
                }
            },
            _ => running.wake.notified().await,
        }
        drop(listener); // frees the port for the container
        let _ = std::fs::remove_file(&marker);
        !running.stopping.load(Ordering::SeqCst)
    }

    /// Stops gracefully: Docker sends SIGTERM and kills the process after the timeout.
    pub async fn stop(self: Arc<Self>, server_id: String) {
        let running = self.servers.lock().unwrap().get(&server_id).cloned();
        let Some(running) = running else {
            self.state(&server_id, ServerStatus::Stopped, None);
            return;
        };
        running.stopping.store(true, Ordering::SeqCst);
        running.wake.notify_one();
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

    /// The server's data directory. Server IDs come from the panel; anything but a plain name is
    /// refused, so an ID can never point outside the data directory.
    fn server_dir(&self, server_id: &str) -> Result<PathBuf, String> {
        if server_id.is_empty()
            || !server_id
                .chars()
                .all(|c| c.is_ascii_alphanumeric() || c == '-')
        {
            return Err("Invalid server ID".into());
        }
        Ok(self.data_dir.join(server_id))
    }

    /// Answers `readFile` (ADR 0011).
    pub async fn read_file(self: Arc<Self>, request: ReadFile) {
        let dir = self.server_dir(&request.server_id);
        let result = tokio::task::spawn_blocking(move || files::read(&dir?, &request.path))
            .await
            .unwrap_or_else(|e| Err(e.to_string()));
        self.reply(request.request_id, result);
    }

    /// Answers `writeFile` (ADR 0011).
    pub async fn write_file(self: Arc<Self>, request: WriteFile) {
        let dir = self.server_dir(&request.server_id);
        let result = tokio::task::spawn_blocking(move || {
            files::write(&dir?, &request.path, &request.content).map(|()| None)
        })
        .await
        .unwrap_or_else(|e| Err(e.to_string()));
        self.reply(request.request_id, result);
    }

    fn reply(&self, request_id: String, result: Result<Option<String>, String>) {
        self.answer(request_id, result.map(|content| (content, None)));
    }

    fn answer(
        &self,
        request_id: String,
        result: Result<(Option<String>, Option<Vec<FileEntry>>), String>,
    ) {
        if let Err(e) = &result {
            warn!("File request failed: {e}");
        }
        let ((content, entries), error) = match result {
            Ok(answer) => (answer, None),
            Err(e) => ((None, None), Some(e)),
        };
        self.emit(AgentMessage::FileResult(FileResult {
            request_id,
            content,
            error,
            entries,
        }));
    }

    /// Runs a file operation on a blocking thread, inside the server's data directory.
    async fn in_server_dir<T: Send + 'static>(
        &self,
        server_id: &str,
        operation: impl FnOnce(&std::path::Path) -> Result<T, String> + Send + 'static,
    ) -> Result<T, String> {
        let dir = self.server_dir(server_id)?;
        tokio::task::spawn_blocking(move || operation(&dir))
            .await
            .unwrap_or_else(|e| Err(e.to_string()))
    }

    /// Answers `listDir` (ADR 0015).
    pub async fn list_dir(self: Arc<Self>, request: PathRequest) {
        let path = request.path;
        let result = self
            .in_server_dir(&request.server_id, move |dir| files::list(dir, &path))
            .await;
        self.answer(
            request.request_id,
            result.map(|entries| (None, Some(entries))),
        );
    }

    /// Answers `deletePath` (ADR 0015).
    pub async fn delete_path(self: Arc<Self>, request: PathRequest) {
        let path = request.path;
        let result = self
            .in_server_dir(&request.server_id, move |dir| files::remove(dir, &path))
            .await;
        self.reply(request.request_id, result.map(|()| None));
    }

    /// Answers `makeDir` (ADR 0015).
    pub async fn make_dir(self: Arc<Self>, request: PathRequest) {
        let path = request.path;
        let result = self
            .in_server_dir(&request.server_id, move |dir| files::make_dir(dir, &path))
            .await;
        self.reply(request.request_id, result.map(|()| None));
    }

    /// Answers `movePath` (ADR 0015).
    pub async fn move_path(self: Arc<Self>, request: MovePath) {
        let (from, to) = (request.from, request.to);
        let result = self
            .in_server_dir(&request.server_id, move |dir| {
                files::rename(dir, &from, &to)
            })
            .await;
        self.reply(request.request_id, result.map(|()| None));
    }

    /// Streams a file to the panel over a transfer channel (ADR 0015).
    pub async fn send_file(self: Arc<Self>, request: Transfer) {
        let url = transfer::url(&self.panel, &request.transfer_id);
        let result = match self.server_dir(&request.server_id) {
            Ok(dir) => transfer::download(&url, &self.token, dir, request.path).await,
            Err(e) => Err(anyhow::anyhow!(e)),
        };
        if let Err(e) = result {
            warn!("Download failed: {e:#}");
        }
    }

    /// Receives a file from the panel over a transfer channel (ADR 0015).
    pub async fn receive_file(self: Arc<Self>, request: Transfer) {
        let url = transfer::url(&self.panel, &request.transfer_id);
        let result = match self.server_dir(&request.server_id) {
            Ok(dir) => transfer::upload(&url, &self.token, dir, request.path).await,
            Err(e) => Err(anyhow::anyhow!(e)),
        };
        if let Err(e) = result {
            warn!("Upload failed: {e:#}");
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
        let Some(stdin) = stdin.as_mut() else { return };
        if let Err(e) = stdin.write_all(format!("{line}\n").as_bytes()).await {
            warn!("Writing to the console of {server_id} failed: {e}");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn restarts_ready_servers_three_times_with_growing_delays() {
        let delays: Vec<_> = (0..4).map(|a| restart_delay(true, a)).collect();
        assert_eq!(
            delays,
            [10, 30, 60]
                .map(|s| Some(Duration::from_secs(s)))
                .into_iter()
                .chain([None])
                .collect::<Vec<_>>()
        );
        assert_eq!(restart_delay(false, 0), None);
    }
}
