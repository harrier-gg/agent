//! Runs game servers as Docker containers from game-neutral runtime specs (ADR 0005).
//! The agent knows no games: image, env, ports and limits all come from the panel.

use crate::{backup, download, files, query, transfer, wake};
use anyhow::Context;
use bollard::Docker;
use bollard::models::{ContainerCreateBody, HostConfig, PortBinding};
use bollard::query_parameters::{
    AttachContainerOptions, CreateContainerOptions, CreateImageOptions, ListContainersOptions,
    RemoveContainerOptions, StatsOptions, StopContainerOptions, WaitContainerOptions,
};
use futures_util::StreamExt;
use harrier_protocol::{
    AgentMessage, BackupAction, BackupRef, BackupResult, ConsoleOutput, CreateBackup, DownloadFile,
    FileEntry, FileResult, InstallSpec, MovePath, PathRequest, PortProtocol, QueryProtocol,
    QuerySpec, Quiesce, ReadFile, Restart, RestoreBackup, RuntimeSpec, ServerPlayers,
    ServerRequest, ServerState, ServerStats, ServerStatus, SleepSpec, Transfer, WakeProtocol,
    WriteFile,
};
use regex::Regex;
use std::collections::{HashMap, HashSet, VecDeque};
// ponytail: Unix only (container user = owner of the data directory); Windows agents come with
// `runtime: native` (ADR 0005)
use std::os::unix::fs::MetadataExt;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::io::{AsyncWrite, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::sync::{Notify, broadcast, mpsc};
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
    /// Whole console lines (see `Lines`) for whoever waits for one, like a backup for
    /// "Saved the game". Subscribers only ever get complete lines, so they never start mid-line.
    output: broadcast::Sender<String>,
    stop_timeout_secs: i32,
}

/// Owns the Docker connection and all servers of this node. Outlives panel connections:
/// containers keep running while the agent reconnects.
pub struct Runtime {
    docker: Docker,
    data_dir: PathBuf,
    events: mpsc::Sender<AgentMessage>,
    servers: Mutex<HashMap<String, Arc<Running>>>,
    /// Servers with a backup or restore in progress; one at a time per server (ADR 0016).
    backing_up: Mutex<HashSet<String>>,
    /// Panel URL and agent token, for transfer channels (ADR 0015).
    panel: String,
    token: String,
}

/// Cuts console output into whole lines. Console output is untrusted: players can write anything
/// into chat, so patterns are only matched against whole lines (templates anchor theirs to the
/// game's own log lines). A reader only trusts a line start after a line break it saw itself:
/// the first line after attaching may be the end of an older one, and so is the rest of a line
/// that was too long to keep.
struct Lines {
    /// The last, incomplete line.
    tail: String,
    /// Skip everything up to the next line break: not at a line start.
    resync: bool,
}

impl Lines {
    /// A line longer than this is dropped instead of growing without end.
    const MAX: usize = 64 * 1024;

    /// For a stream joined at an unknown point, like an attach to a container.
    fn new() -> Self {
        Self {
            tail: String::new(),
            resync: true,
        }
    }

    /// Adds output; returns the lines it completed, without line break and colour codes.
    fn push(&mut self, chunk: &str) -> Vec<String> {
        static ANSI: std::sync::LazyLock<Regex> =
            std::sync::LazyLock::new(|| Regex::new("\\x1b\\[[0-9;?]*[A-Za-z]").unwrap());
        self.tail.push_str(chunk);
        let mut lines = Vec::new();
        while let Some(end) = self.tail.find('\n') {
            let line: String = self.tail.drain(..=end).collect();
            if std::mem::take(&mut self.resync) {
                continue; // the end of a line whose start was not seen
            }
            lines.push(
                ANSI.replace_all(line.trim_end_matches(['\n', '\r']), "")
                    .into_owned(),
            );
        }
        if self.tail.len() > Self::MAX {
            self.tail.clear();
            self.resync = true;
        }
        lines
    }
}

/// `/a/b`: absolute, no empty, `.` or `..` parts, and nothing Docker reads as bind options.
fn is_plain_absolute_path(path: &str) -> bool {
    path.strip_prefix('/').is_some_and(|rest| {
        rest.split('/').all(|part| {
            !part.is_empty()
                && part != "."
                && part != ".."
                && part
                    .chars()
                    .all(|c| c.is_ascii_alphanumeric() || "._-".contains(c))
        })
    })
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
            backing_up: Mutex::new(HashSet::new()),
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
        let mount_path = spec.mount_path.as_deref().unwrap_or("/data");
        anyhow::ensure!(
            is_plain_absolute_path(mount_path),
            "Invalid mount path {mount_path:?}"
        );
        if let Some(signal) = &spec.stop_signal {
            anyhow::ensure!(
                ["SIGINT", "SIGTERM"].contains(&signal.as_str()),
                "Unsupported stop signal {signal:?}"
            );
        }

        self.pull(server_id, &spec.image).await?;

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
        // The server runs as the owner of its data directory, never as root.
        let user = format!("{}:{}", owner.uid(), owner.gid());
        if let Some(install) = &spec.install {
            self.install(server_id, install, &spec, &data, &user)
                .await?;
        }

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
            user: Some(user),
            // Docker sends it on stop, also when the server goes to sleep.
            stop_signal: spec.stop_signal.clone(),
            tty: Some(true),
            open_stdin: Some(true),
            attach_stdin: Some(true),
            host_config: Some(HostConfig {
                binds: Some(vec![format!("{}:{mount_path}", data.display())]),
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

    async fn pull(&self, server_id: &str, image: &str) -> anyhow::Result<()> {
        self.state(
            server_id,
            ServerStatus::Installing,
            Some(format!("Pulling {image}")),
        );
        let mut pull = self.docker.create_image(
            Some(CreateImageOptions {
                from_image: Some(image.to_string()),
                ..Default::default()
            }),
            None,
            None,
        );
        while let Some(progress) = pull.next().await {
            progress.with_context(|| format!("Pulling {image}"))?;
        }
        Ok(())
    }

    /// Runs the install script (ADR 0022): as root with Docker's default capabilities but no new
    /// privileges, and with only the data directory mounted. Afterwards every file is handed back
    /// to `user`, so the server itself never needs root.
    async fn install(
        &self,
        server_id: &str,
        install: &InstallSpec,
        spec: &RuntimeSpec,
        data: &Path,
        user: &str,
    ) -> anyhow::Result<()> {
        self.pull(server_id, &install.image).await?;
        let name = format!("{}-install", container_name(server_id));
        let remove = || {
            self.docker.remove_container(
                &name,
                Some(RemoveContainerOptions {
                    force: true,
                    ..Default::default()
                }),
            )
        };
        let _ = remove().await;
        let result = self
            .run_install(server_id, &name, install, spec, data, user)
            .await;
        let _ = remove().await;
        result
    }

    async fn run_install(
        &self,
        server_id: &str,
        name: &str,
        install: &InstallSpec,
        spec: &RuntimeSpec,
        data: &Path,
        user: &str,
    ) -> anyhow::Result<()> {
        // The script runs in a shell of its own, so `exit` or `set -e` in it cannot skip the
        // chown. Eggs are often saved with Windows line endings.
        let wrapper =
            format!("\"$0\" -c \"$1\"\nstatus=$?; chown -R {user} /mnt/server; exit $status");
        let body = ContainerCreateBody {
            image: Some(install.image.clone()),
            entrypoint: Some(vec![
                install.entrypoint.clone(),
                "-c".into(),
                wrapper,
                install.entrypoint.clone(),
                install.script.replace("\r\n", "\n"),
            ]),
            env: Some(spec.env.iter().map(|(k, v)| format!("{k}={v}")).collect()),
            user: Some("root".into()),
            host_config: Some(HostConfig {
                binds: Some(vec![format!("{}:/mnt/server", data.display())]),
                memory: Some(i64::from(spec.memory_mb) * 1024 * 1024),
                security_opt: Some(vec!["no-new-privileges".into()]),
                ..Default::default()
            }),
            ..Default::default()
        };
        self.docker
            .create_container(
                Some(CreateContainerOptions {
                    name: Some(name.to_string()),
                    ..Default::default()
                }),
                body,
            )
            .await
            .context("Creating the install container")?;
        let mut output = self
            .docker
            .attach_container(
                name,
                Some(AttachContainerOptions {
                    stdout: true,
                    stderr: true,
                    stream: true,
                    ..Default::default()
                }),
            )
            .await?
            .output;
        self.state(
            server_id,
            ServerStatus::Installing,
            Some("Running the install script".into()),
        );
        self.docker
            .start_container(name, None)
            .await
            .context("Starting the install container")?;

        // Attached before the start: the output begins at a line start.
        let mut lines = Lines {
            tail: String::new(),
            resync: false,
        };
        let mut last = VecDeque::new();
        while let Some(Ok(chunk)) = output.next().await {
            let data = String::from_utf8_lossy(&chunk.into_bytes()).into_owned();
            for line in lines.push(&data) {
                if last.len() == 10 {
                    last.pop_front();
                }
                last.push_back(line);
            }
            self.emit(AgentMessage::ConsoleOutput(ConsoleOutput {
                server_id: server_id.to_string(),
                data,
            }));
        }
        let code = self.exit_code(name).await;
        if code != 0 {
            last.push_back(lines.tail);
            let output = Vec::from(last).join("\n");
            anyhow::bail!(
                "The install script exited with code {code}:\n{}",
                output.trim()
            );
        }
        Ok(())
    }

    /// Waits for the container to exit; -1 if that cannot be known.
    async fn exit_code(&self, name: &str) -> i64 {
        match self
            .docker
            .wait_container(name, None::<WaitContainerOptions>)
            .next()
            .await
        {
            Some(Ok(r)) => r.status_code,
            // bollard reports a non-zero exit code as an error
            Some(Err(bollard::errors::Error::DockerContainerWaitError { code, .. })) => code,
            _ => -1,
        }
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
            output: broadcast::channel(256).0,
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

            let code = self.exit_code(&name).await;
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
        // Attached to an already running output stream: the first line may be cut off.
        let mut lines = Lines::new();
        while let Some(chunk) = output.next().await {
            let Ok(chunk) = chunk else { break };
            let data = String::from_utf8_lossy(&chunk.into_bytes()).into_owned();
            for line in lines.push(&data) {
                if ready
                    .as_ref()
                    .is_some_and(|pattern| pattern.is_match(&line))
                {
                    self.state(server_id, ServerStatus::Running, None);
                    ready = None;
                }
                let _ = running.output.send(line);
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
                QueryProtocol::A2s => query::a2s(query.port).await,
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

    /// Stops gracefully: Docker sends the stop signal (SIGTERM unless the spec says otherwise) and kills the process after the timeout.
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

    /// Answers `downloadFile` once the file is in place (ADR 0017).
    pub async fn download_file(self: Arc<Self>, request: DownloadFile) {
        let DownloadFile {
            request_id,
            server_id,
            url,
            path,
            size,
            sha512,
            sha1,
            sha256,
        } = request;
        let result = self
            .in_server_dir(&server_id, move |dir| {
                let expected = download::Expected {
                    size,
                    sha512: sha512.as_deref(),
                    sha1: sha1.as_deref(),
                    sha256: sha256.as_deref(),
                };
                download::download(dir, &path, &url, &expected)
            })
            .await;
        if let Ok(size) = &result {
            info!("Downloaded {size} bytes for {server_id}");
        }
        self.reply(request_id, result.map(|_| None));
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

    /// Answers `createBackup` with `backupResult` when done (ADR 0016).
    pub async fn create_backup(self: Arc<Self>, request: CreateBackup) {
        let id = request.server_id;
        let result = self
            .clone()
            .backup(
                &id,
                Some(request.keep),
                request.quiesce,
                request.exclude,
                false,
            )
            .await;
        self.backup_result(id, BackupAction::Create, result);
    }

    /// Answers `restoreBackup` with `backupResult`. Backs up the current state first.
    pub async fn restore_backup(self: Arc<Self>, request: RestoreBackup) {
        let id = request.server_id;
        let name = request.name;
        let exclude = request.exclude;
        let result = async {
            if self.servers.lock().unwrap().contains_key(&id) {
                return Err("Stop the server before restoring a backup".to_string());
            }
            self.clone()
                .backup(&id, None, None, exclude.clone(), true)
                .await?;
            let (dir, backups) = (self.server_dir(&id)?, backup::dir(&self.data_dir, &id));
            let restored = name.clone();
            tokio::task::spawn_blocking(move || {
                backup::restore(&dir, &backups, &restored, &exclude)
            })
            .await
            .map_err(|e| e.to_string())??;
            info!("Restored {id} from {name}");
            Ok((name, 0))
        }
        .await;
        self.backup_result(id, BackupAction::Restore, result);
    }

    fn backup_result(
        &self,
        server_id: String,
        action: BackupAction,
        result: Result<(String, u64), String>,
    ) {
        if let Err(e) = &result {
            warn!("Backup of {server_id} failed: {e}");
        }
        let (name, size, error) = match result {
            Ok((name, size)) => (Some(name), (size > 0).then_some(size), None),
            Err(e) => (None, None, Some(e)),
        };
        self.emit(AgentMessage::BackupResult(BackupResult {
            server_id,
            action,
            name,
            size,
            error,
        }));
    }

    /// Archives the server; a running one is quiesced with the template's commands first.
    /// `keep` prunes the schedule's backups; `before_restore` names it so it is kept apart.
    async fn backup(
        self: Arc<Self>,
        server_id: &str,
        keep: Option<u32>,
        quiesce: Option<Quiesce>,
        exclude: Vec<String>,
        before_restore: bool,
    ) -> Result<(String, u64), String> {
        let dir = self.server_dir(server_id)?;
        if !self
            .backing_up
            .lock()
            .unwrap()
            .insert(server_id.to_string())
        {
            return Err("A backup of this server is already in progress".into());
        }
        let running = self
            .servers
            .lock()
            .unwrap()
            .get(server_id)
            .cloned()
            .filter(|r| !r.sleeping.load(Ordering::SeqCst));
        let quiesce = running.as_ref().and(quiesce);
        let saved = match (&running, &quiesce) {
            (Some(running), Some(quiesce)) => {
                self.clone().quiesce(server_id, running, quiesce).await
            }
            _ => Ok(()),
        };
        let result = match saved {
            Ok(()) => {
                let backups = backup::dir(&self.data_dir, server_id);
                let name = backup::new_name(before_restore);
                let archived = name.clone();
                tokio::task::spawn_blocking(move || {
                    let size = backup::create(&dir, &backups, &archived, &exclude)?;
                    backup::prune(&backups, keep.unwrap_or(u32::MAX))?;
                    Ok::<_, String>(size)
                })
                .await
                .map_err(|e| e.to_string())
                .and_then(|r| r)
                .map(|size| (name, size))
            }
            Err(e) => Err(e),
        };
        // Saving goes back on even if the copy failed.
        if let Some(quiesce) = quiesce {
            for command in quiesce.after {
                self.clone()
                    .send_command(server_id.to_string(), command)
                    .await;
            }
        }
        self.backing_up.lock().unwrap().remove(server_id);
        result
    }

    /// Sends the `before` commands and waits up to a minute for the `ready` line.
    async fn quiesce(
        self: Arc<Self>,
        server_id: &str,
        running: &Running,
        quiesce: &Quiesce,
    ) -> Result<(), String> {
        let ready = quiesce
            .ready
            .as_deref()
            .map(Regex::new)
            .transpose()
            .map_err(|e| e.to_string())?;
        let mut output = running.output.subscribe();
        for command in &quiesce.before {
            self.clone()
                .send_command(server_id.to_string(), command.clone())
                .await;
        }
        let Some(ready) = ready else { return Ok(()) };
        tokio::time::timeout(Duration::from_secs(60), async {
            while let Ok(line) = output.recv().await {
                if ready.is_match(&line) {
                    return true;
                }
            }
            false
        })
        .await
        .ok()
        .filter(|saved| *saved)
        .map(|_| ())
        .ok_or_else(|| "The game did not confirm saving within a minute".to_string())
    }

    /// Answers `listBackups` (ADR 0016).
    pub async fn list_backups(self: Arc<Self>, request: ServerRequest) {
        let result = match self.server_dir(&request.server_id) {
            Ok(_) => {
                let backups = backup::dir(&self.data_dir, &request.server_id);
                tokio::task::spawn_blocking(move || backup::list(&backups))
                    .await
                    .unwrap_or_else(|e| Err(e.to_string()))
            }
            Err(e) => Err(e),
        };
        self.answer(
            request.request_id,
            result.map(|entries| (None, Some(entries))),
        );
    }

    /// Answers `deleteServer`: removes the container, the data directory and the backups. Refused
    /// while the server runs or sleeps, or while a backup is being made.
    pub async fn delete_server(self: Arc<Self>, request: ServerRequest) {
        let id = request.server_id;
        let result = async {
            let dir = self.server_dir(&id)?;
            if self.servers.lock().unwrap().contains_key(&id) {
                return Err("Stop the server before deleting it".to_string());
            }
            // Holding the backup slot keeps a scheduled backup from starting meanwhile.
            if !self.backing_up.lock().unwrap().insert(id.clone()) {
                return Err(
                    "A backup of this server is in progress; try again when it is done".into(),
                );
            }
            let removed = async {
                match self
                    .docker
                    .remove_container(
                        &container_name(&id),
                        Some(RemoveContainerOptions {
                            force: true,
                            ..Default::default()
                        }),
                    )
                    .await
                {
                    Ok(()) => {}
                    Err(bollard::errors::Error::DockerResponseServerError {
                        status_code: 404,
                        ..
                    }) => {}
                    Err(e) => return Err(format!("Removing the container: {e}")),
                }
                let backups = backup::dir(&self.data_dir, &id);
                tokio::task::spawn_blocking(move || {
                    for dir in [dir, backups] {
                        match std::fs::remove_dir_all(&dir) {
                            Err(e) if e.kind() != std::io::ErrorKind::NotFound => {
                                return Err(format!("Deleting {}: {e}", dir.display()));
                            }
                            _ => {}
                        }
                    }
                    Ok(())
                })
                .await
                .map_err(|e| e.to_string())?
            }
            .await;
            self.backing_up.lock().unwrap().remove(&id);
            removed
        }
        .await;
        if result.is_ok() {
            info!("Deleted server {id}");
        }
        self.reply(request.request_id, result.map(|()| None));
    }

    /// Answers `deleteBackup` (ADR 0016).
    pub async fn delete_backup(self: Arc<Self>, request: BackupRef) {
        let result = self.server_dir(&request.server_id).and_then(|_| {
            backup::remove(
                &backup::dir(&self.data_dir, &request.server_id),
                &request.name,
            )
        });
        self.reply(request.request_id, result.map(|()| None));
    }

    /// Streams a backup to the panel over a transfer channel (ADR 0015, 0016).
    pub async fn send_backup(self: Arc<Self>, request: Transfer) {
        let url = transfer::url(&self.panel, &request.transfer_id);
        let result = match self.server_dir(&request.server_id) {
            Ok(_) => {
                let backups = backup::dir(&self.data_dir, &request.server_id);
                transfer::download(&url, &self.token, backups, request.path).await
            }
            Err(e) => Err(anyhow::anyhow!(e)),
        };
        if let Err(e) = result {
            warn!("Backup download failed: {e:#}");
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
    fn console_patterns_match_whole_lines_only() {
        // The Minecraft template's pattern: the game's own line, not a player's chat.
        let saved = Regex::new(
            r"^\[[0-9:]{8}(\] \[Server thread/INFO\]| INFO\]): (System chat: )?Saved the game$",
        )
        .unwrap();
        let system = "[12:00:00] [Server thread/INFO]: System chat: Saved the game\n";
        let mut lines = Lines::new();
        let mut matches = |chunk: &str| lines.push(chunk).iter().any(|l| saved.is_match(l));

        // Attached mid-stream: the first line may be the end of a chat line, so it never counts.
        assert!(!matches(system));
        assert!(!matches(
            "[12:00:01] [Server thread/INFO]: <Griefer> Saved the game\n"
        ));
        assert!(!matches(
            "[12:00:02] [Server thread/INFO]: <Griefer> INFO]: Saved the game\n"
        ));
        // Split over chunks, with a colour code and \r\n from the terminal.
        assert!(!matches(
            "\x1b[0m[12:00:03] [Server thread/INFO]: System chat: Saved"
        ));
        assert!(matches(" the game\x1b[0m\r\n"));

        // A line that never ends is dropped, and so is the rest of it once its end arrives.
        assert!(!matches(&"x".repeat(Lines::MAX + 1)));
        assert!(!matches(system));
        assert!(matches(system));
    }

    /// Needs Docker: `cargo test -p harrier -- --ignored install`.
    #[tokio::test]
    #[ignore]
    async fn install_runs_as_root_and_hands_the_files_back() {
        let data = std::env::temp_dir().join(format!("harrier-install-{}", std::process::id()));
        std::fs::create_dir_all(&data).unwrap();
        let owner = std::fs::metadata(&data).unwrap();
        let user = format!("{}:{}", owner.uid(), owner.gid());
        let (events, _rx) = mpsc::channel(1024);
        let runtime = Runtime::new(std::env::temp_dir(), events, "", "").unwrap();
        let spec: RuntimeSpec = serde_json::from_value(serde_json::json!({
            "image": "unused", "env": {"GREETING": "hi"}, "ports": [], "memoryMb": 256,
            "stopTimeoutSecs": 10
        }))
        .unwrap();
        let install = |script: &str| InstallSpec {
            image: "ghcr.io/parkervcp/installers:debian".into(),
            entrypoint: "bash".into(),
            script: script.into(),
        };

        let script = "id -u > /mnt/server/uid\r\necho $GREETING > /mnt/server/x\r\nexit 0";
        runtime
            .install("test", &install(script), &spec, &data, &user)
            .await
            .unwrap();
        assert_eq!(std::fs::read_to_string(data.join("uid")).unwrap(), "0\n");
        assert_eq!(std::fs::read_to_string(data.join("x")).unwrap(), "hi\n");

        // Ownership as containers see it (Docker Desktop shows the host user for every file).
        let script = "stat -c %u:%g /mnt/server/x > /mnt/server/owner\nset -e\necho boom\nfalse";
        let error = runtime
            .install("test", &install(script), &spec, &data, &user)
            .await
            .unwrap_err();
        assert_eq!(
            format!("{error:#}"),
            "The install script exited with code 1:\nboom"
        );
        let leftover = runtime
            .docker
            .inspect_container("harrier-test-install", None)
            .await;
        assert!(leftover.is_err(), "the install container is removed");
        let seen = std::fs::read_to_string(data.join("owner")).unwrap();
        assert_eq!(seen.trim(), user);
        std::fs::remove_dir_all(&data).unwrap();
    }

    #[test]
    fn mount_paths_are_plain_absolute_paths() {
        for ok in ["/data", "/home/container", "/a/b.c/d-e_f"] {
            assert!(is_plain_absolute_path(ok), "{ok}");
        }
        for bad in [
            "",
            "/",
            "data",
            "/a/",
            "/a//b",
            "/a/../etc",
            "/./a",
            "/data:ro",
            "/a b",
        ] {
            assert!(!is_plain_absolute_path(bad), "{bad}");
        }
    }

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
