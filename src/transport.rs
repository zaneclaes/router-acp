//! Flushing line transports for stdio and child processes.
//!
//! The SDK's built-in `Stdio` and `AcpAgent` transports write lines without
//! flushing; their underlying writers (`blocking::Unblock`,
//! `async_process::ChildStdin`) buffer internally, so small JSON-RPC
//! messages can sit unsent indefinitely. These replacements are built on the
//! SDK's public [`Lines`] component with an explicit flush after every line.

use std::collections::HashSet;
use std::pin::Pin;
use std::process::Stdio as ProcessStdio;
use std::sync::{Mutex, OnceLock};

use futures::{Sink, Stream, StreamExt};
use tokio::io::{AsyncBufReadExt, AsyncRead, AsyncWrite, AsyncWriteExt, BufReader};

use agent_client_protocol::{ConnectTo, Lines, Role};

/// Drain stdout through the protocol actor before reporting EOF. A transport
/// error otherwise drops the actor with final notifications still queued.
async fn process_lines<R: Role>(
    mut writer: impl AsyncWrite + Unpin + Send + 'static,
    reader: impl AsyncRead + Unpin + Send + 'static,
    client: impl ConnectTo<R::Counterpart>,
) -> Result<(), agent_client_protocol::Error> {
    use agent_client_protocol::RawJsonRpcMessage;
    use agent_client_protocol::schema::v1::Response;

    let (channel, protocol) = client.into_channel_and_future();
    let marker = format!("{DISCONNECT_MARKER}: {}", uuid::Uuid::new_v4());
    let incoming_marker = marker.clone();
    let incoming = async move {
        let mut lines = BufReader::new(reader).lines();
        while let Some(line) = lines
            .next_line()
            .await
            .map_err(agent_client_protocol::Error::into_internal_error)?
        {
            let message = serde_json::from_str::<RawJsonRpcMessage>(&line).map_err(|_| {
                agent_client_protocol::Error::parse_error().data(serde_json::json!({"line": line}))
            });
            channel
                .tx
                .unbounded_send(message)
                .map_err(agent_client_protocol::Error::into_internal_error)?;
        }
        // The SDK acknowledges a queued error through its outgoing channel.
        // Consume this private acknowledgement here, never on the child's wire.
        channel
            .tx
            .unbounded_send(Err(
                agent_client_protocol::Error::internal_error().data(incoming_marker)
            ))
            .map_err(agent_client_protocol::Error::into_internal_error)?;
        std::future::pending::<Result<(), agent_client_protocol::Error>>().await
    };
    let outgoing = async move {
        let mut messages = channel.rx;
        while let Some(message) = messages.next().await {
            let message = message?;
            if let RawJsonRpcMessage::Response(Response::Error { error, .. }) = &message
                && error.data.as_ref().and_then(serde_json::Value::as_str) == Some(marker.as_str())
            {
                return Err(agent_client_protocol::Error::internal_error().data(DISCONNECT_MARKER));
            }
            let mut line = serde_json::to_vec(&message)
                .map_err(agent_client_protocol::Error::into_internal_error)?;
            line.push(b'\n');
            writer
                .write_all(&line)
                .await
                .map_err(agent_client_protocol::Error::into_internal_error)?;
            writer
                .flush()
                .await
                .map_err(agent_client_protocol::Error::into_internal_error)?;
        }
        Ok(())
    };
    futures::try_join!(incoming, outgoing, protocol)?;
    Ok(())
}

type BoxSink = Pin<Box<dyn Sink<String, Error = std::io::Error> + Send>>;
type BoxStream = Pin<Box<dyn Stream<Item = std::io::Result<String>> + Send>>;

/// Newline-delimited JSON transport over any tokio reader/writer pair,
/// flushing after every outgoing line.
///
/// EOF on the reader is reported as an `UnexpectedEof` error so the SDK
/// tears the whole connection down (its actors otherwise keep waiting on
/// the outgoing side forever). Use [`is_disconnect`] to treat that error as
/// a normal peer disconnect.
pub fn lines_transport(
    writer: impl AsyncWrite + Unpin + Send + 'static,
    reader: impl AsyncRead + Unpin + Send + 'static,
) -> Lines<BoxSink, BoxStream> {
    let outgoing: BoxSink = Box::pin(futures::sink::unfold(
        writer,
        async |mut writer, line: String| {
            let mut bytes = line.into_bytes();
            bytes.push(b'\n');
            writer.write_all(&bytes).await?;
            writer.flush().await?;
            Ok::<_, std::io::Error>(writer)
        },
    ));
    let incoming: BoxStream = Box::pin(futures::stream::unfold(
        Some(BufReader::new(reader)),
        async |reader| {
            let mut reader = reader?;
            let mut line = String::new();
            match reader.read_line(&mut line).await {
                Ok(0) => Some((
                    Err(std::io::Error::new(
                        std::io::ErrorKind::UnexpectedEof,
                        DISCONNECT_MARKER,
                    )),
                    None,
                )),
                Ok(_) => {
                    while line.ends_with('\n') || line.ends_with('\r') {
                        line.pop();
                    }
                    Some((Ok(line), Some(reader)))
                }
                Err(err) => Some((Err(err), Some(reader))),
            }
        },
    ));
    Lines::new(outgoing, incoming)
}

const DISCONNECT_MARKER: &str = "peer disconnected (EOF)";

/// True when a connection error is just the peer closing the transport.
pub fn is_disconnect(err: &agent_client_protocol::Error) -> bool {
    let text = format!("{} {}", err.message, err.data.clone().unwrap_or_default());
    text.contains(DISCONNECT_MARKER)
}

/// Serve over this process's stdin/stdout.
pub fn stdio_lines() -> Lines<BoxSink, BoxStream> {
    lines_transport(tokio::io::stdout(), tokio::io::stdin())
}

/// PIDs of live downstream agent processes. Each is spawned as its own
/// process-group leader (`process_group(0)`), so its PID doubles as a group
/// id. `kill_on_drop` handles the normal path, but it does NOT run when the
/// router is terminated by a signal (goose's Ctrl+C) or when the tokio runtime
/// tears down on exit — leaving a runaway agent that can keep editing and even
/// commit to the repo. This registry lets us guarantee teardown in those paths.
fn downstream_pids() -> &'static Mutex<HashSet<u32>> {
    static PIDS: OnceLock<Mutex<HashSet<u32>>> = OnceLock::new();
    PIDS.get_or_init(|| Mutex::new(HashSet::new()))
}

/// SIGKILL every live downstream process group — the agent AND anything it
/// spawned (e.g. a Bash mid-`git commit`). Idempotent; call on shutdown
/// (disconnect) and from the signal handler. No-op on non-unix.
pub fn kill_all_downstreams() {
    let pids: Vec<u32> = downstream_pids().lock().unwrap().drain().collect();
    for pid in pids {
        #[cfg(unix)]
        {
            // Shell builtin `kill` for reliable negative-pid (process-group)
            // semantics across shells/platforms.
            let _ = std::process::Command::new("/bin/sh")
                .arg("-c")
                .arg(format!("kill -KILL -{pid} 2>/dev/null"))
                .status();
        }
        #[cfg(not(unix))]
        let _ = pid;
    }
}

/// Unregisters a downstream PID when its connection future is dropped (any
/// exit path), so the registry never holds stale PIDs that could later be
/// reused by an unrelated process.
pub(crate) struct DownstreamPidGuard(Option<u32>);

impl DownstreamPidGuard {
    pub(crate) fn new(pid: Option<u32>) -> Self {
        if let Some(pid) = pid {
            downstream_pids().lock().unwrap().insert(pid);
        }
        Self(pid)
    }
}

impl Drop for DownstreamPidGuard {
    fn drop(&mut self) {
        if let Some(pid) = self.0 {
            // Account re-login cancels this connection. Retire its children
            // before a new credential writer can start in the same directory.
            #[cfg(unix)]
            let _ = std::process::Command::new("kill")
                .args(["-KILL", "--", &format!("-{pid}")])
                .stdout(std::process::Stdio::null())
                .stderr(std::process::Stdio::null())
                .status();
            downstream_pids().lock().unwrap().remove(&pid);
        }
    }
}

/// A downstream ACP agent process spawned with piped stdio, connected over a
/// flushing line transport. The child is killed when the connection future
/// is dropped (`kill_on_drop`) and, as a backstop for signal/shutdown paths,
/// tracked in [`downstream_pids`] for [`kill_all_downstreams`].
pub struct ProcessTransport {
    pub name: String,
    pub command: String,
    pub args: Vec<String>,
    pub env: Vec<(String, String)>,
    pub scrub_auth_env: bool,
}

/// Environment every downstream adapter process inherits from this router
/// (`ROUTER_ACP_CONFIG`, `ROUTER_ACP_BIN`), so tools running inside an adapter
/// — a host's hooks — can tell they are router-hosted and find this config.
static ROUTER_ENV: std::sync::OnceLock<Vec<(String, String)>> = std::sync::OnceLock::new();

/// Set once at startup by `serve`; later calls are ignored.
pub fn set_router_env(env: Vec<(String, String)>) {
    let _ = ROUTER_ENV.set(env);
}

pub fn router_env() -> &'static [(String, String)] {
    ROUTER_ENV.get().map(Vec::as_slice).unwrap_or_default()
}

impl<R: Role> ConnectTo<R> for ProcessTransport {
    async fn connect_to(
        self,
        client: impl ConnectTo<R::Counterpart>,
    ) -> Result<(), agent_client_protocol::Error> {
        let mut cmd = tokio::process::Command::new(&self.command);
        cmd.args(&self.args)
            .stdin(ProcessStdio::piped())
            .stdout(ProcessStdio::piped())
            .stderr(ProcessStdio::piped())
            .kill_on_drop(true);
        // Own process group so we can kill the agent AND its subprocesses (the
        // Bash it runs) as a unit on shutdown — and so a stray terminal signal
        // doesn't half-terminate it outside our control.
        #[cfg(unix)]
        cmd.process_group(0);
        if self.scrub_auth_env
            || self.name.contains('@')
            || crate::accounts::isolated_environment(&self.env)
        {
            for key in crate::accounts::AUTH_ENV {
                cmd.env_remove(key);
            }
        }
        for (k, v) in router_env() {
            cmd.env(k, v);
        }
        for (k, v) in &self.env {
            cmd.env(k, v);
        }
        let mut child = cmd.spawn().map_err(|e| {
            agent_client_protocol::Error::internal_error()
                .data(format!("failed to spawn `{}`: {e}", self.command))
        })?;
        // Track the PID and unregister when this connection future is dropped.
        let pid = child.id();
        let _pid_guard = DownstreamPidGuard::new(pid);
        let stdin = child.stdin.take().expect("piped stdin");
        let stdout = child.stdout.take().expect("piped stdout");
        let stderr = child.stderr.take().expect("piped stderr");

        // Surface downstream stderr in our logs. This never resolves: only
        // protocol termination or child exit may end the connection (on
        // child death stderr hits EOF too and must not win the race with an
        // `Ok`).
        let name = self.name.clone();
        let stderr_task = async move {
            let mut lines = BufReader::new(stderr).lines();
            while let Ok(Some(line)) = lines.next_line().await {
                // Adapter ERROR lines (e.g. Codex's "failed to flush logs to
                // SQLite error=…") must reach the relay log; the rest is noise.
                if line.contains("ERROR") {
                    tracing::warn!(target: "downstream_stderr", agent = %name, "{line}");
                } else {
                    tracing::debug!(target: "downstream_stderr", agent = %name, "{line}");
                }
            }
            std::future::pending::<()>().await
        };

        let protocol = process_lines::<R>(stdin, stdout, client);
        tokio::pin!(protocol);

        let exit = async move {
            match child.wait().await {
                Ok(status) if status.success() => Ok(()),
                Ok(status) => Err(agent_client_protocol::Error::internal_error()
                    .data(format!("downstream process exited with {status}"))),
                Err(err) => Err(agent_client_protocol::Error::internal_error()
                    .data(format!("failed to wait for downstream process: {err}"))),
            }
        };

        tokio::select! {
            result = &mut protocol => result,
            result = exit => {
                // Let buffered final output reach the handlers. Bound teardown
                // when a grandchild keeps stdout open after its parent exits.
                tokio::time::timeout(std::time::Duration::from_secs(5), &mut protocol)
                    .await
                    .unwrap_or(result)
            },
            () = stderr_task => Ok(()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use agent_client_protocol::schema::ProtocolVersion;
    use agent_client_protocol::schema::v1::InitializeRequest;
    use agent_client_protocol::{Client as ClientPeer, UntypedRole};

    #[tokio::test]
    async fn lines_transport_flushes_each_line() {
        // A duplex pipe: whatever the transport writes must be readable
        // immediately, one line per message.
        let (client_side, server_side) = tokio::io::duplex(64 * 1024);
        let (client_read, client_write) = tokio::io::split(client_side);
        let (server_read, server_write) = tokio::io::split(server_side);

        // Echo server: reads a line, asserts it's the initialize request.
        let server = tokio::spawn(async move {
            let mut reader = BufReader::new(server_read);
            let mut line = String::new();
            reader.read_line(&mut line).await.unwrap();
            assert!(line.contains("\"initialize\""), "got: {line}");
            // Reply so the client can finish.
            let value: serde_json::Value = serde_json::from_str(&line).unwrap();
            let reply = serde_json::json!({
                "jsonrpc": "2.0",
                "id": value["id"],
                "result": {"protocolVersion": 1}
            });
            let mut writer = server_write;
            writer
                .write_all(format!("{reply}\n").as_bytes())
                .await
                .unwrap();
            writer.flush().await.unwrap();
        });

        let transport = lines_transport(client_write, client_read);
        let result = ClientPeer
            .builder()
            .connect_with(transport, async |cx| {
                let resp = cx
                    .send_request(InitializeRequest::new(ProtocolVersion::V1))
                    .block_task()
                    .await?;
                assert_eq!(resp.protocol_version, ProtocolVersion::V1);
                Ok(())
            })
            .await;
        // The server hangs up right after replying; that EOF may race the
        // response delivery and is a normal disconnect, not a failure.
        if let Err(err) = result {
            assert!(is_disconnect(&err), "unexpected error: {err}");
        }
        server.await.unwrap();
    }

    #[cfg(unix)]
    fn pid_alive(pid: i32) -> bool {
        std::process::Command::new("/bin/sh")
            .arg("-c")
            .arg(format!("kill -0 {pid} 2>/dev/null"))
            .status()
            .map(|s| s.success())
            .unwrap_or(false)
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn kill_all_downstreams_reaps_the_whole_process_group() {
        use std::io::Read;
        use std::time::Duration;
        // A group leader (sh) that spawns a grandchild (sleep) — the shape of
        // an agent running a Bash `git commit`. Only kill_all_downstreams (a
        // process-group kill) should reap the grandchild; kill_on_drop would
        // not. Keep `child` alive so the group kill is the sole reaper.
        let pidfile = std::env::temp_dir().join(format!("racp-gc-{}.pid", std::process::id()));
        let _ = std::fs::remove_file(&pidfile);
        let mut cmd = tokio::process::Command::new("/bin/sh");
        cmd.arg("-c")
            .arg(format!("sleep 300 & echo $! > {}; wait", pidfile.display()));
        cmd.process_group(0);
        let child = cmd.spawn().expect("spawn group leader");
        let leader = child.id().expect("pid");
        downstream_pids().lock().unwrap().insert(leader);

        // Wait for the grandchild pid to land in the file.
        let mut buf = String::new();
        for _ in 0..100 {
            buf.clear();
            if std::fs::File::open(&pidfile)
                .and_then(|mut f| f.read_to_string(&mut buf))
                .is_ok()
                && !buf.trim().is_empty()
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        let grandchild: i32 = buf.trim().parse().expect("grandchild pid");
        assert!(pid_alive(grandchild), "grandchild alive before kill");

        kill_all_downstreams();

        let mut reaped = false;
        for _ in 0..100 {
            if !pid_alive(grandchild) {
                reaped = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        assert!(
            reaped,
            "grandchild ({grandchild}) not reaped by process-group kill"
        );
        assert!(
            !downstream_pids().lock().unwrap().contains(&leader),
            "registry drained"
        );
        let _ = std::fs::remove_file(&pidfile);
        drop(child);
    }

    #[tokio::test]
    async fn process_transport_drains_final_frames_before_exit() {
        use agent_client_protocol::{Dispatch, Handled};
        use std::sync::Arc;
        use std::sync::atomic::{AtomicUsize, Ordering};

        let count = Arc::new(AtomicUsize::new(0));
        let received = count.clone();
        let transport = ProcessTransport {
            name: "final-frames".into(),
            command: "/bin/sh".into(),
            args: vec!["-c".into(),
                "i=0; while [ $i -lt 20 ]; do printf '%s\\n' '{\"jsonrpc\":\"2.0\",\"method\":\"final\",\"params\":{}}'; i=$((i+1)); done; exit 1".into()],
            env: vec![],
            scrub_auth_env: false,
        };
        let result = tokio::time::timeout(std::time::Duration::from_secs(3),
            UntypedRole.builder().on_receive_dispatch(move |message: Dispatch, _cx| {
                let received = received.clone();
                async move {
                    if matches!(&message, Dispatch::Notification(msg) if msg.method() == "final") {
                        tokio::time::sleep(std::time::Duration::from_millis(5)).await;
                        received.fetch_add(1, Ordering::SeqCst);
                        Ok(Handled::Yes)
                    } else {
                        Ok(Handled::No { message, retry: false })
                    }
                }
            }, agent_client_protocol::on_receive_dispatch!()).connect_with(transport, async |_cx| {
                std::future::pending::<Result<(), agent_client_protocol::Error>>().await
            })
        ).await.expect("transport must terminate");
        assert!(result.is_err());
        assert_eq!(
            count.load(Ordering::SeqCst),
            20,
            "all buffered frames reached their handlers"
        );
    }

    #[tokio::test]
    async fn adapter_stderr_error_lines_are_logged_at_warn() {
        use std::sync::{Arc, Mutex};

        #[derive(Clone, Default)]
        struct Buf(Arc<Mutex<Vec<u8>>>);
        impl std::io::Write for Buf {
            fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
                self.0.lock().unwrap().extend_from_slice(bytes);
                Ok(bytes.len())
            }
            fn flush(&mut self) -> std::io::Result<()> {
                Ok(())
            }
        }
        let buf = Buf::default();
        let writer = buf.clone();
        let subscriber = tracing_subscriber::fmt()
            .with_max_level(tracing::Level::WARN)
            .with_ansi(false)
            .with_writer(move || writer.clone())
            .finish();
        let _guard = tracing::subscriber::set_default(subscriber);

        // A synthetic adapter that logs like codex app-server, then exits.
        let transport = ProcessTransport {
            name: "codex#gpt".into(),
            command: "/bin/sh".into(),
            args: vec!["-c".into(),
                "echo ' INFO codex: chatter' >&2; echo ' ERROR codex_state: failed to flush logs to SQLite error=database is locked' >&2; sleep 0.2; exit 1".into()],
            env: vec![],
            scrub_auth_env: false,
        };
        let _ = UntypedRole
            .builder()
            .connect_with(transport, async |_cx| {
                std::future::pending::<Result<(), agent_client_protocol::Error>>().await
            })
            .await;

        let logged = String::from_utf8(buf.0.lock().unwrap().clone()).unwrap();
        assert!(
            logged.contains("WARN") && logged.contains("error=database is locked"),
            "{logged}"
        );
        assert!(!logged.contains("chatter"), "{logged}");
    }

    #[tokio::test]
    async fn process_transport_kills_child_and_reports_exit() {
        // `false` exits immediately with status 1: the connection must end
        // with an error instead of hanging.
        let transport = ProcessTransport {
            name: "false".into(),
            command: "false".into(),
            args: vec![],
            env: vec![],
            scrub_auth_env: false,
        };
        let result = UntypedRole
            .builder()
            .connect_with(transport, async |_cx| {
                std::future::pending::<Result<(), agent_client_protocol::Error>>().await
            })
            .await;
        assert!(result.is_err());
    }
}
