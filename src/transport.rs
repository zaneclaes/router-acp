//! Flushing line transports for stdio and child processes.
//!
//! The SDK's built-in `Stdio` and `AcpAgent` transports write lines without
//! flushing; their underlying writers (`blocking::Unblock`,
//! `async_process::ChildStdin`) buffer internally, so small JSON-RPC
//! messages can sit unsent indefinitely. These replacements are built on the
//! SDK's public [`Lines`] component with an explicit flush after every line.

use std::pin::Pin;
use std::process::Stdio as ProcessStdio;

use futures::{Sink, Stream};
use tokio::io::{AsyncBufReadExt, AsyncRead, AsyncWrite, AsyncWriteExt, BufReader};

use agent_client_protocol::{ConnectTo, Lines, Role};

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

/// A downstream ACP agent process spawned with piped stdio, connected over a
/// flushing line transport. The child is killed when the connection future
/// is dropped, and an early child exit ends the connection with an error.
pub struct ProcessTransport {
    pub name: String,
    pub command: String,
    pub args: Vec<String>,
    pub env: Vec<(String, String)>,
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
        for (k, v) in &self.env {
            cmd.env(k, v);
        }
        let mut child = cmd.spawn().map_err(|e| {
            agent_client_protocol::Error::internal_error()
                .data(format!("failed to spawn `{}`: {e}", self.command))
        })?;
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
                tracing::debug!(target: "downstream_stderr", agent = %name, "{line}");
            }
            std::future::pending::<()>().await
        };

        let transport = lines_transport(stdin, stdout);
        let protocol = ConnectTo::<R>::connect_to(transport, client);

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
            result = protocol => result,
            result = exit => result,
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

    #[tokio::test]
    async fn process_transport_kills_child_and_reports_exit() {
        // `false` exits immediately with status 1: the connection must end
        // with an error instead of hanging.
        let transport = ProcessTransport {
            name: "false".into(),
            command: "false".into(),
            args: vec![],
            env: vec![],
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
