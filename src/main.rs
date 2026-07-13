//! router-acp CLI: `serve --config ...` and the `mcp-delegate` stdio helper.

use std::path::PathBuf;

use clap::{Parser, Subcommand};

use router_acp::config::Config;

#[derive(Parser)]
#[command(
    name = "router-acp",
    version,
    about = "ACP session router over (agent, model) candidates"
)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Serve the router as an ACP agent on stdio.
    Serve {
        /// Path to the YAML configuration file.
        #[arg(long)]
        config: PathBuf,
    },
    /// Internal: stdio<->socket bridge for the delegate MCP server.
    /// Spawned by downstream agents as a stdio MCP server.
    McpDelegate {
        /// Unix-domain socket of the parent router.
        #[arg(long)]
        socket: PathBuf,
        /// Per-session token binding this helper to a router session.
        #[arg(long)]
        token: String,
    },
    /// Validate a configuration file and print the resolved candidates.
    CheckConfig {
        #[arg(long)]
        config: PathBuf,
    },
    /// Inspect the session state database (routing decisions + token usage).
    Sessions {
        #[arg(long)]
        config: PathBuf,
        /// Show the interaction log for a specific router session id.
        #[arg(long)]
        session: Option<String>,
        /// Max sessions to list (default 20).
        #[arg(long, default_value_t = 20)]
        limit: usize,
    },
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    match cli.command {
        Command::Serve { config } => {
            // stdout carries the ACP protocol; all logging goes to stderr.
            tracing_subscriber::fmt()
                .with_env_filter(
                    tracing_subscriber::EnvFilter::try_from_default_env()
                        .unwrap_or_else(|_| "router_acp=info".into()),
                )
                .with_writer(std::io::stderr)
                .init();
            let cfg = Config::from_file(&config)?;
            match router_acp::session::serve(cfg, router_acp::transport::stdio_lines()).await {
                Ok(()) => Ok(()),
                Err(e) if router_acp::transport::is_disconnect(&e) => {
                    tracing::info!("client disconnected; shutting down");
                    Ok(())
                }
                Err(e) => Err(anyhow::anyhow!("router exited with error: {e}")),
            }
        }
        Command::McpDelegate { socket, token } => {
            router_acp::delegate_mcp::run_helper(&socket, &token)
                .await
                .map_err(|e| anyhow::anyhow!("mcp-delegate bridge failed: {e}"))
        }
        Command::CheckConfig { config } => {
            let cfg = Config::from_file(&config)?;
            println!("configuration OK: {} agent(s)", cfg.agents.len());
            for id in cfg.declared_candidates() {
                println!("  candidate: {id}");
            }
            Ok(())
        }
        Command::Sessions {
            config,
            session,
            limit,
        } => {
            let cfg = Config::from_file(&config)?;
            let state = router_acp::state::StateFile::load(&cfg.state_file, cfg.retention());
            match session {
                Some(sid) => {
                    let Some(s) = state.get(&sid) else {
                        anyhow::bail!("no such session: {sid}");
                    };
                    println!("session {sid}");
                    println!("  candidate : {}/{}", s.agent, s.model);
                    println!(
                        "  kind      : {}{}",
                        s.kind,
                        s.parent_session_id
                            .map(|p| format!(" (parent {p})"))
                            .unwrap_or_default()
                    );
                    if let Some(l) = &s.run_label {
                        println!("  run_label : {l}");
                    }
                    if let Some(t) = &s.title {
                        println!("  title     : {t}");
                    }
                    println!(
                        "  tokens    : in {} / out {} / total {} · context {}",
                        s.tokens_input, s.tokens_output, s.tokens_total, s.context_used
                    );
                    if let Some(r) = &s.routing {
                        println!(
                            "  why       : {}",
                            r.get("reason").and_then(|v| v.as_str()).unwrap_or("—")
                        );
                    }
                    println!("  --- log ---");
                    for e in state.log_for(&sid, limit.max(1)) {
                        let est = if e.tokens_estimated { "~" } else { "" };
                        println!(
                            "  [{}/{}] {}  (in {}{est} / out {}{est})",
                            e.role,
                            e.kind,
                            e.summary.replace('\n', " "),
                            e.tokens_input,
                            e.tokens_output
                        );
                    }
                }
                None => {
                    let mut rows = state.all();
                    rows.truncate(limit.max(1));
                    println!("{} session(s) (newest first):", rows.len());
                    for (id, s) in rows {
                        let tree = if s.parent_session_id.is_some() {
                            "  └─ "
                        } else {
                            ""
                        };
                        println!(
                            "{tree}{id}  {}/{}  [{}]  tok {}  {}",
                            s.agent,
                            s.model,
                            s.kind,
                            s.tokens_total,
                            s.title.as_deref().unwrap_or("")
                        );
                    }
                }
            }
            Ok(())
        }
    }
}
