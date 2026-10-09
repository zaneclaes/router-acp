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
    /// Keep router-owned account usage snapshots current for hosts that also
    /// run provider adapters directly, without an active router conversation.
    UsageMonitor {
        /// Path to the router configuration file. Reloaded before each poll.
        #[arg(long)]
        config: PathBuf,
    },
    /// Read sanitized router account status without spawning provider adapters.
    AccountStatus {
        #[arg(long)]
        config: PathBuf,
    },
    /// Internal: access-only credential hook for a router-owned Grok adapter.
    CredentialToken {
        #[arg(long)]
        provider: String,
        #[arg(long)]
        directory: PathBuf,
        #[arg(long)]
        runtime_directory: PathBuf,
    },
    /// Internal: finish a credential rotation even if its caller disconnects.
    #[command(hide = true)]
    CredentialRepair {
        #[arg(long)]
        provider: String,
        #[arg(long)]
        directory: PathBuf,
        #[arg(long)]
        observed: String,
    },
    /// Send one message to a router session with no ACP client attached and
    /// stream the reply to stdout — e.g. a supervisor waking a parent session
    /// whose client is gone. The session keeps its provider session and the
    /// router's delegate tools.
    Prompt {
        #[arg(long)]
        config: PathBuf,
        /// Router session id (`rtr-…`) or the provider session id one is
        /// pinned to. Omit to open a new session.
        #[arg(long)]
        session: Option<String>,
        /// Working directory for a new or reloaded session (default: cwd).
        #[arg(long)]
        cwd: Option<PathBuf>,
        /// Session mode to apply first, as a client would (e.g. `auto`).
        #[arg(long)]
        mode: Option<String>,
        /// The message to send.
        #[arg(long)]
        message: String,
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
    /// Dump the FULL interaction log for one router session, including tool
    /// calls (which `Sessions --session` omits and the in-conversation
    /// log-transcript handoff drops entirely). Needs only the state DB, not a
    /// full router config, so a downstream agent picking up a handoff can run
    /// it standalone — this is the command a `terse_handoff` skill route
    /// hands the incoming model.
    Transcript {
        /// Path to the state DB (router.yaml's `state_file`, tilde-expanded).
        #[arg(long)]
        state: PathBuf,
        /// The router session id to dump.
        #[arg(long)]
        session: String,
        /// Max log entries to include (default: effectively unbounded).
        #[arg(long, default_value_t = 100_000)]
        limit: usize,
    },
    /// Print the state DB's size facts (file, WAL, free pages, auto_vacuum,
    /// last maintenance tick) as JSON. Read-only; no row counts.
    StateStats {
        /// Path to the state DB (router.yaml's `state_file`, tilde-expanded).
        #[arg(long)]
        state: PathBuf,
    },
    /// One-off: rewrite the state DB with a full VACUUM and switch it to
    /// incremental auto_vacuum, so maintenance can shrink it from then on.
    /// Holds the write lock for the whole rewrite and needs free disk of
    /// about twice the DB size; set SQLITE_TMPDIR to a large disk.
    StateCompact {
        /// Path to the state DB (router.yaml's `state_file`, tilde-expanded).
        #[arg(long)]
        state: PathBuf,
    },
    /// Report adoption of the ordinary scoped delegation directive: how often
    /// it was injected, how often a real router delegate child was created,
    /// and whether a provider-native subagent bypassed the router.
    DelegationReport {
        #[arg(long)]
        config: PathBuf,
        /// Max prompted sessions to show (summary always covers all retained data).
        #[arg(long, default_value_t = 20)]
        limit: usize,
    },
}

#[cfg(unix)]
async fn termination_signal() {
    use tokio::signal::unix::{SignalKind, signal};
    let mut term = signal(SignalKind::terminate()).ok();
    let mut intr = signal(SignalKind::interrupt()).ok();
    let wait_term = async {
        match term.as_mut() {
            Some(s) => s.recv().await,
            None => std::future::pending().await,
        }
    };
    let wait_intr = async {
        match intr.as_mut() {
            Some(s) => s.recv().await,
            None => std::future::pending().await,
        }
    };
    tokio::select! { _ = wait_term => {}, _ = wait_intr => {} }
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
            export_router_env(&config);
            // Downstream agents run with workspace write access; they must die
            // when the router does. On a signal (goose's Ctrl+C) neither
            // destructors nor `kill_on_drop` run, so an in-flight agent can keep
            // going and even commit to the repo. Explicitly SIGKILL every
            // downstream process group on SIGINT/SIGTERM before exiting.
            #[cfg(unix)]
            tokio::spawn(async {
                termination_signal().await;
                router_acp::transport::kill_all_downstreams();
                std::process::exit(130);
            });
            let result =
                router_acp::session::serve(cfg, router_acp::transport::stdio_lines()).await;
            // Normal-exit / disconnect path: also a backstop against
            // `kill_on_drop` not firing during runtime teardown.
            router_acp::transport::kill_all_downstreams();
            match result {
                Ok(()) => Ok(()),
                Err(e) if router_acp::transport::is_disconnect(&e) => {
                    tracing::info!("client disconnected; shutting down");
                    Ok(())
                }
                Err(e) => Err(anyhow::anyhow!("router exited with error: {e}")),
            }
        }
        Command::AccountStatus { config } => {
            let config = Config::from_file(&config)?;
            println!("{}", router_acp::account_usage::status(&config));
            Ok(())
        }
        Command::CredentialToken {
            provider,
            directory,
            runtime_directory,
        } => {
            let token = router_acp::credentials::token_for_helper(
                &provider,
                &directory,
                std::env::var("GROK_AUTH_EXPIRED").as_deref() == Ok("1"),
                &runtime_directory,
            )
            .await
            .map_err(anyhow::Error::msg)?;
            println!("{token}");
            Ok(())
        }
        Command::CredentialRepair {
            provider,
            directory,
            observed,
        } => {
            let result =
                router_acp::credentials::repair_for_helper(&provider, &directory, &observed)
                    .await
                    .map_err(anyhow::Error::msg)?;
            anyhow::ensure!(
                result == router_acp::credentials::RepairOutcome::Repaired,
                "Credential repair unavailable"
            );
            Ok(())
        }
        Command::UsageMonitor { config } => {
            tracing_subscriber::fmt()
                .with_env_filter(
                    tracing_subscriber::EnvFilter::try_from_default_env()
                        .unwrap_or_else(|_| "router_acp=info".into()),
                )
                .with_writer(std::io::stderr)
                .init();
            #[cfg(unix)]
            {
                // Dropping the poll future runs kill_on_drop for an in-flight
                // Codex reader. A default SIGTERM would orphan that child.
                tokio::select! {
                    result = router_acp::usage::monitor(config) => result,
                    _ = termination_signal() => Ok(()),
                }
            }
            #[cfg(not(unix))]
            router_acp::usage::monitor(config).await
        }
        Command::Prompt {
            config,
            session,
            cwd,
            mode,
            message,
        } => {
            tracing_subscriber::fmt()
                .with_env_filter(
                    tracing_subscriber::EnvFilter::try_from_default_env()
                        .unwrap_or_else(|_| "router_acp=warn".into()),
                )
                .with_writer(std::io::stderr)
                .init();
            let cfg = Config::from_file(&config)?;
            export_router_env(&config);
            let cwd = match cwd {
                Some(cwd) => cwd,
                None => std::env::current_dir()?,
            };
            let result = router_acp::headless::run(
                cfg,
                router_acp::headless::PromptOptions {
                    session,
                    cwd,
                    message,
                    mode,
                },
            )
            .await;
            router_acp::transport::kill_all_downstreams();
            match result {
                Ok(stop) => {
                    eprintln!("router-acp: turn ended ({stop:?})");
                    Ok(())
                }
                Err(e) => Err(anyhow::anyhow!("prompt failed: {e}")),
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
                let manual = if cfg.auto_eligible(&id) {
                    ""
                } else {
                    "  (explicit selection only)"
                };
                let pinned = match cfg.pinned_version(&id) {
                    Some(version) => format!("  → pinned version {}", version.api_model),
                    None => String::new(),
                };
                println!("  candidate: {id}{manual}{pinned}");
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
                    if let Some(prior) = &s.prior_session_id {
                        println!("  switched  : from downstream session {prior}");
                    }
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
                    if s.llm_requests_total > 0 {
                        println!(
                            "  requests  : {} LLM calls · API-equivalent cost ${:.6}",
                            s.llm_requests_total, s.llm_request_cost_usd
                        );
                    }
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
        Command::Transcript {
            state,
            session,
            limit,
        } => {
            // No `--config` here by design (see the subcommand's doc comment):
            // this must run standalone from a bare state-file path, without a
            // resolved router.yaml. That means the configured retention
            // window is unknown — passing the library default would prune
            // this DB against the WRONG window on open if the real config
            // set something longer, silently deleting sessions this
            // inspection-only command has no business touching. Load with an
            // effectively-infinite retention instead: `prune_at` computes its
            // cutoff via `now.saturating_sub(max_age.as_secs())`, so
            // `Duration::MAX` saturates to a cutoff of 0 and nothing is ever
            // pruned by this path.
            let never_prune = router_acp::state::Retention {
                max_age: std::time::Duration::MAX,
            };
            let path = router_acp::config::expand_tilde(&state);
            let state = router_acp::state::StateFile::load(&path, never_prune);
            let entries = state.log_for(&session, limit.max(1));
            if entries.is_empty() {
                println!(
                    "no log entries for session {session} in {} (wrong --state path, or the \
                     session was pruned/never existed)",
                    path.display()
                );
                return Ok(());
            }
            println!(
                "transcript for session {session} ({} entries):\n",
                entries.len()
            );
            for e in entries {
                let est = if e.tokens_estimated { "~" } else { "" };
                let model = e.model.as_deref().unwrap_or("");
                println!(
                    "[{}/{}{}] {}  (in {}{est} / out {}{est})",
                    e.role,
                    e.kind,
                    if model.is_empty() {
                        String::new()
                    } else {
                        format!(" {model}")
                    },
                    e.summary.replace('\n', " "),
                    e.tokens_input,
                    e.tokens_output
                );
                if let Some(detail) = &e.detail {
                    println!("    detail: {detail}");
                }
            }
            Ok(())
        }
        Command::StateStats { state } => {
            let path = router_acp::config::expand_tilde(&state);
            let stats = router_acp::maintenance::stats(&path)?;
            println!("{}", serde_json::to_string_pretty(&stats)?);
            Ok(())
        }
        Command::StateCompact { state } => {
            let path = router_acp::config::expand_tilde(&state);
            let (before, after) = router_acp::maintenance::compact(&path)?;
            println!(
                "compacted {}: {before} -> {after} bytes (file + WAL); auto_vacuum is incremental",
                path.display()
            );
            Ok(())
        }
        Command::DelegationReport { config, limit } => {
            let cfg = Config::from_file(&config)?;
            let state = router_acp::state::StateFile::load(&cfg.state_file, cfg.retention());
            let all = state.all();
            let mut children: std::collections::HashMap<String, Vec<_>> =
                std::collections::HashMap::new();
            for (id, session) in &all {
                if let Some(parent) = &session.parent_session_id {
                    children
                        .entry(parent.clone())
                        .or_default()
                        .push((id.clone(), session.clone()));
                }
            }
            let prompted: Vec<_> = all
                .iter()
                .filter(|(_, session)| {
                    session.kind == "primary" && session.delegation_directive_injections > 0
                })
                .collect();
            let adopted = prompted
                .iter()
                .filter(|(id, _)| children.get(id).is_some_and(|kids| !kids.is_empty()))
                .count();
            let injections: u64 = prompted
                .iter()
                .map(|(_, session)| session.delegation_directive_injections)
                .sum();
            let native_bypasses: u64 = prompted
                .iter()
                .map(|(_, session)| session.native_subagent_calls)
                .sum();
            let effective_cost = |session: &router_acp::state::PersistedSession| {
                if session.llm_requests_total > 0 && session.llm_request_cost_usd > 0.0 {
                    session.llm_request_cost_usd
                } else {
                    session.cost_usd
                }
            };
            let parent_cost: f64 = prompted
                .iter()
                .map(|(_, session)| effective_cost(session))
                .sum();
            let delegate_cost: f64 = prompted
                .iter()
                .flat_map(|(id, _)| children.get(id).into_iter().flatten())
                .map(|(_, session)| effective_cost(session))
                .sum();
            let delegate_cost = if delegate_cost == 0.0 {
                0.0
            } else {
                delegate_cost
            };

            println!(
                "ordinary delegation adoption report ({} prompted sessions)\n",
                prompted.len()
            );
            for (id, session) in prompted.iter().take(limit.max(1)) {
                let kid_count = children.get(id).map_or(0, Vec::len);
                println!(
                    "{}  {}/{}  injections {} | delegates {} | native bypasses {}",
                    &id[..id.len().min(20)],
                    session.agent,
                    session.model,
                    session.delegation_directive_injections,
                    kid_count,
                    session.native_subagent_calls,
                );
            }
            println!("\n── summary ──");
            println!("  prompted sessions      : {}", prompted.len());
            println!("  directive injections   : {injections}");
            println!(
                "  sessions that delegated: {} ({}%)",
                adopted,
                if prompted.is_empty() {
                    0
                } else {
                    adopted * 100 / prompted.len()
                }
            );
            println!("  native bypass calls    : {native_bypasses}");
            println!(
                "  cost: parents ${parent_cost:.2} + delegates ${delegate_cost:.2} = ${:.2}",
                parent_cost + delegate_cost
            );
            Ok(())
        }
    }
}

/// Tell every adapter this router spawns where it came from, so tools running
/// inside an adapter (a host's hooks) can recognize a router-hosted session and
/// wake it again through `router-acp prompt --config <this config>`.
fn export_router_env(config: &std::path::Path) {
    let mut env = Vec::new();
    if let Ok(path) = std::fs::canonicalize(config) {
        env.push(("ROUTER_ACP_CONFIG".to_string(), path.display().to_string()));
    }
    if let Ok(exe) = std::env::current_exe() {
        env.push(("ROUTER_ACP_BIN".to_string(), exe.display().to_string()));
    }
    router_acp::transport::set_router_env(env);
}
