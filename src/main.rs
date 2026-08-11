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
    /// Summarize orchestrated runs: planner vs delegate cost/compute, whether
    /// sub-tasks were actually delegated, and whether orchestration degraded to
    /// the adapter's built-in sub-agent tool.
    Report {
        #[arg(long)]
        config: PathBuf,
        /// Only runs with this run_label (default "orchestrate").
        #[arg(long, default_value = "orchestrate")]
        run_label: String,
        /// Max runs to show (default 20).
        #[arg(long, default_value_t = 20)]
        limit: usize,
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
            // Downstream agents run with workspace write access; they must die
            // when the router does. On a signal (goose's Ctrl+C) neither
            // destructors nor `kill_on_drop` run, so an in-flight agent can keep
            // going and even commit to the repo. Explicitly SIGKILL every
            // downstream process group on SIGINT/SIGTERM before exiting.
            #[cfg(unix)]
            tokio::spawn(async {
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
        Command::Report {
            config,
            run_label,
            limit,
        } => {
            let cfg = Config::from_file(&config)?;
            let state = router_acp::state::StateFile::load(&cfg.state_file, cfg.retention());
            let all = state.all(); // newest first
            // Index children by parent.
            let mut children: std::collections::HashMap<String, Vec<_>> =
                std::collections::HashMap::new();
            for (id, s) in &all {
                if let Some(p) = &s.parent_session_id {
                    children
                        .entry(p.clone())
                        .or_default()
                        .push((id.clone(), s.clone()));
                }
            }
            let runs: Vec<_> = all
                .iter()
                .filter(|(_, s)| s.kind == "primary" && s.run_label.as_deref() == Some(&run_label))
                .take(limit.max(1))
                .collect();

            println!(
                "orchestration report — run_label='{run_label}' ({} runs)\n",
                runs.len()
            );
            let (mut delegated_runs, mut degraded_runs) = (0usize, 0usize);
            let (mut sum_planner_cost, mut sum_delegate_cost) = (0f64, 0f64);
            for (id, s) in &runs {
                let kids = children.get(id).cloned().unwrap_or_default();
                let effective_cost = |session: &router_acp::state::PersistedSession| {
                    if session.llm_requests_total > 0 && session.llm_request_cost_usd > 0.0 {
                        session.llm_request_cost_usd
                    } else {
                        session.cost_usd
                    }
                };
                let planner_cost = effective_cost(s);
                let delegate_cost: f64 =
                    kids.iter().map(|(_, c)| effective_cost(c)).sum::<f64>() + 0.0;
                let delegate_cost = if delegate_cost == 0.0 {
                    0.0
                } else {
                    delegate_cost
                };
                // Lineage = company (agents[].lineage, default agent name):
                // a delegate on a same-vendor sibling agent is NOT cross-lineage.
                let cross_lineage = kids.iter().any(|(_, c)| {
                    router_acp::session::agent_lineage(&cfg, &c.agent)
                        != router_acp::session::agent_lineage(&cfg, &s.agent)
                });
                if !kids.is_empty() {
                    delegated_runs += 1;
                }
                if s.native_subagent_calls > 0 {
                    degraded_runs += 1;
                }
                sum_planner_cost += planner_cost;
                sum_delegate_cost += delegate_cost;

                println!(
                    "{}  planner {}/{}",
                    &id[..id.len().min(20)],
                    s.agent,
                    s.model
                );
                println!(
                    "    cost: planner ${:.4} + delegates ${:.4} = ${:.4}  | compute {}s | context {}",
                    planner_cost,
                    delegate_cost,
                    planner_cost + delegate_cost,
                    s.compute_ms / 1000,
                    s.context_used
                );
                println!(
                    "    delegates: {} | cross-lineage review: {} | native-subagent (degraded): {}{}",
                    kids.len(),
                    if cross_lineage { "yes" } else { "NO" },
                    s.native_subagent_calls,
                    if s.native_subagent_calls > 0 {
                        "  ⚠ planner bypassed delegate_task"
                    } else {
                        ""
                    }
                );
                for (_, c) in &kids {
                    println!(
                        "      └─ {}/{}  ${:.4}  {}s  {}",
                        c.agent,
                        c.model,
                        effective_cost(c),
                        c.compute_ms / 1000,
                        c.title
                            .as_deref()
                            .unwrap_or("")
                            .replace('\n', " ")
                            .chars()
                            .take(60)
                            .collect::<String>()
                    );
                }
                if let Some(sha) = &s.git_sha {
                    println!(
                        "    git: {}@{}",
                        s.git_branch.as_deref().unwrap_or("?"),
                        &sha[..sha.len().min(10)]
                    );
                }
                println!();
            }
            println!("── summary ──");
            println!("  runs analyzed          : {}", runs.len());
            println!(
                "  runs that delegated    : {} ({}%)",
                delegated_runs,
                if runs.is_empty() {
                    0
                } else {
                    delegated_runs * 100 / runs.len()
                }
            );
            println!(
                "  runs degraded (native) : {} ({}%)",
                degraded_runs,
                if runs.is_empty() {
                    0
                } else {
                    degraded_runs * 100 / runs.len()
                }
            );
            println!(
                "  cost: planner ${:.2} + delegates ${:.2} = ${:.2}",
                sum_planner_cost,
                sum_delegate_cost,
                sum_planner_cost + sum_delegate_cost
            );
            println!(
                "\nnote: cost is the adapter's own usage_update.cost (USD). \"degraded\" runs used\n\
                 the built-in Task tool instead of delegate_task — their sub-work is not captured\n\
                 here. Join git_sha/branch to CI/merge outcomes for accuracy signal."
            );
            Ok(())
        }
        Command::DelegationReport { config, limit } => {
            let cfg = Config::from_file(&config)?;
            let state = router_acp::state::StateFile::load(&cfg.state_file, cfg.retention());
            let all = state.all();
            // Probe per primary session so SQLite uses idx_log_session; a
            // global kind scan is prohibitively expensive on long-lived DBs.
            let directive_counts: std::collections::HashMap<String, u64> = all
                .iter()
                .filter(|(_, session)| session.kind == "primary")
                .filter_map(|(id, _)| {
                    let count = state.log_kind_count(id, "delegation_directive");
                    (count > 0).then(|| (id.clone(), count))
                })
                .collect();
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
                .filter(|(id, session)| {
                    session.kind == "primary" && directive_counts.contains_key(id)
                })
                .collect();
            let adopted = prompted
                .iter()
                .filter(|(id, _)| children.get(id).is_some_and(|kids| !kids.is_empty()))
                .count();
            let injections: u64 = prompted
                .iter()
                .map(|(id, _)| directive_counts.get(id).copied().unwrap_or(0))
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
                    directive_counts.get(id).copied().unwrap_or(0),
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
