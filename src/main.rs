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
                let delegate_cost: f64 = kids.iter().map(|(_, c)| c.cost_usd).sum::<f64>() + 0.0;
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
                sum_planner_cost += s.cost_usd;
                sum_delegate_cost += delegate_cost;

                println!(
                    "{}  planner {}/{}",
                    &id[..id.len().min(20)],
                    s.agent,
                    s.model
                );
                println!(
                    "    cost: planner ${:.4} + delegates ${:.4} = ${:.4}  | compute {}s | context {}",
                    s.cost_usd,
                    delegate_cost,
                    s.cost_usd + delegate_cost,
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
                        c.cost_usd,
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
    }
}
