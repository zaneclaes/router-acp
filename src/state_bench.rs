//! `router-acp state-bench`: many router-shaped processes writing state at
//! once, to compare the legacy single file with cwd shards.
//!
//! Each worker process drives one session through the public `StateStore`
//! API with the live write mix (2026-10 sample of `session_log`): 65%
//! buffered `session_update`, 25% buffered `tool_call` plus its
//! `tool_calls` row, 5% `llm_request` start, 4.5% `llm_response` finish, and
//! 0.5% load-bearing `user_prompt`/`agent_response` rows with a session
//! upsert. Like the real flusher it flushes queued rows every second.

use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use crate::state::{
    LlmRequestStart, LlmRequestUsage, LogEntry, PersistedSession, Retention, StateStore,
};
use crate::state_layout::ShardingMode;

const FLUSH_EVERY: Duration = Duration::from_secs(1);

/// What one worker process saw.
#[derive(Debug, Default, Clone, serde::Serialize, serde::Deserialize)]
pub struct WorkerReport {
    pub ops: u64,
    pub p50_ms: f64,
    pub p99_ms: f64,
    pub max_ms: f64,
    /// Periodic flushes that found the file busy and kept their rows queued.
    pub deferred_flushes: u64,
    /// Errors from writes the router cannot defer: direct log rows and upserts.
    pub load_bearing_failures: u64,
    /// Buffered rows dropped because the queue overflowed.
    pub dropped_rows: u64,
    /// Buffered rows still unwritten after the final flush.
    pub unflushed_rows: u64,
}

/// The whole run, aggregated over every worker.
#[derive(Debug, Clone, serde::Serialize)]
pub struct BenchReport {
    pub mode: &'static str,
    pub processes: usize,
    pub checkouts: usize,
    pub seconds: u64,
    pub rate_per_process: u64,
    pub total_ops: u64,
    pub ops_per_sec: f64,
    /// Median of the workers' p50 op latencies.
    pub p50_ms: f64,
    /// Worst worker p99 op latency.
    pub p99_ms: f64,
    pub max_ms: f64,
    pub deferred_flushes: u64,
    pub load_bearing_failures: u64,
    pub dropped_rows: u64,
    pub unflushed_rows: u64,
    /// Workers that exited non-zero or printed no report.
    pub failed_workers: usize,
}

/// xorshift64: deterministic per worker, no dependency.
fn next_rand(state: &mut u64) -> u64 {
    *state ^= *state << 13;
    *state ^= *state >> 7;
    *state ^= *state << 17;
    *state
}

fn millis(duration: Duration) -> f64 {
    duration.as_secs_f64() * 1000.0
}

fn entry(kind: &str, detail: &serde_json::Value) -> LogEntry {
    LogEntry {
        kind: kind.into(),
        role: "agent".into(),
        summary: kind.into(),
        detail: Some(detail.clone()),
        tokens_input: 10,
        tokens_output: 10,
        ..Default::default()
    }
}

/// One worker process: a session started in `cwd`, written at `rate` ops/s
/// for `seconds`.
pub fn worker(
    state: &Path,
    cwd: &Path,
    mode: ShardingMode,
    seconds: u64,
    rate: u64,
    seed: u64,
) -> anyhow::Result<WorkerReport> {
    let store = StateStore::try_load_with(state, Retention::default(), mode)?;
    let sid = store.new_session_id(cwd)?;
    let session = PersistedSession {
        agent: "bench".into(),
        model: "m".into(),
        downstream_session_id: format!("bench-{seed}"),
        cwd: cwd.to_path_buf(),
        kind: "primary".into(),
        ..Default::default()
    };
    let mut report = WorkerReport::default();
    if store.upsert_checked(sid.clone(), session.clone()).is_err() {
        report.load_bearing_failures += 1;
    }
    let detail = serde_json::json!({
        "sessionUpdate": "agent_message_chunk",
        "content": {"type": "text", "text": "x".repeat(400)},
    });
    let interval = Duration::from_secs_f64(1.0 / rate.max(1) as f64);
    let deadline = Instant::now() + Duration::from_secs(seconds);
    let mut rng = seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1;
    let mut latencies = Vec::new();
    let mut next = Instant::now();
    let mut last_flush = Instant::now();
    let mut open_request: Option<String> = None;
    while Instant::now() < deadline {
        let started = Instant::now();
        let ok = match next_rand(&mut rng) % 1000 {
            0..650 => {
                store.log_buffered(&sid, entry("session_update", &detail));
                true
            }
            650..900 => {
                let id = format!("tool-{}", report.ops);
                store.record_tool_call(&sid, &id, "bench", "completed", None, &detail);
                store.log_buffered(&sid, entry("tool_call", &detail));
                true
            }
            900..950 => {
                let id = format!("llm-{seed}-{}", report.ops);
                store.start_llm_request(&LlmRequestStart {
                    request_id: id.clone(),
                    router_session_id: sid.clone(),
                    parent_router_session_id: None,
                    agent: "bench".into(),
                    protocol: "anthropic".into(),
                    endpoint: "/v1/messages".into(),
                    pinned_model: "bench/m".into(),
                    model: "bench/m".into(),
                    routing_reason: "bench".into(),
                    routing_event: "steady".into(),
                    estimated_input_tokens: 100,
                });
                open_request = Some(id);
                store
                    .log_checked(&sid, &entry("llm_request", &detail))
                    .is_ok()
            }
            950..995 => {
                if let Some(id) = open_request.take() {
                    let usage = LlmRequestUsage {
                        input: 100,
                        output: 50,
                        ..Default::default()
                    };
                    store.finish_llm_request(&id, 200, 10, &usage, 0.001, None);
                }
                store
                    .log_checked(&sid, &entry("llm_response", &detail))
                    .is_ok()
            }
            _ => {
                let kind = if report.ops % 2 == 0 {
                    "user_prompt"
                } else {
                    "agent_response"
                };
                store.log_checked(&sid, &entry(kind, &detail)).is_ok()
                    && store.upsert_checked(sid.clone(), session.clone()).is_ok()
            }
        };
        report.load_bearing_failures += u64::from(!ok);
        latencies.push(started.elapsed());
        report.ops += 1;
        if last_flush.elapsed() >= FLUSH_EVERY {
            report.deferred_flushes += u64::from(store.flush_log().is_err());
            last_flush = Instant::now();
        }
        next += interval;
        if let Some(wait) = next.checked_duration_since(Instant::now()) {
            std::thread::sleep(wait);
        }
    }
    let _ = store.flush_log_final();
    latencies.sort();
    let at = |q: f64| {
        latencies
            .get(((latencies.len() as f64 * q) as usize).min(latencies.len().saturating_sub(1)))
            .copied()
            .map_or(0.0, millis)
    };
    report.p50_ms = at(0.50);
    report.p99_ms = at(0.99);
    report.max_ms = latencies.last().copied().map_or(0.0, millis);
    report.dropped_rows = store.dropped_log_rows();
    report.unflushed_rows = store.pending_log_rows() as u64;
    Ok(report)
}

/// Spawn `processes` workers of this binary against `state` and aggregate
/// their reports. Sharded workers are spread over `checkouts` directories
/// beside `state`; legacy workers write the one legacy file.
pub fn run(
    state: &Path,
    mode: ShardingMode,
    processes: usize,
    seconds: u64,
    checkouts: usize,
    rate: u64,
) -> anyhow::Result<BenchReport> {
    let root = state.parent().unwrap_or(Path::new(".")).to_path_buf();
    // A live box creates the legacy file long before any burst.
    StateStore::try_load(state, Retention::default())?;
    let checkouts = checkouts.max(1);
    let dirs: Vec<PathBuf> = (0..checkouts)
        .map(|n| root.join("checkouts").join(format!("checkout-{n}")))
        .collect();
    for dir in &dirs {
        std::fs::create_dir_all(dir)?;
    }
    let exe = std::env::current_exe()?;
    let mode_arg = match mode {
        ShardingMode::Off => "off",
        ShardingMode::Cwd => "cwd",
    };
    let started = Instant::now();
    let children = (0..processes)
        .map(|n| {
            std::process::Command::new(&exe)
                .arg("state-bench-worker")
                .arg("--state")
                .arg(state)
                .arg("--cwd")
                .arg(&dirs[n % checkouts])
                .args(["--mode", mode_arg])
                .args(["--seconds", &seconds.to_string()])
                .args(["--rate", &rate.to_string()])
                .args(["--seed", &(n as u64 + 1).to_string()])
                .stdout(std::process::Stdio::piped())
                .stderr(std::process::Stdio::inherit())
                .spawn()
        })
        .collect::<std::io::Result<Vec<_>>>()?;
    let mut reports = Vec::new();
    let mut failed_workers = 0;
    for child in children {
        let output = child.wait_with_output()?;
        match serde_json::from_slice::<WorkerReport>(&output.stdout) {
            Ok(report) if output.status.success() => reports.push(report),
            _ => failed_workers += 1,
        }
    }
    let elapsed = started.elapsed().as_secs_f64();
    let mut p50s: Vec<f64> = reports.iter().map(|r| r.p50_ms).collect();
    p50s.sort_by(f64::total_cmp);
    let sum = |field: fn(&WorkerReport) -> u64| reports.iter().map(field).sum::<u64>();
    let total_ops = sum(|r| r.ops);
    Ok(BenchReport {
        mode: if mode == ShardingMode::Cwd {
            "sharded"
        } else {
            "legacy"
        },
        processes,
        checkouts,
        seconds,
        rate_per_process: rate,
        total_ops,
        ops_per_sec: total_ops as f64 / elapsed.max(f64::EPSILON),
        p50_ms: p50s.get(p50s.len() / 2).copied().unwrap_or(0.0),
        p99_ms: reports.iter().map(|r| r.p99_ms).fold(0.0, f64::max),
        max_ms: reports.iter().map(|r| r.max_ms).fold(0.0, f64::max),
        deferred_flushes: sum(|r| r.deferred_flushes),
        load_bearing_failures: sum(|r| r.load_bearing_failures),
        dropped_rows: sum(|r| r.dropped_rows),
        unflushed_rows: sum(|r| r.unflushed_rows),
        failed_workers,
    })
}
