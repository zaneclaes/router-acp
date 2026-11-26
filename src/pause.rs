//! Graceful pause and resume.
//!
//! Two conditions stop every model at once and say nothing about the work in
//! progress: the network is gone, or no candidate is routeable (usage caps,
//! outages, capacity). A turn that meets either one is held open instead of
//! failed, cancelled or handed to another model, and continues by itself when
//! the condition ends.
//!
//! Live failure (2026-11-25): a workstation slept for four hours. The proxy
//! answered 502/503 to the adapters, which printed `API Error: …` as their
//! reply and ended every turn normally, so work in flight stopped at those
//! errors. The same errors cordoned healthy models as provider outages.
//!
//! - **Offline** is measured, never inferred: after a transport failure the
//!   router connects to the configured providers. Only when none answers is
//!   the machine offline. While offline nothing is cordoned or quarantined,
//!   and the proxy holds the failed request itself, so an adapter usually
//!   never sees the interruption. A turn that did end at the error is resumed
//!   on its own pin. When a provider answers again, outage cordons the loss
//!   caused are lifted.
//! - **Unroutable** is the ordinary selector's verdict. The router never
//!   bypasses a cordon or a pool rule; it waits and asks the selector again
//!   when availability changes.
//!
//! The hold is published as `_meta.routerAcp.paused` beside `busy`
//! (`session::republish_session_title`). `label` is display text, so a host
//! needs no list of reasons. Cancellation always ends a hold.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, Weak};
use std::time::{Duration, Instant, SystemTime};

use serde_json::{Value, json};

use crate::candidate::CandidateId;
use crate::session::Shared;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Reason {
    Offline,
    UsageCap,
    AtCapacity,
    Outage,
    SignedOut,
}

impl Reason {
    pub fn slug(self) -> &'static str {
        match self {
            Reason::Offline => "offline",
            Reason::UsageCap => "usage_cap",
            Reason::AtCapacity => "at_capacity",
            Reason::Outage => "outage",
            Reason::SignedOut => "signed_out",
        }
    }

    /// What the host shows where `working` was.
    pub fn label(self) -> &'static str {
        match self {
            Reason::Offline => "offline",
            Reason::UsageCap => "usage cap",
            Reason::AtCapacity => "models at capacity",
            Reason::Outage => "provider outage",
            Reason::SignedOut => "signed out",
        }
    }
}

#[derive(Debug, Clone)]
struct Held {
    id: u64,
    reason: Reason,
    since: SystemTime,
}

#[derive(Debug, Clone, Copy)]
struct Loss {
    since: Instant,
}

/// Every hold in this router, and whether the network is gone.
#[derive(Debug, Default)]
pub struct PauseState {
    next: AtomicU64,
    held: Mutex<HashMap<String, Vec<Held>>>,
    /// When each session's latest hold ended: its silence is not a stall.
    ended: Mutex<HashMap<String, i64>>,
    /// What excluded every candidate at the session's last routing decision.
    unroutable: Mutex<HashMap<String, Reason>>,
    offline: Mutex<Option<Loss>>,
    /// One measurement at a time; callers share its verdict.
    measuring: tokio::sync::Mutex<()>,
}

impl PauseState {
    pub fn is_offline(&self) -> bool {
        self.offline.lock().unwrap().is_some()
    }

    pub fn is_held(&self, sid: &str) -> bool {
        self.held.lock().unwrap().contains_key(sid)
    }

    /// Unix seconds when `sid`'s latest hold ended.
    pub fn last_ended(&self, sid: &str) -> Option<i64> {
        self.ended.lock().unwrap().get(sid).copied()
    }

    pub fn set_unroutable(&self, sid: &str, reason: Option<Reason>) {
        let mut unroutable = self.unroutable.lock().unwrap();
        match reason {
            Some(reason) => unroutable.insert(sid.to_string(), reason),
            None => unroutable.remove(sid),
        };
    }

    pub fn unroutable(&self, sid: &str) -> Option<Reason> {
        self.unroutable.lock().unwrap().get(sid).copied()
    }

    /// The `paused` value published for `sid`: null when it is not held.
    pub fn published(&self, sid: &str) -> Value {
        match self.held.lock().unwrap().get(sid).and_then(|h| h.first()) {
            None => Value::Null,
            Some(held) => json!({
                "reason": held.reason.slug(),
                "label": held.reason.label(),
                "since": crate::usage::epoch_to_rfc3339(
                    held.since
                        .duration_since(SystemTime::UNIX_EPOCH)
                        .map(|d| d.as_secs())
                        .unwrap_or(0),
                ),
            }),
        }
    }
}

/// One hold on a session, published while it lives.
pub struct Guard {
    shared: Arc<Shared>,
    sids: Vec<String>,
    id: u64,
    reason: Reason,
    announced: bool,
    since: Instant,
}

impl Guard {
    /// Hold `sids` (a session, and for a proxied request its owner too).
    pub fn begin(shared: &Arc<Shared>, sids: &[&str], reason: Reason) -> Self {
        let id = shared.pause.next.fetch_add(1, Ordering::Relaxed);
        let mut owned: Vec<String> = Vec::new();
        for sid in sids {
            if owned.iter().any(|known| known == sid) {
                continue;
            }
            owned.push(sid.to_string());
        }
        {
            let mut held = shared.pause.held.lock().unwrap();
            for sid in &owned {
                held.entry(sid.clone()).or_default().push(Held {
                    id,
                    reason,
                    since: SystemTime::now(),
                });
            }
        }
        for sid in &owned {
            tracing::warn!(session = sid, reason = reason.slug(), "paused");
            crate::session::republish_session_title(shared, sid);
        }
        Self {
            shared: shared.clone(),
            sids: owned,
            id,
            reason,
            announced: false,
            since: Instant::now(),
        }
    }

    /// The held turn waits on the router, not on its adapter.
    pub fn note(&self) {
        for sid in &self.sids {
            crate::planner_watchdog::note_progress(&self.shared, sid);
        }
    }

    pub fn elapsed(&self) -> Duration {
        self.since.elapsed()
    }

    pub fn reason(&self) -> Reason {
        self.reason
    }

    /// True once: the hold is told to the user one time.
    pub fn announce(&mut self) -> bool {
        !std::mem::replace(&mut self.announced, true)
    }

    /// The same hold, for another reason (a cap that became an outage).
    pub fn becomes(mut self, reason: Reason) -> Self {
        self.reason = reason;
        {
            let mut held = self.shared.pause.held.lock().unwrap();
            for sid in &self.sids {
                for entry in held.get_mut(sid).into_iter().flatten() {
                    if entry.id == self.id {
                        entry.reason = reason;
                    }
                }
            }
        }
        for sid in &self.sids {
            crate::session::republish_session_title(&self.shared, sid);
        }
        self
    }
}

impl Drop for Guard {
    fn drop(&mut self) {
        let now = chrono::Utc::now().timestamp();
        {
            let mut held = self.shared.pause.held.lock().unwrap();
            for sid in &self.sids {
                if let Some(list) = held.get_mut(sid) {
                    list.retain(|h| h.id != self.id);
                    if list.is_empty() {
                        held.remove(sid);
                    }
                }
            }
        }
        {
            let mut ended = self.shared.pause.ended.lock().unwrap();
            for sid in &self.sids {
                ended.insert(sid.clone(), now);
            }
        }
        self.note();
        for sid in &self.sids {
            tracing::info!(
                session = sid,
                held_secs = self.since.elapsed().as_secs(),
                "resumed"
            );
            crate::session::republish_session_title(&self.shared, sid);
        }
    }
}

const POLL: Duration = Duration::from_millis(250);
/// The selector is asked again this often even when nothing looks different,
/// so a hold can never outlive its condition by more than this.
const RECHECK: Duration = Duration::from_secs(30);
/// Retrying a pool that just refused is not instant: a cordon that flaps
/// cannot spin.
const MIN_HOLD: Duration = Duration::from_secs(2);

/// Wait until the network is back, with `sids` shown as paused. `owners` wait
/// on that work in silence, which is no stall either. False when `stopped`
/// ended the wait.
pub async fn until_online(
    shared: &Arc<Shared>,
    sids: &[&str],
    owners: &[&str],
    stopped: impl Fn() -> bool,
) -> bool {
    let guard = Guard::begin(shared, sids, Reason::Offline);
    loop {
        if stopped() {
            return false;
        }
        if !shared.pause.is_offline() {
            return true;
        }
        guard.note();
        for owner in owners {
            crate::planner_watchdog::note_progress(shared, owner);
        }
        tokio::time::sleep(POLL).await;
    }
}

/// Wait until routing is worth trying again. False when `stopped` ended it.
pub async fn until_routable(
    shared: &Arc<Shared>,
    guard: &Guard,
    class: crate::candidate::TaskClass,
    stopped: impl Fn() -> bool,
) -> bool {
    let available = || -> std::collections::HashSet<CandidateId> {
        shared
            .eligible_views(&crate::candidate::RequiredCaps::default(), class)
            .into_iter()
            .map(|view| view.id)
            .collect()
    };
    let started = Instant::now();
    let seen = available();
    let mut looked = Instant::now();
    loop {
        if stopped() {
            return false;
        }
        guard.note();
        if started.elapsed() >= MIN_HOLD && looked.elapsed() >= Duration::from_secs(1) {
            looked = Instant::now();
            // Offline holds routing too: it ends in `until_online`.
            let grew = available().difference(&seen).next().is_some();
            if !shared.pause.is_offline() && (grew || started.elapsed() >= RECHECK) {
                return true;
            }
        }
        tokio::time::sleep(POLL).await;
    }
}

/// Why `id` cannot serve now, when waiting can cure it. A disabled account, a
/// dead adapter or a candidate that never matched is not something to wait for.
fn recoverable(shared: &Shared, id: &CandidateId) -> Option<Reason> {
    if shared
        .agent_configs()
        .iter()
        .any(|agent| agent.name == id.agent && crate::accounts::routing_disabled(agent))
    {
        return None;
    }
    let lineage = shared.runtime_config().lineage_of(&id.agent);
    {
        let mut headroom = shared.headroom.lock().unwrap();
        if let Some(cordon) = headroom.usage_cordon(id) {
            return Some(if cordon.reason.contains("at capacity") {
                Reason::AtCapacity
            } else {
                Reason::UsageCap
            });
        }
        if headroom.cordon_active(&id.agent).is_some() || headroom.seat_exhausted(id) {
            return Some(Reason::UsageCap);
        }
        if headroom.model_outage(&lineage, &id.model).is_some() || headroom.is_quarantined(id) {
            return Some(Reason::Outage);
        }
    }
    shared
        .auth_rejection(&id.agent)
        .is_some()
        .then_some(Reason::SignedOut)
}

/// The condition that excludes every candidate `admits` allows, if it is one
/// that ends by itself. The most common cure is named first.
pub fn unroutable_reason(shared: &Shared, admits: impl Fn(&CandidateId) -> bool) -> Option<Reason> {
    let ids: Vec<CandidateId> = shared
        .candidates
        .lock()
        .unwrap()
        .iter()
        .map(|candidate| candidate.id.clone())
        .collect();
    let found: Vec<Reason> = ids
        .iter()
        .filter(|id| admits(id))
        .filter_map(|id| recoverable(shared, id))
        .collect();
    [
        Reason::UsageCap,
        Reason::AtCapacity,
        Reason::Outage,
        Reason::SignedOut,
    ]
    .into_iter()
    .find(|reason| found.contains(reason))
}

/// Where this router's providers are, as `host:port`.
fn probe_targets(shared: &Shared) -> Vec<String> {
    let configured = &shared.cfg.failover.connectivity_probes;
    if !configured.is_empty() {
        return configured.clone();
    }
    let mut targets = Vec::new();
    for agent in shared.agent_configs() {
        let upstream = agent
            .llm_proxy
            .as_ref()
            .and_then(|proxy| reqwest::Url::parse(&proxy.upstream_base_url).ok())
            .and_then(|url| {
                let host = url.host_str()?.to_string();
                // A loopback upstream answers with no network at all.
                (host != "localhost" && !host.starts_with("127.") && host != "[::1]")
                    .then(|| format!("{host}:{}", url.port_or_known_default().unwrap_or(443)))
            });
        let target = upstream.or_else(|| {
            match crate::accounts::provider(&agent)? {
                "claude" => Some("api.anthropic.com:443"),
                "codex" => Some("chatgpt.com:443"),
                "grok" => Some("api.x.ai:443"),
                "kimi" => Some("api.moonshot.ai:443"),
                _ => None,
            }
            .map(str::to_string)
        });
        if let Some(target) = target
            && !targets.contains(&target)
        {
            targets.push(target);
        }
    }
    targets
}

const PROBE_TIMEOUT: Duration = Duration::from_secs(3);

/// Whether any provider accepts a connection.
async fn reachable(targets: &[String]) -> bool {
    use futures::StreamExt;
    let mut probes: futures::stream::FuturesUnordered<_> = targets
        .iter()
        .map(|target| async move {
            matches!(
                tokio::time::timeout(PROBE_TIMEOUT, tokio::net::TcpStream::connect(target)).await,
                Ok(Ok(_))
            )
        })
        .collect();
    while let Some(answered) = probes.next().await {
        if answered {
            return true;
        }
    }
    false
}

/// After a transport failure: is the network gone? Measured, with one
/// confirmation, so a single dropped connection is never a pause. Routers with
/// no provider to ask (and every router with pausing off) answer no.
pub async fn confirm_offline(shared: &Arc<Shared>) -> bool {
    if !shared.cfg.failover.pause {
        return false;
    }
    if shared.pause.is_offline() {
        return true;
    }
    let targets = probe_targets(shared);
    if targets.is_empty() {
        return false;
    }
    let _one = shared.pause.measuring.lock().await;
    if shared.pause.is_offline() {
        return true;
    }
    let since = Instant::now();
    if reachable(&targets).await {
        return false;
    }
    tokio::time::sleep(Duration::from_millis(500)).await;
    if reachable(&targets).await {
        return false;
    }
    *shared.pause.offline.lock().unwrap() = Some(Loss { since });
    tracing::warn!(?targets, "offline: no provider answers; holding turns");
    spawn_recovery(Arc::downgrade(shared), targets);
    true
}

/// `confirm_offline`, only after a failure a lost network can cause. A dead
/// local adapter is not one.
pub async fn confirm_offline_after(
    shared: &Arc<Shared>,
    err: &agent_client_protocol::Error,
) -> bool {
    let local =
        crate::limits::is_local_adapter_failure(&crate::limits::error_text(err).to_lowercase());
    let transport = crate::limits::classify_failure(err) == crate::limits::FailureClass::Outage;
    transport && !local && confirm_offline(shared).await
}

/// Failures this long before the loss was measured belong to it.
const LOSS_GRACE: Duration = Duration::from_secs(120);

fn spawn_recovery(shared: Weak<Shared>, targets: Vec<String>) {
    tokio::spawn(async move {
        let mut wait = Duration::from_secs(1);
        loop {
            tokio::time::sleep(wait).await;
            wait = (wait * 2).min(Duration::from_secs(5));
            if shared.strong_count() == 0 {
                return;
            }
            if !reachable(&targets).await {
                continue;
            }
            let Some(shared) = shared.upgrade() else {
                return;
            };
            let Some(loss) = shared.pause.offline.lock().unwrap().take() else {
                return;
            };
            let lost_for = loss.since.elapsed() + LOSS_GRACE;
            let outage = Duration::from_secs(shared.cfg.failover.outage_cordon_secs);
            shared
                .headroom
                .lock()
                .unwrap()
                .forgive_outages(lost_for, outage);
            shared.publish_config_options();
            tracing::info!(
                lost_secs = loss.since.elapsed().as_secs(),
                "online again; resuming held turns"
            );
            return;
        }
    });
}

/// A reply that ends in the adapter's own report of a failed request
/// (`API Error: 503 …`, `API Error: Connection error.`). Returns where that
/// report starts. The text alone is never enough: the caller must know that
/// this very turn lost a request, because a model may quote such a line.
pub fn transport_error_reply(text: &str) -> Option<usize> {
    let start = text.rfind("API Error: ")?;
    let first = text.find("API Error: ")?;
    // Several failed requests print their reports back to back.
    let run = &text[first..];
    let start = if run
        .split("API Error: ")
        .skip(1)
        .all(|report| !report.trim().contains('\n'))
    {
        first
    } else {
        start
    };
    if start != 0 && !text[..start].ends_with('\n') {
        return None;
    }
    let report = text[start..].trim();
    if report.contains('\n') {
        return None;
    }
    let lower = report.to_lowercase();
    [
        "500",
        "502",
        "503",
        "504",
        "529",
        "connection",
        "timed out",
        "timeout",
        "fetch failed",
        "econn",
        "enotfound",
        "network",
        "unable to connect",
    ]
    .iter()
    .any(|sign| lower.contains(sign))
    .then_some(start)
}

/// How a resumed turn is told what happened.
pub const CONTINUE: &str = "[router-acp: the network connection was lost during this task and is back. Continue the interrupted task using the request above as context. Verify uncertain tool effects and do not repeat completed actions.]";

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_a_terminal_transport_report_is_a_lost_turn() {
        assert_eq!(
            transport_error_reply("API Error: 503 Managed credential temporarily unavailable."),
            Some(0)
        );
        let partial = "Pushed the branch.\n\nAPI Error: Connection error.";
        assert_eq!(transport_error_reply(partial), Some(20));
        assert_eq!(
            transport_error_reply(
                "API Error: 503 unavailable.API Error: 502 upstream request failed"
            ),
            Some(0),
            "reports printed back to back are one failure"
        );
        for prose in [
            "The log says API Error: 503 unavailable",
            "API Error: 503 unavailable\nRetried and it worked.",
            "API Error: 400 invalid request",
            "All done.",
        ] {
            assert_eq!(transport_error_reply(prose), None, "{prose}");
        }
    }

    #[test]
    fn every_reason_names_itself() {
        for reason in [
            Reason::Offline,
            Reason::UsageCap,
            Reason::AtCapacity,
            Reason::Outage,
            Reason::SignedOut,
        ] {
            assert!(!reason.slug().contains(' '));
            assert!(!reason.label().is_empty());
        }
    }

    #[tokio::test]
    async fn reachability_needs_one_provider() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let up = listener.local_addr().unwrap().to_string();
        let closed = {
            let gone = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            gone.local_addr().unwrap().to_string()
        };
        assert!(reachable(&[closed.clone(), up]).await);
        assert!(!reachable(&[closed]).await);
    }
}
