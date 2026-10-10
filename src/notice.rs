//! Typed router notices: the labeled failover, switch and retry lines a client
//! renders as dividers.
//!
//! Every notice is also ordinary text: its `headline` leads the `router-acp ·`
//! blockquote a plain ACP client prints. The structured copy rides
//! `_meta.router_acp.notices[]` on the chunk that carries that text, and the
//! delivered block is logged as a `router_notice` row so `session/load`
//! replays both.

use serde_json::{Value, json};

use crate::candidate::CandidateId;
use crate::config::Config;

/// Why the router moved (or holds) a session.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Reason {
    /// The provider failed to serve the model (HTTP 5xx, overloaded, …).
    Outage,
    /// The account hit a plan, member, rate or spend limit.
    AccountLimit,
    /// The account crossed its configured reserve but still has headroom.
    Reserve,
    /// The provider reported the model at capacity.
    Capacity,
    /// The session no longer fits the model's context window.
    ContextFull,
    /// The account's credentials were rejected.
    SignIn,
    /// The provider durably rejected a per-request alternate model (400/404).
    Rejected,
}

impl Reason {
    pub fn slug(self) -> &'static str {
        match self {
            Reason::Outage => "outage",
            Reason::AccountLimit => "account_limit",
            Reason::Reserve => "reserve",
            Reason::Capacity => "capacity",
            Reason::ContextFull => "context_overflow",
            Reason::SignIn => "auth",
            Reason::Rejected => "rejected",
        }
    }

    /// A soft cordon keeps the outgoing model usable for a handoff summary;
    /// every other reason means it cannot serve.
    pub fn cordon(self) -> &'static str {
        match self {
            Reason::Reserve => "soft",
            _ => "hard",
        }
    }

    /// The short label a divider leads with, e.g. `GPT-6.1 Sol outage`.
    pub fn label(self, model: &str) -> String {
        match self {
            Reason::Outage => format!("{model} outage"),
            Reason::AccountLimit => "Account limit".into(),
            Reason::Reserve => "Reserve reached".into(),
            Reason::Capacity => format!("{model} at capacity"),
            Reason::ContextFull => "Context full".into(),
            Reason::SignIn => "Sign-in failed".into(),
            Reason::Rejected => format!("{model} rejected"),
        }
    }
}

/// The configured display name of a candidate's model (`GPT-6.1 Sol`), or its
/// model id when none is configured.
pub fn display(cfg: &Config, id: &CandidateId) -> String {
    cfg.model_config(id)
        .and_then(|m| m.display_name.clone())
        .unwrap_or_else(|| id.model.clone())
}

/// One notice as it rides `_meta.router_acp.notices[]`.
#[derive(Debug, Clone)]
pub struct Notice {
    /// `failover`, `switch`, `retry`, or `retry_wait`.
    pub kind: &'static str,
    pub reason: Reason,
    pub label: String,
    pub headline: String,
    pub from: Option<String>,
    pub to: Option<String>,
    /// `summary`, `lookup`, or `none`.
    pub handoff: Option<&'static str>,
    /// RFC 3339 instant a `retry_wait` notice retries at.
    pub retry_at: Option<String>,
    /// The full router lines and any raw provider error, for the expanded view.
    pub detail: Vec<String>,
}

impl Notice {
    pub fn to_json(&self) -> Value {
        json!({
            "kind": self.kind,
            "reason": self.reason.slug(),
            "cordon": self.reason.cordon(),
            "label": self.label,
            "headline": self.headline,
            "from": self.from,
            "to": self.to,
            "handoff": self.handoff,
            "retry_at": self.retry_at,
            "detail": self.detail.join("\n"),
        })
    }

    /// The text line a plain client prints for this notice.
    pub fn line(&self) -> String {
        format!("router-acp · {}", self.headline)
    }
}
