//! router-acp: an ACP session router over `(agent, model)` candidates,
//! with bounded in-session delegation.

pub mod account_usage;
pub mod accounts;
pub mod auth;
pub mod candidate;
pub mod classifier;
pub mod codex_logs;
pub mod config;
pub mod credentials;
pub mod delegate_hook;
pub mod delegate_mcp;
pub mod downstream;
pub mod headless;
pub mod headroom;
pub mod lifecycle;
pub mod limits;
pub mod llm_proxy;
pub mod maintenance;
pub mod pre_classifier;
pub mod relay;
pub mod restoration;
pub mod session;
pub mod state;
pub mod state_bench;
pub mod state_layout;
pub mod strategies;
pub mod tickets;
pub mod transport;
pub mod usage;
pub mod usage_cache;
pub mod window_capacity;
pub mod xai_questions;
