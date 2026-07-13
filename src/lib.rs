//! router-acp: an ACP session router over `(agent, model)` candidates,
//! with bounded in-session delegation.

pub mod candidate;
pub mod classifier;
pub mod config;
pub mod delegate_mcp;
pub mod downstream;
pub mod headroom;
pub mod lifecycle;
pub mod limits;
pub mod relay;
pub mod session;
pub mod state;
pub mod strategies;
pub mod transport;
