//! Router-owned OAuth 2.1 for remote MCP servers declared in a project's
//! `.mcp.json`.
//!
//! The router defines no MCP servers. Each session's project (the nearest
//! `.mcp.json` from its cwd up to the git root, the file Claude Code and the
//! Claude ACP adapter already read) names them; this module signs the router
//! in to a remote one once, and the broker (`mcp_broker.rs`) hands that
//! sign-in to every session and agent family that uses the same server.
//!
//! Ported from Kory Code's Datadog connector and generalized to any server
//! that speaks the MCP authorization profile: authorization code + PKCE (S256)
//! as a public client, endpoints from RFC 9728 protected-resource metadata and
//! RFC 8414 / OpenID metadata, dynamic client registration (`client_name`
//! `router-acp`), and an RFC 8707 resource indicator on every grant.
//!
//! State is keyed by the server's normalized URL (its resource indicator), in
//! `~/.config/router-acp/mcp/<host>-<hash>/` (`ROUTER_ACP_MCP_DIR` overrides
//! the root), so one sign-in serves every project that names that server:
//! directory 0700, every file 0600 and written atomically (temp + rename).
//! `client.json` names the client that issued the current refresh token and
//! is written only after a successful exchange; `tokens.json` holds the
//! tokens; `configured` is the durable marker that a sign-in once succeeded,
//! so a lost store reads "reconnect needed" rather than "never signed in";
//! `status.json` records the broker's last live outcome; `flow.json` holds a
//! host-delegated pending sign-in.
//!
//! The router is the only reader and refresher of these files. Token material
//! is never logged, printed, or copied into an error string; error text names
//! HTTP statuses and OAuth error codes only, never a response body.

use std::fs::{File, OpenOptions};
use std::path::{Path, PathBuf};
use std::time::Duration;

use base64::Engine as _;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};

pub const CLIENT_NAME: &str = "router-acp";
/// A started sign-in is usable for ten minutes.
pub const FLOW_TTL_MS: i64 = 10 * 60 * 1000;
/// Refresh an access token this close to its expiry.
pub const REFRESH_WINDOW_MS: i64 = 120 * 1000;
/// A recorded `degraded` outcome stops counting after this long.
const HEALTH_TTL_MS: i64 = 15 * 60 * 1000;
const HTTP_TIMEOUT: Duration = Duration::from_secs(10);
const LOCK_WAIT: Duration = Duration::from_secs(25);

const B64: base64::engine::GeneralPurpose = base64::engine::general_purpose::URL_SAFE_NO_PAD;

pub fn now_ms() -> i64 {
    chrono::Utc::now().timestamp_millis()
}

pub const STORE_ENV: &str = "ROUTER_ACP_MCP_DIR";

/// `ROUTER_ACP_MCP_DIR`, else `~/.config/router-acp/mcp`.
pub fn store_root() -> PathBuf {
    match std::env::var_os(STORE_ENV) {
        Some(dir) if !dir.is_empty() => PathBuf::from(dir),
        _ => crate::config::expand_tilde(Path::new("~/.config/router-acp/mcp")),
    }
}

/// A remote MCP server the router can sign in to.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RemoteServer {
    /// The name the project gives it (sessions see this name).
    pub name: String,
    /// The streamable-HTTP endpoint, query string included.
    pub url: String,
    /// `.mcp.json` `oauth.clientId`: a pre-registered public client.
    pub client_id: Option<String>,
    /// `.mcp.json` `oauth.callbackPort`: a fixed loopback redirect port.
    pub callback_port: Option<u16>,
}

impl RemoteServer {
    pub fn new(name: impl Into<String>, url: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            url: url.into(),
            client_id: None,
            callback_port: None,
        }
    }

    /// The store directory name for this server's URL: `<host>-<hash>` of
    /// its resource indicator, so query variants (`?toolsets=…`) share one
    /// sign-in and different servers never collide.
    pub fn store_key(&self) -> String {
        let resource = resource_indicator(&self.url);
        let normalized = resource.trim_end_matches('/');
        let host = reqwest::Url::parse(normalized)
            .ok()
            .and_then(|u| u.host_str().map(String::from))
            .unwrap_or_else(|| "server".into())
            .chars()
            .map(|c| {
                if c.is_ascii_alphanumeric() || c == '.' || c == '-' {
                    c
                } else {
                    '_'
                }
            })
            .collect::<String>();
        let digest = Sha256::digest(normalized.as_bytes());
        let hash: String = digest[..6].iter().map(|b| format!("{b:02x}")).collect();
        format!("{host}-{hash}")
    }

    pub fn store(&self, root: &Path) -> Store {
        Store::new(root, &self.store_key())
    }
}

// ----------------------------------------------------------------------
// Project `.mcp.json`
// ----------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum Transport {
    Http,
    Sse,
    Stdio,
}

/// One `mcpServers` entry of a project `.mcp.json`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProjectServer {
    pub name: String,
    pub transport: Transport,
    /// The URL with `${VAR}` / `${VAR:-default}` expanded; None when absent
    /// or a variable is unset.
    pub url: Option<String>,
    /// The entry carries its own credential (`headers` / `headersHelper`).
    pub static_auth: bool,
    pub client_id: Option<String>,
    pub callback_port: Option<u16>,
}

impl ProjectServer {
    /// The broker only takes remote streamable-HTTP entries without their own
    /// credential. stdio entries, legacy SSE entries and entries with static
    /// headers stay exactly as they are today: left to the adapters.
    pub fn remote(&self) -> Option<RemoteServer> {
        if self.transport != Transport::Http || self.static_auth {
            return None;
        }
        Some(RemoteServer {
            name: self.name.clone(),
            url: self.url.clone()?,
            client_id: self.client_id.clone(),
            callback_port: self.callback_port,
        })
    }

    /// Why the broker does or does not take this entry, for status output.
    pub fn handling(&self) -> &'static str {
        match (self.transport, self.static_auth, self.url.is_some()) {
            (Transport::Stdio, _, _) => "pass-through (stdio)",
            (Transport::Sse, _, _) => "pass-through (sse)",
            (Transport::Http, true, _) => "pass-through (static auth)",
            (Transport::Http, false, false) => "pass-through (unresolved url)",
            (Transport::Http, false, true) => "brokered",
        }
    }
}

/// The nearest `.mcp.json` from `start` up to the enclosing git root.
pub fn project_file(start: &Path) -> Option<PathBuf> {
    let start = start.canonicalize().unwrap_or_else(|_| start.to_path_buf());
    for dir in start.ancestors() {
        let file = dir.join(".mcp.json");
        if file.is_file() {
            return Some(file);
        }
        if dir.join(".git").exists() {
            return None;
        }
    }
    None
}

/// `${VAR}` and `${VAR:-default}`, as Claude Code expands `.mcp.json`.
fn expand_vars(value: &str) -> Option<String> {
    let mut out = String::new();
    let mut rest = value;
    while let Some(start) = rest.find("${") {
        out.push_str(&rest[..start]);
        let tail = &rest[start + 2..];
        let end = tail.find('}')?;
        let expr = &tail[..end];
        let (var, default) = match expr.split_once(":-") {
            Some((var, default)) => (var, Some(default)),
            None => (expr, None),
        };
        match std::env::var(var).ok().filter(|v| !v.is_empty()) {
            Some(v) => out.push_str(&v),
            None => out.push_str(default?),
        }
        rest = &tail[end + 1..];
    }
    out.push_str(rest);
    Some(out)
}

pub fn parse_project_servers(text: &str) -> Vec<ProjectServer> {
    let Ok(root) = serde_json::from_str::<Value>(text) else {
        return Vec::new();
    };
    let Some(servers) = root.get("mcpServers").and_then(Value::as_object) else {
        return Vec::new();
    };
    let mut out: Vec<ProjectServer> = servers
        .iter()
        .filter_map(|(name, entry)| {
            let entry = entry.as_object()?;
            let url = entry.get("url").and_then(Value::as_str);
            let transport = match entry.get("type").and_then(Value::as_str) {
                Some("http") | Some("streamable-http") => Transport::Http,
                Some("sse") => Transport::Sse,
                Some("stdio") => Transport::Stdio,
                Some(_) => return None,
                None if entry.contains_key("command") => Transport::Stdio,
                None if url.is_some() => Transport::Http,
                None => return None,
            };
            let static_auth = entry
                .get("headers")
                .and_then(Value::as_object)
                .is_some_and(|h| !h.is_empty())
                || entry.contains_key("headersHelper");
            let oauth = entry.get("oauth");
            Some(ProjectServer {
                name: name.clone(),
                transport,
                url: url.and_then(expand_vars),
                static_auth,
                client_id: oauth
                    .and_then(|o| o.get("clientId"))
                    .and_then(Value::as_str)
                    .map(String::from),
                callback_port: oauth
                    .and_then(|o| o.get("callbackPort"))
                    .and_then(Value::as_u64)
                    .and_then(|p| u16::try_from(p).ok()),
            })
        })
        .collect();
    out.sort_by(|a, b| a.name.cmp(&b.name));
    out
}

/// The servers a session in `cwd` is configured with (empty without a file).
pub fn project_servers(cwd: &Path) -> Vec<ProjectServer> {
    project_file(cwd)
        .and_then(|file| std::fs::read_to_string(file).ok())
        .map(|text| parse_project_servers(&text))
        .unwrap_or_default()
}

/// Resolve a CLI target: a raw `--url`, else `name` in the project's file.
pub fn resolve_target(
    project: &Path,
    name: Option<&str>,
    url: Option<&str>,
) -> Result<RemoteServer, String> {
    if let Some(url) = url {
        let parsed = reqwest::Url::parse(url).map_err(|_| "invalid --url".to_string())?;
        let name = name
            .map(String::from)
            .or_else(|| parsed.host_str().map(String::from))
            .unwrap_or_else(|| "server".into());
        return Ok(RemoteServer::new(name, url));
    }
    let name = name.ok_or("name a server from the project's .mcp.json, or pass --url")?;
    let Some(file) = project_file(project) else {
        return Err(format!(
            "no .mcp.json in {} or its parents up to the git root; pass --project or --url",
            project.display()
        ));
    };
    let servers = std::fs::read_to_string(&file)
        .map(|text| parse_project_servers(&text))
        .unwrap_or_default();
    let Some(server) = servers.iter().find(|s| s.name == name) else {
        let known: Vec<_> = servers.iter().map(|s| s.name.as_str()).collect();
        return Err(format!(
            "{} has no MCP server `{name}` (it has: {})",
            file.display(),
            known.join(", ")
        ));
    };
    server.remote().ok_or_else(|| {
        format!(
            "`{name}` in {} is not brokered: {}",
            file.display(),
            server.handling()
        )
    })
}

// ----------------------------------------------------------------------
// Endpoints
// ----------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Endpoints {
    pub authorize: String,
    pub token: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub register: Option<String>,
}

/// The RFC 8707 resource indicator: the MCP URL without its query or
/// fragment (`?toolsets=…` selects tools, it does not name another resource).
pub fn resource_indicator(url: &str) -> String {
    match reqwest::Url::parse(url) {
        Ok(mut parsed) => {
            parsed.set_query(None);
            parsed.set_fragment(None);
            parsed.to_string()
        }
        Err(_) => url.to_string(),
    }
}

/// Datadog's endpoints derived from the MCP host without a network call, the
/// fallback Kory Code relies on when discovery fails. The MCP host is
/// `mcp.<site>`; OAuth lives on the bare sites' `app.<site>` host and on the
/// regional site host itself (`us5.datadoghq.com`). None for other hosts.
pub fn derive_datadog_endpoints(resource_url: &str) -> Option<Endpoints> {
    let url = reqwest::Url::parse(resource_url).ok()?;
    let site = url.host_str()?.strip_prefix("mcp.")?;
    const BARE: [&str; 3] = ["datadoghq.com", "datadoghq.eu", "ddog-gov.com"];
    if !BARE
        .iter()
        .any(|bare| site == *bare || site.ends_with(&format!(".{bare}")))
    {
        return None;
    }
    let app = if BARE.contains(&site) {
        format!("app.{site}")
    } else {
        site.to_string()
    };
    let base = format!("https://{app}");
    Some(Endpoints {
        authorize: format!("{base}/oauth2/v1/authorize"),
        token: format!("{base}/oauth2/v1/token"),
        register: Some(format!("{base}/api/v2/oauth2/register")),
    })
}

/// RFC 8414 §3.1 / RFC 9728 §3.1: the well-known segment goes between the
/// origin and the path; the origin-only form is the common fallback.
fn well_known(url: &reqwest::Url, suffix: &str) -> Vec<String> {
    let origin = url.origin().ascii_serialization();
    let path = url.path().trim_end_matches('/');
    let mut out = Vec::new();
    if !path.is_empty() {
        out.push(format!("{origin}/.well-known/{suffix}{path}"));
    }
    out.push(format!("{origin}/.well-known/{suffix}"));
    out
}

async fn get_json(http: &reqwest::Client, url: &str) -> Option<Value> {
    let response = http
        .get(url)
        .header("Accept", "application/json")
        .timeout(HTTP_TIMEOUT)
        .send()
        .await
        .ok()?;
    if !response.status().is_success() {
        return None;
    }
    response.json().await.ok()
}

/// Discover the OAuth endpoints the way an MCP client does: protected-resource
/// metadata off the MCP URL names the authorization server, whose metadata
/// names the endpoints. None on any failure; never errors.
pub async fn discover_endpoints(http: &reqwest::Client, resource_url: &str) -> Option<Endpoints> {
    let url = reqwest::Url::parse(resource_url).ok()?;
    let mut protected = None;
    for candidate in well_known(&url, "oauth-protected-resource") {
        if let Some(value) = get_json(http, &candidate).await {
            protected = Some(value);
            break;
        }
    }
    let issuer = protected
        .as_ref()
        .and_then(|p| p.pointer("/authorization_servers/0"))
        .and_then(Value::as_str)
        .and_then(|s| reqwest::Url::parse(s).ok())
        .unwrap_or_else(|| {
            reqwest::Url::parse(&url.origin().ascii_serialization()).unwrap_or(url.clone())
        });
    let mut candidates = well_known(&issuer, "oauth-authorization-server");
    candidates.push(format!(
        "{}/.well-known/openid-configuration",
        issuer.as_str().trim_end_matches('/')
    ));
    candidates.extend(well_known(&issuer, "openid-configuration"));
    let mut seen = std::collections::HashSet::new();
    for candidate in candidates {
        if !seen.insert(candidate.clone()) {
            continue;
        }
        let Some(metadata) = get_json(http, &candidate).await else {
            continue;
        };
        let field = |key: &str| metadata.get(key).and_then(Value::as_str).map(String::from);
        if let (Some(authorize), Some(token)) =
            (field("authorization_endpoint"), field("token_endpoint"))
        {
            return Some(Endpoints {
                authorize,
                token,
                register: field("registration_endpoint")
                    .or_else(|| derive_datadog_endpoints(resource_url)?.register),
            });
        }
    }
    None
}

/// Discovery, else the Datadog-derived fallback.
pub async fn resolve_endpoints(
    http: &reqwest::Client,
    server: &RemoteServer,
) -> Result<Endpoints, String> {
    if let Some(endpoints) = discover_endpoints(http, &server.url).await {
        return Ok(endpoints);
    }
    derive_datadog_endpoints(&server.url).ok_or_else(|| {
        format!(
            "could not discover OAuth endpoints for `{}` (no RFC 9728 / RFC 8414 metadata)",
            server.name
        )
    })
}

// ----------------------------------------------------------------------
// PKCE and random values
// ----------------------------------------------------------------------

/// `bytes` of OS randomness, base64url without padding.
pub fn random_token(bytes: usize) -> String {
    let mut buf = vec![0u8; bytes];
    getrandom::getrandom(&mut buf).expect("OS randomness is unavailable");
    B64.encode(buf)
}

pub struct Pkce {
    pub verifier: String,
    pub challenge: String,
}

/// RFC 7636 S256: a 43-character verifier and its SHA-256 challenge.
pub fn pkce() -> Pkce {
    let verifier = random_token(32);
    Pkce {
        challenge: pkce_challenge(&verifier),
        verifier,
    }
}

pub fn pkce_challenge(verifier: &str) -> String {
    B64.encode(Sha256::digest(verifier.as_bytes()))
}

pub(crate) fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    a.len() == b.len() && a.iter().zip(b).fold(0u8, |acc, (x, y)| acc | (x ^ y)) == 0
}

// ----------------------------------------------------------------------
// Store
// ----------------------------------------------------------------------

#[derive(Clone, PartialEq, Serialize, Deserialize)]
pub struct Tokens {
    pub access_token: String,
    #[serde(default)]
    pub refresh_token: Option<String>,
    /// Absolute expiry, milliseconds since the epoch.
    #[serde(default)]
    pub expires_at: Option<i64>,
    /// Set when the authorization server definitively refused this
    /// credential: the server reads "reconnect needed" until a new sign-in.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub refresh_failed_at: Option<i64>,
}

impl std::fmt::Debug for Tokens {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Tokens")
            .field("access_token", &"<redacted>")
            .field(
                "refresh_token",
                &self.refresh_token.as_ref().map(|_| "<redacted>"),
            )
            .field("expires_at", &self.expires_at)
            .field("refresh_failed_at", &self.refresh_failed_at)
            .finish()
    }
}

/// The public client that issued the stored tokens.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ClientRecord {
    /// The project name the server was signed in under (display only).
    pub name: String,
    pub client_id: String,
    pub redirect_uri: String,
    pub resource: String,
    pub endpoints: Endpoints,
}

/// A started sign-in: everything the callback needs. Persisted (0600) only for
/// a host-delegated sign-in whose callback arrives in another process.
#[derive(Clone, Serialize, Deserialize)]
pub struct PendingFlow {
    pub state: String,
    pub verifier: String,
    pub client: ClientRecord,
    pub expires_at: i64,
}

impl std::fmt::Debug for PendingFlow {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PendingFlow")
            .field("client", &self.client)
            .field("expires_at", &self.expires_at)
            .finish_non_exhaustive()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum StatusState {
    SignedIn,
    SignedOut,
    Degraded,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ServerStatus {
    pub state: StatusState,
    pub label: String,
}

impl ServerStatus {
    fn new(state: StatusState, label: &str) -> Self {
        Self {
            state,
            label: label.into(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Health {
    state: StatusState,
    label: String,
    at: i64,
}

/// One server's private directory.
#[derive(Debug, Clone)]
pub struct Store {
    dir: PathBuf,
}

const TOKENS: &str = "tokens.json";
const CLIENT: &str = "client.json";
const CONFIGURED: &str = "configured";
const HEALTH: &str = "status.json";
const FLOW: &str = "flow.json";

impl Store {
    pub fn new(root: &Path, name: &str) -> Self {
        Self {
            dir: root.join(name),
        }
    }

    pub fn dir(&self) -> &Path {
        &self.dir
    }

    fn read<T: serde::de::DeserializeOwned>(&self, name: &str) -> Option<T> {
        let bytes = std::fs::read(self.dir.join(name)).ok()?;
        serde_json::from_slice(&bytes).ok()
    }

    /// Never log the result.
    pub fn tokens(&self) -> Option<Tokens> {
        self.read(TOKENS)
    }

    pub fn client(&self) -> Option<ClientRecord> {
        self.read(CLIENT)
    }

    pub fn flow(&self) -> Option<PendingFlow> {
        self.read(FLOW)
    }

    /// A sign-in once completed here (the marker or either credential file).
    pub fn configured(&self) -> bool {
        [CONFIGURED, CLIENT, TOKENS]
            .iter()
            .any(|name| self.dir.join(name).exists())
    }

    fn ensure_dir(&self) -> Result<(), String> {
        std::fs::create_dir_all(&self.dir).map_err(|_| "MCP credential store unavailable")?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let private = std::fs::Permissions::from_mode(0o700);
            if let Some(root) = self.dir.parent() {
                std::fs::set_permissions(root, private.clone())
                    .map_err(|_| "Could not protect the MCP credential store")?;
            }
            std::fs::set_permissions(&self.dir, private)
                .map_err(|_| "Could not protect the MCP credential store")?;
        }
        Ok(())
    }

    /// Atomic 0600 write: a fresh temp file, synced, then renamed over.
    fn write_private(&self, name: &str, bytes: &[u8]) -> Result<(), String> {
        use std::io::Write;
        self.ensure_dir()?;
        let path = self.dir.join(name);
        let temp = self
            .dir
            .join(format!(".{name}.{}.tmp", uuid::Uuid::new_v4().simple()));
        let mut options = OpenOptions::new();
        options.write(true).create_new(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }
        let result = (|| {
            let mut file = options.open(&temp)?;
            file.write_all(bytes)?;
            file.sync_all()?;
            std::fs::rename(&temp, &path)
        })();
        let _ = std::fs::remove_file(&temp);
        result.map_err(|_| format!("Could not write MCP credential file {name}"))
    }

    fn write_json(&self, name: &str, value: &impl Serialize) -> Result<(), String> {
        let bytes = serde_json::to_vec(value).map_err(|_| "Could not encode MCP credentials")?;
        self.write_private(name, &bytes)
    }

    pub fn write_tokens(&self, tokens: &Tokens) -> Result<(), String> {
        self.write_json(TOKENS, tokens)
    }

    pub fn write_client(&self, client: &ClientRecord) -> Result<(), String> {
        self.write_json(CLIENT, client)
    }

    pub fn write_flow(&self, flow: &PendingFlow) -> Result<(), String> {
        self.write_json(FLOW, flow)
    }

    pub fn clear_flow(&self) {
        let _ = std::fs::remove_file(self.dir.join(FLOW));
    }

    fn mark_configured(&self) -> Result<(), String> {
        self.write_private(CONFIGURED, b"signed-in\n")
    }

    fn health(&self) -> Option<Health> {
        self.read(HEALTH)
    }

    fn write_health(&self, health: &Health) -> Result<(), String> {
        self.write_json(HEALTH, health)
    }

    /// Remove the whole server directory. True when anything was there.
    pub fn remove(&self) -> Result<bool, String> {
        match std::fs::remove_dir_all(&self.dir) {
            Ok(()) => Ok(true),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(false),
            Err(_) => Err("Could not remove the MCP credential store".into()),
        }
    }

    /// The cross-process lock every token write takes: one refresh at a time
    /// across every router process, and waiters re-read the result.
    pub async fn lock(&self) -> Result<File, String> {
        self.ensure_dir()?;
        let mut options = OpenOptions::new();
        options.read(true).write(true).create(true).truncate(false);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }
        let file = options
            .open(self.dir.join(".lock"))
            .map_err(|_| "MCP credential lock unavailable")?;
        tokio::time::timeout(LOCK_WAIT, async {
            loop {
                match file.try_lock() {
                    Ok(()) => return Ok(file),
                    Err(std::fs::TryLockError::WouldBlock) => {
                        tokio::time::sleep(Duration::from_millis(25)).await
                    }
                    Err(_) => return Err("MCP credential lock unavailable".to_string()),
                }
            }
        })
        .await
        .map_err(|_| "Timed out waiting for the MCP credential lock".to_string())?
    }
}

/// signed-in / signed-out / degraded from the store alone (no network), the
/// states Kory Code's Datadog connection reports.
pub fn status(store: &Store) -> ServerStatus {
    status_at(store, now_ms())
}

fn status_at(store: &Store, now: i64) -> ServerStatus {
    if let Err(signed_out) = usable_tokens(store) {
        return ServerStatus::new(StatusState::SignedOut, &signed_out);
    }
    let tokens = store.tokens().expect("usable tokens exist");
    if tokens.refresh_token.is_none() && tokens.expires_at.is_some_and(|at| at <= now) {
        return ServerStatus::new(StatusState::SignedOut, "reconnect needed (token expired)");
    }
    if let Some(health) = store.health()
        && health.state == StatusState::Degraded
        && now - health.at < HEALTH_TTL_MS
    {
        return ServerStatus::new(StatusState::Degraded, &health.label);
    }
    ServerStatus::new(StatusState::SignedIn, "signed in")
}

/// The stored tokens when they may be used; otherwise the signed-out label.
fn usable_tokens(store: &Store) -> Result<Tokens, String> {
    let Some(tokens) = store.tokens() else {
        return Err(if store.configured() {
            "reconnect needed".into()
        } else {
            "not signed in".into()
        });
    };
    if tokens.refresh_failed_at.is_some() {
        return Err("reconnect needed".into());
    }
    Ok(tokens)
}

fn status_row(store: &Store) -> Value {
    let status = status(store);
    let client = store.client();
    json!({"key": store.dir().file_name().map(|n| n.to_string_lossy().into_owned()),
        "name": client.as_ref().map(|c| c.name.clone()),
        "resource": client.as_ref().map(|c| c.resource.clone()),
        "state": status.state, "label": status.label})
}

/// `account-status` rows: every server the router has signed in to.
pub fn status_json(root: &Path) -> Vec<Value> {
    let Ok(entries) = std::fs::read_dir(root) else {
        return Vec::new();
    };
    let mut dirs: Vec<_> = entries
        .filter_map(Result::ok)
        .filter(|e| e.path().is_dir())
        .map(|e| e.file_name().to_string_lossy().into_owned())
        .collect();
    dirs.sort();
    dirs.iter()
        .map(|key| Store::new(root, key))
        .filter(Store::configured)
        .map(|store| status_row(&store))
        .collect()
}

/// `mcp-status`: every server in the project's `.mcp.json`, how the broker
/// handles it, and the router's sign-in state for the brokered ones.
/// With `helper`, a server only the host's credential helper serves reports
/// `signed-in` / "provided credential".
pub async fn project_status_json(
    project: &Path,
    root: &Path,
    helper: Option<&crate::config::McpCredentialCommand>,
) -> Value {
    let file = project_file(project);
    let mut servers = Vec::new();
    for server in project_servers(project) {
        let mut row = json!({"name": server.name, "transport": server.transport,
            "url": server.url, "handling": server.handling()});
        if let Some(remote) = server.remote() {
            let source = TokenSource::new(remote.store(root), reqwest::Client::new())
                .with_supplier(Supplier::for_server(helper, &remote));
            let status = source.effective_status().await;
            row["state"] = json!(status.state);
            row["label"] = status.label.into();
        }
        servers.push(row);
    }
    json!({"project": file.map(|f| f.display().to_string()), "servers": servers})
}

// ----------------------------------------------------------------------
// Sign-in
// ----------------------------------------------------------------------

/// An OAuth error code from a JSON error body, only when it is a plain
/// identifier; never any other part of the body.
fn oauth_error(body: &Value, status: reqwest::StatusCode) -> String {
    match body.get("error").and_then(Value::as_str) {
        Some(code)
            if !code.is_empty()
                && code.len() <= 64
                && code.chars().all(|c| c.is_ascii_alphanumeric() || c == '_') =>
        {
            format!("{code} (HTTP {})", status.as_u16())
        }
        _ => format!("HTTP {}", status.as_u16()),
    }
}

fn tokens_from(body: &Value, previous_refresh: Option<&str>, now: i64) -> Option<Tokens> {
    let access_token = body
        .get("access_token")
        .and_then(Value::as_str)
        .filter(|t| !t.is_empty())?
        .to_string();
    let refresh_token = body
        .get("refresh_token")
        .and_then(Value::as_str)
        .filter(|t| !t.is_empty())
        .or(previous_refresh)
        .map(String::from);
    let expires_at = body
        .get("expires_in")
        .and_then(Value::as_f64)
        .map(|seconds| now + (seconds * 1000.0) as i64);
    Some(Tokens {
        access_token,
        refresh_token,
        expires_at,
        refresh_failed_at: None,
    })
}

async fn register(
    http: &reqwest::Client,
    server: &RemoteServer,
    store: &Store,
    endpoints: &Endpoints,
    redirect_uri: &str,
) -> Result<String, String> {
    let Some(endpoint) = &endpoints.register else {
        return Err(format!(
            "`{}` advertises no client registration endpoint; give its .mcp.json entry \
             an `oauth.clientId`",
            server.name
        ));
    };
    let body = json!({
        "client_name": CLIENT_NAME,
        "redirect_uris": [redirect_uri],
        "grant_types": ["authorization_code", "refresh_token"],
        "response_types": ["code"],
        "token_endpoint_auth_method": "none",
    });
    // Registration runs fresh on every sign-in (servers dedupe it, and a
    // server-side record whose redirect drifted fails authorize while a cache
    // looks fine). The cache is only the fallback when the call itself fails.
    let failure = match http
        .post(endpoint)
        .json(&body)
        .timeout(HTTP_TIMEOUT)
        .send()
        .await
    {
        Ok(response) => {
            let status = response.status();
            let data: Value = response.json().await.unwrap_or(Value::Null);
            match data.get("client_id").and_then(Value::as_str) {
                Some(id) if status.is_success() && !id.is_empty() => return Ok(id.to_string()),
                _ => format!(
                    "client registration failed ({})",
                    oauth_error(&data, status)
                ),
            }
        }
        Err(_) => "client registration endpoint unreachable".to_string(),
    };
    match store.client() {
        Some(cached) if cached.redirect_uri == redirect_uri && cached.endpoints == *endpoints => {
            tracing::warn!(server = server.name, "{failure}; using the cached client");
            Ok(cached.client_id)
        }
        _ => Err(failure),
    }
}

/// Resolve endpoints, register, and build the authorize URL. The returned
/// flow carries its own client so the exchange uses the same client id the
/// authorize URL named; the client is persisted only after that exchange.
pub async fn start_flow(
    http: &reqwest::Client,
    server: &RemoteServer,
    store: &Store,
    redirect_uri: &str,
) -> Result<(String, PendingFlow), String> {
    let endpoints = resolve_endpoints(http, server).await?;
    let client_id = match &server.client_id {
        Some(id) => id.clone(),
        None => register(http, server, store, &endpoints, redirect_uri).await?,
    };
    let client = ClientRecord {
        name: server.name.clone(),
        client_id,
        redirect_uri: redirect_uri.to_string(),
        resource: resource_indicator(&server.url),
        endpoints,
    };
    let pkce = pkce();
    let state = random_token(16);
    let mut url = reqwest::Url::parse(&client.endpoints.authorize)
        .map_err(|_| format!("`{}`: invalid authorization endpoint", server.name))?;
    {
        let mut query = url.query_pairs_mut();
        query
            .append_pair("response_type", "code")
            .append_pair("client_id", &client.client_id)
            .append_pair("redirect_uri", &client.redirect_uri)
            .append_pair("code_challenge", &pkce.challenge)
            .append_pair("code_challenge_method", "S256")
            .append_pair("state", &state)
            .append_pair("resource", &client.resource);
    }
    let flow = PendingFlow {
        state,
        verifier: pkce.verifier,
        client,
        expires_at: now_ms() + FLOW_TTL_MS,
    };
    Ok((url.to_string(), flow))
}

/// Exchange the authorization code with the held verifier and persist the
/// client and tokens. The state check is the CSRF defense.
pub async fn finish_flow(
    http: &reqwest::Client,
    store: &Store,
    flow: &PendingFlow,
    code: &str,
    state: &str,
) -> Result<(), String> {
    if now_ms() > flow.expires_at {
        return Err("this sign-in expired; start it again".into());
    }
    if code.is_empty() || !constant_time_eq(state.as_bytes(), flow.state.as_bytes()) {
        return Err("this sign-in link is stale or invalid; start it again".into());
    }
    let client = &flow.client;
    let response = http
        .post(&client.endpoints.token)
        .form(&[
            ("grant_type", "authorization_code"),
            ("code", code),
            ("redirect_uri", &client.redirect_uri),
            ("client_id", &client.client_id),
            ("code_verifier", &flow.verifier),
            ("resource", &client.resource),
        ])
        .timeout(HTTP_TIMEOUT)
        .send()
        .await
        .map_err(|_| "token endpoint unreachable".to_string())?;
    let status = response.status();
    let body: Value = response.json().await.unwrap_or(Value::Null);
    let tokens = match tokens_from(&body, None, now_ms()) {
        Some(tokens) if status.is_success() => tokens,
        _ => {
            return Err(format!(
                "token exchange failed ({})",
                oauth_error(&body, status)
            ));
        }
    };
    let _lock = store.lock().await?;
    // The exchange succeeded under this client: now it may own client.json
    // (the new refresh token is bound to it).
    store.write_client(client)?;
    store.write_tokens(&tokens)?;
    store.mark_configured()?;
    let _ = std::fs::remove_file(store.dir().join(HEALTH));
    store.clear_flow();
    Ok(())
}

// ----------------------------------------------------------------------
// Token source (the broker's view)
// ----------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AccessError {
    /// Needs a human sign-in: never signed in, or definitively refused.
    SignedOut(String),
    /// Transient: the token endpoint or lock is unavailable.
    Unavailable(String),
}

enum Refresh {
    Renewed(String),
    Rejected,
    Transient(String),
}

/// The host's `mcp_credential_command` for one server: a bearer when the
/// router has no sign-in of its own. Held in memory only, never on disk.
pub struct Supplier {
    helper: crate::config::McpCredentialCommand,
    request: Value,
    /// The last answer and when it lapses: `Some(token)` or a decline.
    cached: tokio::sync::Mutex<Option<(Option<String>, i64)>>,
}

/// A supplied token is reused until this long before its `expiresAt`.
const SUPPLIED_HEADROOM_MS: i64 = 60_000;
/// Reuse for a supplied token without `expiresAt`.
const SUPPLIED_TTL_MS: i64 = 5 * 60_000;
/// A decline (or a failed helper) is asked again after this long.
const DECLINE_TTL_MS: i64 = 60_000;

impl Supplier {
    /// The helper for `server`, or None when its `servers` list leaves it out.
    pub fn for_server(
        helper: Option<&crate::config::McpCredentialCommand>,
        server: &RemoteServer,
    ) -> Option<Self> {
        helper
            .filter(|h| h.covers(&server.name))
            .map(|h| Self::new(h.clone(), server))
    }

    pub fn new(helper: crate::config::McpCredentialCommand, server: &RemoteServer) -> Self {
        Self {
            helper,
            request: json!({"name": server.name, "url": server.url,
                "resource": resource_indicator(&server.url)}),
            cached: tokio::sync::Mutex::new(None),
        }
    }

    /// The helper's bearer, or None when it declines. `rejected` is a token
    /// the upstream just refused: it is dropped and the helper asked once more.
    pub async fn token(&self, rejected: Option<&str>) -> Option<String> {
        let mut cached = self.cached.lock().await;
        if let Some((answer, until)) = cached.as_ref() {
            let refused = rejected.is_some() && answer.as_deref() == rejected;
            if !refused && *until > now_ms() {
                return answer.clone();
            }
        }
        let answer = self.run().await;
        let until = match &answer {
            Some((_, Some(expires_at))) => expires_at - SUPPLIED_HEADROOM_MS,
            Some((_, None)) => now_ms() + SUPPLIED_TTL_MS,
            None => now_ms() + DECLINE_TTL_MS,
        };
        let token = answer.map(|(token, _)| token);
        *cached = Some((token.clone(), until));
        token
    }

    async fn run(&self) -> Option<(String, Option<i64>)> {
        use tokio::io::AsyncWriteExt;
        let name = self.request["name"]
            .as_str()
            .unwrap_or_default()
            .to_string();
        let mut child = tokio::process::Command::new(&self.helper.command)
            .args(&self.helper.args)
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::null())
            .kill_on_drop(true)
            .spawn()
            .map_err(|e| tracing::warn!(server = name, "MCP credential helper did not start: {e}"))
            .ok()?;
        if let Some(mut stdin) = child.stdin.take() {
            let _ = stdin.write_all(self.request.to_string().as_bytes()).await;
        }
        let timeout = Duration::from_millis(self.helper.timeout_ms);
        let output = match tokio::time::timeout(timeout, child.wait_with_output()).await {
            Ok(Ok(output)) => output,
            Ok(Err(e)) => {
                tracing::warn!(server = name, "MCP credential helper failed: {e}");
                return None;
            }
            Err(_) => {
                tracing::warn!(server = name, "MCP credential helper timed out");
                return None;
            }
        };
        if !output.status.success() {
            return None;
        }
        let answer: Value = serde_json::from_slice(&output.stdout).ok()?;
        let token = answer.get("token")?.as_str()?.trim();
        if token.is_empty() {
            return None;
        }
        Some((
            token.to_string(),
            answer.get("expiresAt").and_then(Value::as_i64),
        ))
    }
}

/// The broker's one reader and refresher of a server's credentials.
pub struct TokenSource {
    /// The store key, for logs.
    pub name: String,
    pub store: Store,
    http: reqwest::Client,
    refreshing: tokio::sync::Mutex<()>,
    last_health: std::sync::Mutex<Option<(StatusState, String)>>,
    /// Consulted only when the router has no usable sign-in of its own.
    supplier: Option<Supplier>,
}

impl TokenSource {
    pub fn new(store: Store, http: reqwest::Client) -> Self {
        Self {
            name: store
                .dir()
                .file_name()
                .map(|n| n.to_string_lossy().into_owned())
                .unwrap_or_default(),
            store,
            http,
            refreshing: tokio::sync::Mutex::new(()),
            last_health: std::sync::Mutex::new(None),
            supplier: None,
        }
    }

    pub fn with_supplier(mut self, supplier: Option<Supplier>) -> Self {
        self.supplier = supplier;
        self
    }

    /// Sessions receive this server unless it needs a human sign-in. With a
    /// host credential helper they always do: the helper answers at request
    /// time, and a decline reaches the agent as the sign-in message.
    pub fn injectable(&self) -> bool {
        usable_tokens(&self.store).is_ok() || self.supplier.is_some()
    }

    async fn supplied(&self, rejected: Option<&str>) -> Option<String> {
        match &self.supplier {
            Some(supplier) => supplier.token(rejected).await,
            None => None,
        }
    }

    /// The router's own state, or `signed-in` / "provided credential" when only
    /// the host's helper supplies a bearer. The router's sign-in always wins.
    pub async fn effective_status(&self) -> ServerStatus {
        let own = self.status();
        if own.state == StatusState::SignedOut && self.supplied(None).await.is_some() {
            return ServerStatus::new(StatusState::SignedIn, "provided credential");
        }
        own
    }

    pub fn status(&self) -> ServerStatus {
        status(&self.store)
    }

    /// A current access token, refreshed within `REFRESH_WINDOW_MS` of expiry.
    /// `force` refreshes regardless (the upstream refused the bearer). With
    /// `rejected`, a replacement another request already stored is the
    /// recovery and is never rotated again.
    pub async fn access_token(
        &self,
        force: bool,
        rejected: Option<&str>,
    ) -> Result<String, AccessError> {
        let tokens = match usable_tokens(&self.store) {
            Ok(tokens) => tokens,
            Err(why) => {
                return self
                    .supplied(rejected)
                    .await
                    .ok_or(AccessError::SignedOut(why));
            }
        };
        if rejected.is_some_and(|r| r != tokens.access_token) {
            return Ok(tokens.access_token);
        }
        let remaining = tokens.expires_at.map(|at| at - now_ms());
        if !force && remaining.is_none_or(|left| left > REFRESH_WINDOW_MS) {
            return Ok(tokens.access_token);
        }
        let _single = self.refreshing.lock().await;
        let _file = self.store.lock().await.map_err(AccessError::Unavailable)?;
        // Another request, or another router process, may have refreshed
        // while this one waited: reuse its result.
        let current = usable_tokens(&self.store).map_err(AccessError::SignedOut)?;
        if current.access_token != tokens.access_token {
            return Ok(current.access_token);
        }
        match self.refresh(&current).await {
            Refresh::Renewed(token) => Ok(token),
            Refresh::Rejected => {
                self.mark_failed(&current);
                match self.supplied(None).await {
                    Some(token) => Ok(token),
                    None => Err(AccessError::SignedOut("reconnect needed".into())),
                }
            }
            // Headroom never costs a working credential: a token that is
            // short but still live is better than none. A bearer the server
            // just refused is not.
            Refresh::Transient(why) if !force && remaining.is_some_and(|left| left > 0) => {
                tracing::warn!(server = self.name, "MCP token refresh failed: {why}");
                Ok(current.access_token)
            }
            Refresh::Transient(why) => Err(AccessError::Unavailable(why)),
        }
    }

    async fn refresh(&self, tokens: &Tokens) -> Refresh {
        let (Some(refresh_token), Some(client)) = (&tokens.refresh_token, self.store.client())
        else {
            return Refresh::Rejected;
        };
        let response = match self
            .http
            .post(&client.endpoints.token)
            .form(&[
                ("grant_type", "refresh_token"),
                ("refresh_token", refresh_token.as_str()),
                ("client_id", client.client_id.as_str()),
                ("resource", client.resource.as_str()),
            ])
            .timeout(HTTP_TIMEOUT)
            .send()
            .await
        {
            Ok(response) => response,
            Err(_) => return Refresh::Transient("token endpoint unreachable".into()),
        };
        let status = response.status();
        let body: Value = response.json().await.unwrap_or(Value::Null);
        if !status.is_success() {
            // Only a client error is a durable refusal. Network, rate-limit
            // and server failures retry on the next request.
            let code = status.as_u16();
            return if (400..500).contains(&code) && code != 408 && code != 429 {
                tracing::warn!(
                    server = self.name,
                    "MCP token refresh refused ({})",
                    oauth_error(&body, status)
                );
                Refresh::Rejected
            } else {
                Refresh::Transient(format!("token refresh {}", oauth_error(&body, status)))
            };
        }
        let Some(next) = tokens_from(&body, Some(refresh_token), now_ms()) else {
            return Refresh::Transient("token refresh returned no access token".into());
        };
        match self.store.write_tokens(&next) {
            Ok(()) => Refresh::Renewed(next.access_token),
            Err(why) => Refresh::Transient(why),
        }
    }

    fn mark_failed(&self, tokens: &Tokens) {
        let mut marked = tokens.clone();
        marked.refresh_failed_at = Some(now_ms());
        if let Err(why) = self.store.write_tokens(&marked) {
            tracing::warn!(server = self.name, "{why}");
        }
    }

    /// The upstream refused a freshly refreshed bearer too: reconnect needed.
    /// Only the bearer that was refused is marked, never a newer one.
    pub async fn mark_access_rejected(&self, token: &str) {
        let Ok(_file) = self.store.lock().await else {
            return;
        };
        if let Some(current) = self.store.tokens()
            && current.access_token == token
        {
            self.mark_failed(&current);
        }
    }

    /// Record the live outcome for `account-status`, writing only on change.
    pub fn note(&self, state: StatusState, label: &str) {
        let mut last = self.last_health.lock().unwrap();
        if last
            .as_ref()
            .is_some_and(|(s, l)| *s == state && l == label)
        {
            return;
        }
        *last = Some((state, label.to_string()));
        drop(last);
        let health = Health {
            state,
            label: label.to_string(),
            at: now_ms(),
        };
        if let Err(why) = self.store.write_health(&health) {
            tracing::debug!(server = self.name, "{why}");
        }
    }
}

// ----------------------------------------------------------------------
// CLI: mcp-login / mcp-logout
// ----------------------------------------------------------------------

/// Only an identifier-shaped OAuth `error` from a callback is ever echoed.
fn safe_code(value: &str) -> &str {
    if !value.is_empty()
        && value.len() <= 64
        && value.chars().all(|c| c.is_ascii_alphanumeric() || c == '_')
    {
        value
    } else {
        "error"
    }
}

fn open_browser(url: &str) {
    let opener = if cfg!(target_os = "macos") {
        "open"
    } else {
        "xdg-open"
    };
    let _ = std::process::Command::new(opener)
        .arg(url)
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .spawn();
}

const CALLBACK_PAGE: &str = "<!doctype html><html lang=\"en\"><head><meta charset=\"utf-8\">\
<title>router-acp</title></head><body style=\"font:16px system-ui;padding:2em\">\
<p>MESSAGE</p></body></html>";

/// Verify a fresh sign-in with an MCP `tools/list`. Tokens are kept either
/// way: verification is an availability check, not part of the credential.
async fn verify(http: &reqwest::Client, server: &RemoteServer, store: &Store) -> String {
    let Some(tokens) = store.tokens() else {
        return format!("signed in to `{}`", server.name);
    };
    match crate::mcp_broker::probe_tools(http, &server.url, &tokens.access_token).await {
        Ok(count) => format!("signed in to `{}` ({count} tools)", server.name),
        Err(why) => format!(
            "signed in to `{}`; the MCP check failed ({why}), tokens kept",
            server.name
        ),
    }
}

/// Interactive sign-in through a loopback redirect (RFC 8252 §7.3). The
/// redirect is `http://localhost:<port>/callback`, the form Claude Code's own
/// MCP sign-in registers, served on both 127.0.0.1 and ::1.
pub async fn login_interactive(server: &RemoteServer, browser: bool) -> Result<String, String> {
    let name = &server.name;
    let store = server.store(&store_root());
    let http = reqwest::Client::new();
    let listener = tokio::net::TcpListener::bind(("127.0.0.1", server.callback_port.unwrap_or(0)))
        .await
        .map_err(|e| format!("cannot listen for the sign-in redirect: {e}"))?;
    let port = listener
        .local_addr()
        .map_err(|e| format!("cannot read the redirect address: {e}"))?
        .port();
    let listener_v6 = tokio::net::TcpListener::bind(("::1", port)).await.ok();
    let redirect = format!("http://localhost:{port}/callback");
    let (url, flow) = start_flow(&http, server, &store, &redirect).await?;
    eprintln!("Sign in to `{name}` in your browser:\n\n  {url}\n");
    if browser {
        open_browser(&url);
    }

    type Callback = (Option<String>, Option<String>, Option<String>);
    let (tx, rx) = tokio::sync::oneshot::channel::<Callback>();
    let tx = std::sync::Arc::new(std::sync::Mutex::new(Some(tx)));
    let app = axum::Router::new().route(
        "/callback",
        axum::routing::get(
            move |axum::extract::Query(q): axum::extract::Query<
                std::collections::HashMap<String, String>,
            >| {
                let tx = tx.clone();
                async move {
                    let (code, state, error) = (
                        q.get("code").cloned(),
                        q.get("state").cloned(),
                        q.get("error").cloned(),
                    );
                    if code.is_none() && error.is_none() {
                        return axum::response::Html(
                            CALLBACK_PAGE.replace("MESSAGE", "Waiting for sign-in."),
                        );
                    }
                    let message = if error.is_some() {
                        "Sign-in was not completed. You can close this tab."
                    } else {
                        "Signed in. You can close this tab and return to the terminal."
                    };
                    if let Some(tx) = tx.lock().unwrap().take() {
                        let _ = tx.send((code, state, error));
                    }
                    axum::response::Html(CALLBACK_PAGE.replace("MESSAGE", message))
                }
            },
        ),
    );
    let stop = tokio_util::sync::CancellationToken::new();
    let mut tasks = Vec::new();
    for listener in std::iter::once(listener).chain(listener_v6) {
        let (app, stop) = (app.clone(), stop.clone());
        tasks.push(tokio::spawn(async move {
            let _ = axum::serve(listener, app)
                .with_graceful_shutdown(stop.cancelled_owned())
                .await;
        }));
    }
    let received = tokio::time::timeout(Duration::from_millis(FLOW_TTL_MS as u64), rx).await;
    stop.cancel();
    for task in tasks {
        let _ = task.await;
    }
    let (code, state, error) = received
        .map_err(|_| "timed out waiting for the browser sign-in".to_string())?
        .map_err(|_| "the sign-in listener stopped".to_string())?;
    if let Some(error) = error {
        return Err(format!("authorization was refused ({})", safe_code(&error)));
    }
    finish_flow(
        &http,
        &store,
        &flow,
        code.as_deref().unwrap_or_default(),
        state.as_deref().unwrap_or_default(),
    )
    .await?;
    Ok(verify(&http, server, &store).await)
}

/// A loopback listener for one sign-in redirect: the redirect URI and the
/// full URL the browser lands on. It stops after one callback, or when the
/// flow expires.
pub async fn loopback_redirect(
    port: Option<u16>,
) -> Result<(String, tokio::sync::oneshot::Receiver<String>), String> {
    let listener = tokio::net::TcpListener::bind(("127.0.0.1", port.unwrap_or(0)))
        .await
        .map_err(|e| format!("cannot listen for the sign-in redirect: {e}"))?;
    let port = listener
        .local_addr()
        .map_err(|e| format!("cannot read the redirect address: {e}"))?
        .port();
    let redirect = format!("http://localhost:{port}/callback");
    let (tx, rx) = tokio::sync::oneshot::channel::<String>();
    let tx = std::sync::Arc::new(std::sync::Mutex::new(Some(tx)));
    let stop = tokio_util::sync::CancellationToken::new();
    let base = redirect.clone();
    let done = stop.clone();
    let app = axum::Router::new().route(
        "/callback",
        axum::routing::get(
            move |axum::extract::RawQuery(query): axum::extract::RawQuery| {
                let (tx, base, done) = (tx.clone(), base.clone(), done.clone());
                async move {
                    let query = query.unwrap_or_default();
                    if !query.contains("code=") && !query.contains("error=") {
                        return axum::response::Html(
                            CALLBACK_PAGE.replace("MESSAGE", "Waiting for sign-in."),
                        );
                    }
                    if let Some(tx) = tx.lock().unwrap().take() {
                        let _ = tx.send(format!("{base}?{query}"));
                    }
                    done.cancel();
                    axum::response::Html(CALLBACK_PAGE.replace(
                        "MESSAGE",
                        "Sign-in received. You can close this tab and return to your session.",
                    ))
                }
            },
        ),
    );
    tokio::spawn(async move {
        let expiry = tokio::time::sleep(Duration::from_millis(FLOW_TTL_MS as u64));
        let _ = axum::serve(listener, app)
            .with_graceful_shutdown(async move {
                tokio::select! { () = stop.cancelled() => {}, () = expiry => {} }
            })
            .await;
    });
    Ok((redirect, rx))
}

/// Host-delegated sign-in, step one: the host owns the redirect (a public
/// callback URL). Persists the pending flow (0600) and returns `{url, expiresAt}`.
pub async fn login_start(server: &RemoteServer, redirect_uri: &str) -> Result<Value, String> {
    let store = server.store(&store_root());
    let (url, flow) = start_flow(&reqwest::Client::new(), server, &store, redirect_uri).await?;
    store.write_flow(&flow)?;
    Ok(
        json!({"url": url, "expiresAt": chrono::DateTime::from_timestamp_millis(flow.expires_at)
        .map(|t| t.to_rfc3339())}),
    )
}

/// Host-delegated sign-in, step two: the full redirect URL the browser landed
/// on. Returns the server's status row.
pub async fn login_finish(server: &RemoteServer, callback_url: &str) -> Result<Value, String> {
    let store = server.store(&store_root());
    let flow = store
        .flow()
        .ok_or("no sign-in is pending for this server; start one with --redirect-uri")?;
    let url = reqwest::Url::parse(callback_url).map_err(|_| "invalid callback URL".to_string())?;
    let query: std::collections::HashMap<_, _> = url.query_pairs().into_owned().collect();
    if let Some(error) = query.get("error") {
        store.clear_flow();
        return Err(format!("authorization was refused ({})", safe_code(error)));
    }
    let http = reqwest::Client::new();
    finish_flow(
        &http,
        &store,
        &flow,
        query.get("code").map(String::as_str).unwrap_or_default(),
        query.get("state").map(String::as_str).unwrap_or_default(),
    )
    .await?;
    let message = verify(&http, server, &store).await;
    let status = status(&store);
    Ok(
        json!({"name": server.name, "key": server.store_key(), "state": status.state,
        "label": status.label, "message": message}),
    )
}

/// Forget a server's client and tokens (local only; nothing is revoked).
pub fn logout(server: &RemoteServer) -> Result<String, String> {
    let name = &server.name;
    Ok(if server.store(&store_root()).remove()? {
        format!("signed out of `{name}`")
    } else {
        format!("`{name}` was not signed in")
    })
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use axum::{Router, extract::State, routing::get, routing::post};
    use std::sync::{Arc, Mutex};

    #[tokio::test]
    async fn the_loopback_redirect_hands_back_the_full_landing_url_once() {
        let (redirect, received) = loopback_redirect(None).await.unwrap();
        assert!(redirect.starts_with("http://localhost:") && redirect.ends_with("/callback"));
        let port = redirect
            .trim_start_matches("http://localhost:")
            .trim_end_matches("/callback");
        let base = format!("http://127.0.0.1:{port}/callback");
        // A bare visit (a prefetch) is not the redirect.
        reqwest::get(&base).await.unwrap();
        reqwest::get(format!("{base}?code=abc&state=xyz"))
            .await
            .unwrap();
        assert_eq!(
            received.await.unwrap(),
            format!("{redirect}?code=abc&state=xyz")
        );
    }

    pub(crate) fn server(name: &str, url: &str) -> RemoteServer {
        RemoteServer::new(name, url)
    }

    pub(crate) async fn serve(app: Router) -> String {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        format!("http://{addr}")
    }

    /// A store already signed in to `server` with these tokens.
    pub(crate) fn signed_in(
        root: &Path,
        server: &RemoteServer,
        token_url: &str,
        tokens: &Tokens,
    ) -> Store {
        let store = server.store(root);
        store
            .write_client(&ClientRecord {
                name: server.name.clone(),
                client_id: "client-1".into(),
                redirect_uri: "http://127.0.0.1:1/callback".into(),
                resource: resource_indicator(&server.url),
                endpoints: Endpoints {
                    authorize: "http://127.0.0.1:1/authorize".into(),
                    token: token_url.into(),
                    register: None,
                },
            })
            .unwrap();
        store.write_tokens(tokens).unwrap();
        store.mark_configured().unwrap();
        store
    }

    pub(crate) fn tokens(access: &str, expires_in_ms: i64) -> Tokens {
        Tokens {
            access_token: access.into(),
            refresh_token: Some("refresh-1".into()),
            expires_at: Some(now_ms() + expires_in_ms),
            refresh_failed_at: None,
        }
    }

    #[test]
    fn datadog_endpoints_follow_bare_and_regional_sites() {
        let us1 =
            derive_datadog_endpoints("https://mcp.datadoghq.com/v1/mcp?toolsets=core").unwrap();
        assert_eq!(
            us1.authorize,
            "https://app.datadoghq.com/oauth2/v1/authorize"
        );
        assert_eq!(us1.token, "https://app.datadoghq.com/oauth2/v1/token");
        assert_eq!(
            us1.register.as_deref(),
            Some("https://app.datadoghq.com/api/v2/oauth2/register")
        );
        let us5 = derive_datadog_endpoints("https://mcp.us5.datadoghq.com/v1/mcp").unwrap();
        assert_eq!(us5.token, "https://us5.datadoghq.com/oauth2/v1/token");
        let eu = derive_datadog_endpoints("https://mcp.datadoghq.eu/v1/mcp").unwrap();
        assert_eq!(eu.authorize, "https://app.datadoghq.eu/oauth2/v1/authorize");
        assert!(derive_datadog_endpoints("https://mcp.buildkite.com/mcp/readonly").is_none());
        assert!(derive_datadog_endpoints("https://mcp.notdatadoghq.com/v1/mcp").is_none());
    }

    #[test]
    fn one_store_per_server_url_shared_across_query_variants_and_names() {
        let a = server(
            "datadog",
            "https://mcp.datadoghq.com/v1/mcp?toolsets=core,dbm,error-tracking,rum",
        );
        assert_eq!(
            resource_indicator(&a.url),
            "https://mcp.datadoghq.com/v1/mcp"
        );
        let b = server("dd", "https://MCP.datadoghq.com/v1/mcp/");
        assert_eq!(a.store_key(), b.store_key());
        assert!(
            a.store_key().starts_with("mcp.datadoghq.com-"),
            "{}",
            a.store_key()
        );
        let us5 = server("datadog", "https://mcp.us5.datadoghq.com/v1/mcp");
        assert_ne!(a.store_key(), us5.store_key());
        let buildkite = server("buildkite", "https://mcp.buildkite.com/mcp/readonly");
        assert_ne!(
            buildkite.store_key(),
            server("x", "https://mcp.buildkite.com/mcp").store_key()
        );
    }

    #[test]
    fn project_file_entries_are_classified_like_claude_reads_them() {
        // SAFETY: test-unique variable name.
        unsafe { std::env::set_var("ROUTER_ACP_TEST_DD_SITE", "us5.datadoghq.com") };
        let servers = parse_project_servers(
            r#"{"mcpServers": {
                "datadog": {"type": "http", "url": "https://mcp.${ROUTER_ACP_TEST_DD_SITE}/v1/mcp"},
                "buildkite": {"type": "http", "url": "https://mcp.buildkite.com/mcp/readonly",
                    "oauth": {"clientId": "pre-registered", "callbackPort": 8765}},
                "keyed": {"type": "http", "url": "https://k.test/mcp", "headers": {"Authorization": "Bearer x"}},
                "legacy": {"type": "sse", "url": "https://s.test/sse"},
                "local": {"command": "uvx", "args": ["thing"]},
                "unset": {"url": "https://${ROUTER_ACP_TEST_UNSET_VAR}/mcp"},
                "defaulted": {"url": "https://${ROUTER_ACP_TEST_UNSET_VAR:-d.test}/mcp"}
            }}"#,
        );
        let handling: Vec<_> = servers
            .iter()
            .map(|s| (s.name.as_str(), s.handling()))
            .collect();
        assert_eq!(
            handling,
            vec![
                ("buildkite", "brokered"),
                ("datadog", "brokered"),
                ("defaulted", "brokered"),
                ("keyed", "pass-through (static auth)"),
                ("legacy", "pass-through (sse)"),
                ("local", "pass-through (stdio)"),
                ("unset", "pass-through (unresolved url)"),
            ]
        );
        let datadog = servers[1].remote().unwrap();
        assert_eq!(datadog.url, "https://mcp.us5.datadoghq.com/v1/mcp");
        let buildkite = servers[0].remote().unwrap();
        assert_eq!(buildkite.client_id.as_deref(), Some("pre-registered"));
        assert_eq!(buildkite.callback_port, Some(8765));
        assert_eq!(servers[2].url.as_deref(), Some("https://d.test/mcp"));
    }

    #[test]
    fn the_project_file_is_the_nearest_one_up_to_the_git_root() {
        let root = tempfile::tempdir().unwrap();
        let repo = root.path().join("repo");
        std::fs::create_dir_all(repo.join(".git")).unwrap();
        std::fs::create_dir_all(repo.join("src/deep")).unwrap();
        std::fs::write(root.path().join(".mcp.json"), "{}").unwrap();
        assert_eq!(
            project_file(&repo.join("src/deep")),
            None,
            "never above the git root"
        );
        std::fs::write(
            repo.join(".mcp.json"),
            r#"{"mcpServers":{"a":{"type":"http","url":"https://a.test/mcp"}}}"#,
        )
        .unwrap();
        let found = project_file(&repo.join("src/deep")).unwrap();
        assert_eq!(found, repo.canonicalize().unwrap().join(".mcp.json"));
        assert_eq!(project_servers(&repo.join("src")).len(), 1);
        let target = resolve_target(&repo, Some("a"), None).unwrap();
        assert_eq!(target.url, "https://a.test/mcp");
        assert!(
            resolve_target(&repo, Some("b"), None)
                .unwrap_err()
                .contains("has no MCP server")
        );
        let raw = resolve_target(&repo, None, Some("https://raw.test/mcp")).unwrap();
        assert_eq!(raw.name, "raw.test");
    }

    #[test]
    fn well_known_inserts_between_origin_and_path() {
        let url = reqwest::Url::parse("https://mcp.buildkite.com/mcp/readonly").unwrap();
        assert_eq!(
            well_known(&url, "oauth-protected-resource"),
            vec![
                "https://mcp.buildkite.com/.well-known/oauth-protected-resource/mcp/readonly",
                "https://mcp.buildkite.com/.well-known/oauth-protected-resource",
            ]
        );
        let bare = reqwest::Url::parse("https://auth.example.test").unwrap();
        assert_eq!(
            well_known(&bare, "oauth-authorization-server"),
            vec!["https://auth.example.test/.well-known/oauth-authorization-server"]
        );
    }

    #[test]
    fn pkce_is_s256_of_a_43_character_verifier() {
        let pkce = pkce();
        assert_eq!(pkce.verifier.len(), 43);
        assert!(
            pkce.verifier
                .chars()
                .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_')
        );
        // Expected value computed independently:
        // printf %s <verifier> | openssl dgst -sha256 -binary | base64url
        assert_eq!(
            pkce_challenge("dBjftJeZ4CVP-mB92K27uhbUJU59-1Lv2WzoM8ThGI"),
            "MHYv10oMmjhPCRCye8Hb20JJIg7M4WlF7JCgts9JUyI"
        );
        assert_ne!(pkce.verifier, super::pkce().verifier);
    }

    #[tokio::test]
    async fn discovery_follows_protected_resource_to_authorization_server_metadata() {
        let base = Arc::new(Mutex::new(String::new()));
        let app = Router::new()
            .route(
                "/.well-known/oauth-protected-resource/mcp",
                get(|State(base): State<Arc<Mutex<String>>>| async move {
                    let base = base.lock().unwrap().clone();
                    axum::Json(json!({"authorization_servers": [format!("{base}/tenant")]}))
                }),
            )
            .route(
                "/.well-known/oauth-authorization-server/tenant",
                get(|State(base): State<Arc<Mutex<String>>>| async move {
                    let base = base.lock().unwrap().clone();
                    axum::Json(json!({
                        "authorization_endpoint": format!("{base}/authorize"),
                        "token_endpoint": format!("{base}/token"),
                        "registration_endpoint": format!("{base}/register"),
                    }))
                }),
            )
            .with_state(base.clone());
        let url = serve(app).await;
        *base.lock().unwrap() = url.clone();
        let http = reqwest::Client::new();
        let found = discover_endpoints(&http, &format!("{url}/mcp"))
            .await
            .unwrap();
        assert_eq!(found.authorize, format!("{url}/authorize"));
        assert_eq!(found.token, format!("{url}/token"));
        assert_eq!(found.register, Some(format!("{url}/register")));
        // Nothing to discover: None, and resolution says so.
        let empty = serve(Router::new()).await;
        assert!(
            discover_endpoints(&http, &format!("{empty}/mcp"))
                .await
                .is_none()
        );
        let err = resolve_endpoints(&http, &server("x", &format!("{empty}/mcp")))
            .await
            .unwrap_err();
        assert!(err.contains("could not discover"), "{err}");
    }

    #[cfg(unix)]
    #[test]
    fn the_store_is_private_and_atomic() {
        use std::os::unix::fs::PermissionsExt;
        let root = tempfile::tempdir().unwrap();
        let store = Store::new(&root.path().join("mcp"), "datadog");
        assert!(!store.configured());
        store.write_tokens(&tokens("a", 60_000)).unwrap();
        store.write_tokens(&tokens("b", 60_000)).unwrap();
        let mode = |p: &Path| std::fs::metadata(p).unwrap().permissions().mode() & 0o777;
        assert_eq!(mode(store.dir()), 0o700);
        assert_eq!(mode(&root.path().join("mcp")), 0o700);
        assert_eq!(mode(&store.dir().join(TOKENS)), 0o600);
        assert_eq!(store.tokens().unwrap().access_token, "b");
        let names: Vec<_> = std::fs::read_dir(store.dir())
            .unwrap()
            .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
            .collect();
        assert_eq!(names, vec![TOKENS.to_string()], "no temp file left behind");
        assert!(store.configured());
        assert!(!format!("{:?}", store.tokens().unwrap()).contains("refresh-1"));
        assert!(store.remove().unwrap());
        assert!(!store.remove().unwrap());
    }

    #[test]
    fn status_reports_the_three_states() {
        let root = tempfile::tempdir().unwrap();
        let s = server("datadog", "https://mcp.datadoghq.com/v1/mcp");
        assert_eq!(
            status(&s.store(root.path())),
            ServerStatus::new(StatusState::SignedOut, "not signed in")
        );
        assert!(status_json(root.path()).is_empty());
        let store = signed_in(
            root.path(),
            &s,
            "http://127.0.0.1:1/token",
            &tokens("a", 60_000),
        );
        assert_eq!(status(&store).state, StatusState::SignedIn);
        let rows = status_json(root.path());
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0]["name"], "datadog");
        assert_eq!(rows[0]["state"], "signed-in");
        assert_eq!(rows[0]["resource"], "https://mcp.datadoghq.com/v1/mcp");
        store
            .write_health(&Health {
                state: StatusState::Degraded,
                label: "upstream unavailable".into(),
                at: now_ms(),
            })
            .unwrap();
        assert_eq!(
            status(&store),
            ServerStatus::new(StatusState::Degraded, "upstream unavailable")
        );
        // Another Datadog site is another server, with no sign-in.
        let other = server("datadog", "https://mcp.us5.datadoghq.com/v1/mcp");
        assert_eq!(
            status(&other.store(root.path())).state,
            StatusState::SignedOut
        );
        // A lost token file after a sign-in is reconnect-needed.
        std::fs::remove_file(store.dir().join(TOKENS)).unwrap();
        assert_eq!(
            status(&store),
            ServerStatus::new(StatusState::SignedOut, "reconnect needed")
        );
    }

    type Calls = Arc<Mutex<Vec<String>>>;
    type Seen = Arc<Mutex<Vec<(String, String)>>>;

    /// A fake token endpoint: counts calls, answers `reply`.
    pub(crate) async fn token_endpoint(reply: (u16, Value)) -> (String, Calls) {
        let calls = Arc::new(Mutex::new(Vec::new()));
        let state = (calls.clone(), reply);
        let app =
            Router::new()
                .route(
                    "/token",
                    post(
                        |State((calls, (code, body))): State<(Calls, (u16, Value))>,
                         form: String| async move {
                            calls.lock().unwrap().push(form);
                            (
                                axum::http::StatusCode::from_u16(code).unwrap(),
                                axum::Json(body),
                            )
                        },
                    ),
                )
                .with_state(state);
        (format!("{}/token", serve(app).await), calls)
    }

    fn source(root: &Path, token_url: &str, tokens: &Tokens) -> TokenSource {
        let s = server("datadog", "https://mcp.datadoghq.com/v1/mcp");
        let store = signed_in(root, &s, token_url, tokens);
        TokenSource::new(store, reqwest::Client::new())
    }

    #[tokio::test]
    async fn the_stored_token_is_used_until_the_refresh_window_then_refreshed() {
        let (url, calls) = token_endpoint((
            200,
            json!({"access_token": "fresh", "refresh_token": "refresh-2", "expires_in": 3600}),
        ))
        .await;
        let root = tempfile::tempdir().unwrap();
        let live = source(root.path(), &url, &tokens("live", 10 * 60_000));
        assert_eq!(live.access_token(false, None).await.unwrap(), "live");
        assert!(calls.lock().unwrap().is_empty());

        let root = tempfile::tempdir().unwrap();
        let near = source(root.path(), &url, &tokens("old", 60_000));
        let (a, b) = tokio::join!(
            near.access_token(false, None),
            near.access_token(false, None)
        );
        assert_eq!((a.unwrap(), b.unwrap()), ("fresh".into(), "fresh".into()));
        assert_eq!(
            calls.lock().unwrap().len(),
            1,
            "concurrent callers share one refresh"
        );
        let form = calls.lock().unwrap()[0].clone();
        assert!(form.contains("grant_type=refresh_token"), "{form}");
        assert!(form.contains("refresh_token=refresh-1"), "{form}");
        assert!(form.contains("client_id=client-1"), "{form}");
        assert!(form.contains("resource=https"), "{form}");
        let stored = near.store.tokens().unwrap();
        assert_eq!(stored.refresh_token.as_deref(), Some("refresh-2"));
        assert!(stored.expires_at.unwrap() > now_ms() + 3_500_000);
    }

    #[tokio::test]
    async fn a_refused_refresh_marks_reconnect_needed() {
        let (url, _) = token_endpoint((400, json!({"error": "invalid_grant"}))).await;
        let root = tempfile::tempdir().unwrap();
        let src = source(root.path(), &url, &tokens("old", -1000));
        assert_eq!(
            src.access_token(false, None).await,
            Err(AccessError::SignedOut("reconnect needed".into()))
        );
        assert!(src.store.tokens().unwrap().refresh_failed_at.is_some());
        assert!(!src.injectable());
        assert_eq!(src.status().state, StatusState::SignedOut);
    }

    #[tokio::test]
    async fn a_transient_refresh_failure_keeps_a_live_token_and_marks_nothing() {
        let (url, _) = token_endpoint((503, json!({}))).await;
        let root = tempfile::tempdir().unwrap();
        let src = source(root.path(), &url, &tokens("short", 30_000));
        assert_eq!(src.access_token(false, None).await.unwrap(), "short");
        // A bearer the upstream just refused is never handed back.
        assert!(matches!(
            src.access_token(true, Some("short")).await,
            Err(AccessError::Unavailable(_))
        ));
        assert!(src.store.tokens().unwrap().refresh_failed_at.is_none());
    }

    #[tokio::test]
    async fn a_rejected_old_bearer_reuses_a_replacement_already_stored() {
        let (url, calls) = token_endpoint((200, json!({"access_token": "never"}))).await;
        let root = tempfile::tempdir().unwrap();
        let src = source(root.path(), &url, &tokens("replacement", 30 * 60_000));
        assert_eq!(
            src.access_token(true, Some("stale")).await.unwrap(),
            "replacement"
        );
        assert!(calls.lock().unwrap().is_empty());
        src.mark_access_rejected("stale").await;
        assert!(src.injectable(), "only the refused bearer is ever marked");
        src.mark_access_rejected("replacement").await;
        assert!(!src.injectable());
    }

    #[tokio::test]
    async fn sign_in_registers_builds_pkce_url_and_persists_after_exchange() {
        let seen = Arc::new(Mutex::new(Vec::<(String, String)>::new()));
        let base_cell = Arc::new(Mutex::new(String::new()));
        // No protected-resource metadata: discovery falls back to the
        // resource origin's authorization-server metadata, as Kory's does.
        let metadata = Router::new()
            .route(
                "/.well-known/oauth-authorization-server",
                get(|State(base): State<Arc<Mutex<String>>>| async move {
                    let base = base.lock().unwrap().clone();
                    axum::Json(json!({
                        "authorization_endpoint": format!("{base}/authorize"),
                        "token_endpoint": format!("{base}/token"),
                        "registration_endpoint": format!("{base}/register"),
                    }))
                }),
            )
            .with_state(base_cell.clone());
        let app = Router::new()
            .route(
                "/register",
                post(|State(seen): State<Seen>, body: String| async move {
                    seen.lock().unwrap().push(("register".into(), body));
                    axum::Json(json!({"client_id": "dcr-client"}))
                }),
            )
            .route(
                "/token",
                post(|State(seen): State<Seen>, body: String| async move {
                    seen.lock().unwrap().push(("token".into(), body));
                    axum::Json(
                        json!({"access_token": "at", "refresh_token": "rt", "expires_in": 3600}),
                    )
                }),
            )
            .with_state(seen.clone())
            .merge(metadata);
        let base = serve(app).await;
        *base_cell.lock().unwrap() = base.clone();
        let s = server("svc", &format!("{base}/mcp?toolsets=a"));
        let root = tempfile::tempdir().unwrap();
        let store = s.store(root.path());
        let http = reqwest::Client::new();
        let redirect = "http://127.0.0.1:4567/callback";
        let (url, flow) = start_flow(&http, &s, &store, redirect).await.unwrap();
        let url = reqwest::Url::parse(&url).unwrap();
        let q: std::collections::HashMap<_, _> = url.query_pairs().into_owned().collect();
        assert_eq!(q["client_id"], "dcr-client");
        assert_eq!(q["redirect_uri"], redirect);
        assert_eq!(q["code_challenge_method"], "S256");
        assert_eq!(q["code_challenge"], pkce_challenge(&flow.verifier));
        assert_eq!(q["state"], flow.state);
        assert_eq!(q["resource"], format!("{base}/mcp"));
        let registration: Value = serde_json::from_str(&seen.lock().unwrap()[0].1).unwrap();
        assert_eq!(registration["client_name"], "router-acp");
        assert_eq!(registration["token_endpoint_auth_method"], "none");
        assert_eq!(registration["redirect_uris"], json!([redirect]));
        assert!(
            store.client().is_none(),
            "no client before a successful exchange"
        );

        assert!(
            finish_flow(&http, &store, &flow, "code-1", "wrong-state")
                .await
                .is_err()
        );
        assert!(store.tokens().is_none());
        finish_flow(&http, &store, &flow, "code-1", &flow.state.clone())
            .await
            .unwrap();
        let exchange = seen.lock().unwrap()[1].1.clone();
        assert!(
            exchange.contains("grant_type=authorization_code"),
            "{exchange}"
        );
        assert!(exchange.contains("code=code-1"), "{exchange}");
        assert!(
            exchange.contains(&format!("code_verifier={}", flow.verifier)),
            "{exchange}"
        );
        assert_eq!(store.client().unwrap().client_id, "dcr-client");
        assert_eq!(store.tokens().unwrap().access_token, "at");
        assert_eq!(store.client().unwrap().name, "svc");
        assert_eq!(status(&store).state, StatusState::SignedIn);
    }

    #[tokio::test]
    async fn a_failed_exchange_names_the_error_code_but_never_the_body() {
        let (token_url, _) = token_endpoint((
            400,
            json!({"error": "invalid_grant", "error_description": "secret-ish body text"}),
        ))
        .await;
        let root = tempfile::tempdir().unwrap();
        let store = Store::new(root.path(), "svc");
        let flow = PendingFlow {
            state: "s".into(),
            verifier: "v".into(),
            client: ClientRecord {
                name: "svc".into(),
                client_id: "c".into(),
                redirect_uri: "http://127.0.0.1:1/callback".into(),
                resource: "https://x.test/mcp".into(),
                endpoints: Endpoints {
                    authorize: "https://x.test/a".into(),
                    token: token_url,
                    register: None,
                },
            },
            expires_at: now_ms() + 60_000,
        };
        let http = reqwest::Client::new();
        let err = finish_flow(&http, &store, &flow, "code", "s")
            .await
            .unwrap_err();
        assert_eq!(err, "token exchange failed (invalid_grant (HTTP 400))");
        assert!(store.tokens().is_none() && store.client().is_none());
        let expired = PendingFlow {
            expires_at: now_ms() - 1,
            ..flow
        };
        assert!(
            finish_flow(&http, &store, &expired, "code", "s")
                .await
                .is_err()
        );
    }
}
