//! The cross-process authority for provider credential repair and rejection.
//! Locks cover a single credential repair, never an adapter's session or turn.

use std::fs::{File, OpenOptions};
use std::path::{Path, PathBuf};
use std::time::Duration;

use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use crate::auth::AuthAvailability;
use crate::config::AgentConfig;

const WAIT: Duration = Duration::from_secs(25);
const RETRY_DELAY: Duration = Duration::from_secs(5);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum RepairOutcome {
    Repaired,
    Rejected,
    Unknown,
}

#[derive(Serialize, Deserialize)]
struct Record {
    generation: String,
    epoch: String,
    outcome: RepairOutcome,
    attempted_at: String,
}

pub(crate) fn directory(agent: &AgentConfig) -> Option<PathBuf> {
    let dir = match crate::accounts::provider(agent)? {
        "claude" => agent.config_dir("CLAUDE_CONFIG_DIR", ".claude"),
        "codex" => agent.config_dir("CODEX_HOME", ".codex"),
        "grok" => agent.config_dir("GROK_HOME", ".grok"),
        "kimi" => agent
            .env_var("KIMI_SHARE_DIR")
            .or_else(|| agent.env_var("KIMI_CODE_HOME"))
            .map(PathBuf::from)
            .or_else(|| {
                let home = PathBuf::from(agent.env_var("HOME")?);
                let legacy = home.join(".kimi");
                let current = home.join(".kimi-code");
                Some(if !legacy.exists() && current.exists() {
                    current
                } else {
                    legacy
                })
            }),
        _ => None,
    }?;
    Some(dir.canonicalize().unwrap_or(dir))
}

struct Credential {
    path: PathBuf,
    value: Value,
    generation: String,
}

pub(crate) struct RuntimeCredential {
    root: PathBuf,
    pub generation: String,
}

impl Drop for RuntimeCredential {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.root);
    }
}

/// Adapters receive access-only copies. Their built-in refreshers cannot
/// rotate or erase the manager's refresh token, or another session's store.
pub(crate) async fn runtime(
    agent: &AgentConfig,
    env: &mut Vec<(String, String)>,
) -> Result<Option<RuntimeCredential>, String> {
    let provider = crate::accounts::provider(agent).unwrap_or("unknown");
    if !matches!(provider, "claude" | "codex" | "grok" | "kimi") {
        return Ok(None);
    }
    let template_args: &[String] = match &agent.model_selection {
        crate::config::ModelSelectionConfig::SpawnConfig { process_template } => {
            &process_template.args
        }
        _ => &[],
    };
    let probe_args = agent
        .auth_probe
        .as_ref()
        .map(|p| p.args.as_slice())
        .unwrap_or_default();
    if provider == "kimi"
        && agent
            .command
            .args
            .iter()
            .chain(template_args)
            .chain(probe_args)
            .any(|arg| {
                matches!(arg.as_str(), "--config" | "--config-file")
                    || arg.starts_with("--config=")
                    || arg.starts_with("--config-file=")
            })
    {
        return Err("Kimi credential isolation requires its default config location. Use KIMI_SHARE_DIR or KIMI_CODE_HOME for account configuration.".into());
    }
    env.retain(|(k, _)| !crate::accounts::AUTH_ENV.contains(&k.as_str()));
    if matches!(
        availability(agent),
        AuthAvailability::Unauthenticated { .. }
    ) {
        return Err(
            "Authentication required: credential refresh was definitively rejected. Use /login."
                .into(),
        );
    }
    // Missing or unreadable credentials still get a private runtime. Falling
    // back to the adapter's default directory would create a second writer.
    let mut credential = read(agent);
    let expires = if provider == "kimi" {
        credential
            .as_ref()
            .and_then(|c| c.value.get("expires_at"))
            .and_then(Value::as_f64)
            .and_then(|seconds| chrono::DateTime::from_timestamp(seconds as i64, 0))
    } else if provider == "claude" {
        credential
            .as_ref()
            .and_then(|c| c.value.pointer("/claudeAiOauth/expiresAt"))
            .and_then(Value::as_i64)
            .and_then(chrono::DateTime::from_timestamp_millis)
    } else {
        credential
            .as_ref()
            .and_then(|c| c.value.pointer("/tokens/access_token"))
            .and_then(Value::as_str)
            .and_then(crate::accounts::jwt_claims)
            .and_then(|v| v.get("exp").and_then(Value::as_i64))
            .and_then(|s| chrono::DateTime::from_timestamp(s, 0))
    };
    if expires.is_some_and(|e| e <= chrono::Utc::now() + chrono::Duration::minutes(1)) {
        let observed = request_generation(agent);
        if repair(agent, observed.as_deref()).await == RepairOutcome::Rejected {
            return Err("Credential refresh was rejected. Sign in again with /login.".into());
        }
        credential = Some(read(agent).ok_or("Credential unavailable after refresh")?);
    }
    let dir = directory(agent).ok_or("Credential directory unavailable")?;
    let root = dir
        .join(".router-acp-runtime")
        .join(uuid::Uuid::new_v4().to_string());
    std::fs::create_dir_all(&root).map_err(|_| "Could not isolate adapter credentials")?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(
            root.parent().unwrap(),
            std::fs::Permissions::from_mode(0o700),
        )
        .map_err(|_| "Could not protect adapter credentials")?;
        std::fs::set_permissions(&root, std::fs::Permissions::from_mode(0o700))
            .map_err(|_| "Could not protect adapter credentials")?;
    }
    let runtime = RuntimeCredential {
        root,
        generation: credential
            .as_ref()
            .map(|c| marker(c, record(&dir).as_ref()))
            .unwrap_or_else(|| "missing".into()),
    };
    for entry in std::fs::read_dir(&dir).map_err(|_| "Could not read adapter configuration")? {
        let entry = entry.map_err(|_| "Could not read adapter configuration")?;
        let name = entry.file_name();
        if name == ".credentials.json"
            || name == "auth.json"
            || ((provider == "kimi"
                && (name == "credentials" || name == "config.toml" || name == "config.json"))
                || (matches!(provider, "codex" | "grok") && name == "config.toml"))
            || matches!(
                name.to_string_lossy().as_ref(),
                "projects"
                    | "sessions"
                    | "archived_sessions"
                    | "history.jsonl"
                    | "session_log"
                    | "session.db"
                    | "sessions.db"
            )
            || name.to_string_lossy().starts_with(".router-acp-")
        {
            continue;
        }
        #[cfg(unix)]
        std::os::unix::fs::symlink(entry.path(), runtime.root.join(name))
            .map_err(|_| "Could not preserve adapter configuration")?;
    }
    if matches!(provider, "codex" | "grok") {
        write_runtime_config(provider, &dir, &runtime.root)?;
    }
    let mut value = credential.map(|c| c.value).unwrap_or_else(|| json!({}));
    if provider == "claude" {
        if let Some(object) = value
            .get_mut("claudeAiOauth")
            .and_then(Value::as_object_mut)
        {
            object.remove("refreshToken");
        }
        if let Some(token) = value
            .pointer("/claudeAiOauth/accessToken")
            .and_then(Value::as_str)
        {
            env.push(("CLAUDE_CODE_OAUTH_TOKEN".into(), token.into()));
        }
    } else if provider == "codex" && value.get("tokens").is_some() {
        value["tokens"]["refresh_token"] = "".into();
    }
    if provider == "kimi" {
        let source = dir.join("config.toml");
        let mut config: Value = if source.exists() {
            toml::from_str(
                &std::fs::read_to_string(&source).map_err(|_| "Kimi configuration unavailable")?,
            )
            .map_err(|_| "Invalid Kimi configuration")?
        } else {
            json!({})
        };
        let access = value.get("access_token").and_then(Value::as_str);
        // OAuthManager scans every provider and service. Remove all OAuth
        // references so it never loads keyring or a canonical refresh token.
        for group in ["providers", "services"] {
            if let Some(entries) = config.get_mut(group).and_then(Value::as_object_mut) {
                for entry in entries.values_mut().filter_map(Value::as_object_mut) {
                    if let Some(oauth) = entry.remove("oauth") {
                        let token = oauth
                            .get("key")
                            .and_then(Value::as_str)
                            .filter(|key| matches!(*key, "oauth/kimi-code" | "kimi-code"))
                            .and(access)
                            .unwrap_or("");
                        entry.remove("api_key_env");
                        entry.insert("api_key".into(), token.into());
                    }
                }
            }
        }
        let config =
            toml::to_string(&config).map_err(|_| "Could not isolate Kimi configuration")?;
        write_private_bytes(&runtime.root.join("config.toml"), config.as_bytes())?;
        for variable in ["KIMI_SHARE_DIR", "KIMI_CODE_HOME"] {
            env.retain(|(key, _)| key != variable);
            env.push((variable.into(), runtime.root.to_string_lossy().into()));
        }
        if let Some(access) = access {
            env.push(("KIMI_API_KEY".into(), access.into()));
        }
    } else if provider == "grok" {
        let home = agent.env_var("HOME").ok_or("HOME is unavailable")?;
        let isolated_home = runtime.root.join("home");
        std::fs::create_dir(&isolated_home).map_err(|_| "Could not isolate Grok authentication")?;
        #[cfg(unix)]
        {
            // Preserve tools without exposing another provider's auth store.
            for name in [".local", ".cargo", ".rustup", ".gitconfig", ".ssh"] {
                let source = Path::new(&home).join(name);
                if source.exists() {
                    std::os::unix::fs::symlink(source, isolated_home.join(name))
                        .map_err(|_| "Could not preserve Grok tools")?;
                }
            }
            std::fs::create_dir(isolated_home.join(".config"))
                .map_err(|_| "Could not preserve Grok tools")?;
            for name in ["gh", "linear", "gcloud"] {
                let source = Path::new(&home).join(".config").join(name);
                if source.exists() {
                    std::os::unix::fs::symlink(source, isolated_home.join(".config").join(name))
                        .map_err(|_| "Could not preserve Grok tools")?;
                }
            }
            std::os::unix::fs::symlink(&runtime.root, isolated_home.join(".grok"))
                .map_err(|_| "Could not isolate Grok authentication")?;
        }
        let binary = crate::transport::router_env()
            .iter()
            .find(|(k, _)| k == "ROUTER_ACP_BIN")
            .map(|(_, v)| v.clone())
            .unwrap_or_else(|| "router-acp".into());
        let quote = |s: &str| format!("'{}'", s.replace('\'', "'\\''"));
        let helper = runtime.root.join(".router-acp-auth-provider");
        let script = format!(
            "#!/bin/sh\nexec {} credential-token --provider grok --directory {} --runtime-directory {}\n",
            quote(&binary),
            quote(&dir.to_string_lossy()),
            quote(&runtime.root.to_string_lossy()),
        );
        write_private_bytes(&helper, script.as_bytes())?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&helper, std::fs::Permissions::from_mode(0o700))
                .map_err(|_| "Could not isolate Grok authentication")?;
        }
        write_private(
            &runtime.root.join(".router-acp-token.json"),
            &json!({"generation":runtime.generation}),
        )?;
        env.retain(|(k, _)| k != "HOME" && k != "GROK_HOME" && k != "GROK_AUTH_PROVIDER_COMMAND");
        env.push(("HOME".into(), isolated_home.to_string_lossy().into()));
        env.push(("GROK_HOME".into(), runtime.root.to_string_lossy().into()));
        env.push((
            "GROK_AUTH_PROVIDER_COMMAND".into(),
            helper.to_string_lossy().into(),
        ));
    } else {
        write_private(
            &runtime.root.join(if provider == "claude" {
                ".credentials.json"
            } else {
                "auth.json"
            }),
            &value,
        )?;
        let variable = if provider == "claude" {
            "CLAUDE_CONFIG_DIR"
        } else {
            "CODEX_HOME"
        };
        env.retain(|(k, _)| k != variable);
        env.push((variable.into(), runtime.root.to_string_lossy().into()));
    }
    Ok(Some(runtime))
}

fn write_runtime_config(provider: &str, dir: &Path, root: &Path) -> Result<(), String> {
    let source = dir.join("config.toml");
    let mut config = if source.exists() {
        toml::from_str(
            &std::fs::read_to_string(&source)
                .map_err(|_| format!("{provider} configuration unavailable"))?,
        )
        .map_err(|_| format!("Invalid {provider} configuration"))?
    } else {
        toml::Value::Table(Default::default())
    };
    let root_table = config
        .as_table_mut()
        .ok_or_else(|| format!("Invalid {provider} configuration"))?;
    match provider {
        "codex" => {
            let features = root_table
                .entry("features")
                .or_insert_with(|| toml::Value::Table(Default::default()))
                .as_table_mut()
                .ok_or("Invalid codex configuration")?;
            features.insert(
                "default_mode_request_user_input".into(),
                toml::Value::Boolean(true),
            );
        }
        "grok" => {
            let folder_trust = root_table
                .entry("folder_trust")
                .or_insert_with(|| toml::Value::Table(Default::default()))
                .as_table_mut()
                .ok_or("Invalid grok configuration")?;
            folder_trust.insert("enabled".into(), toml::Value::Boolean(false));

            let skills = root_table
                .entry("skills")
                .or_insert_with(|| toml::Value::Table(Default::default()))
                .as_table_mut()
                .ok_or("Invalid grok configuration")?;
            merge_toml_strings(
                skills,
                "disabled",
                &["resume-codex", "resume-claude", "resume-cursor"],
            )?;
            merge_toml_strings(
                skills,
                "ignore",
                &[
                    "~/.grok/bundled/skills/resume-codex",
                    "~/.grok/bundled/skills/resume-claude",
                    "~/.grok/bundled/skills/resume-cursor",
                ],
            )?;
        }
        _ => return Ok(()),
    }
    let config = toml::to_string(&config)
        .map_err(|_| format!("Could not isolate {provider} configuration"))?;
    write_private_bytes(&root.join("config.toml"), config.as_bytes())
}

fn merge_toml_strings(
    table: &mut toml::map::Map<String, toml::Value>,
    key: &str,
    required: &[&str],
) -> Result<(), String> {
    let values = table
        .entry(key)
        .or_insert_with(|| toml::Value::Array(Vec::new()))
        .as_array_mut()
        .ok_or("Invalid grok configuration")?;
    for required in required {
        if !values.iter().any(|value| value.as_str() == Some(*required)) {
            values.push(toml::Value::String((*required).into()));
        }
    }
    Ok(())
}

fn read(agent: &AgentConfig) -> Option<Credential> {
    let provider = crate::accounts::provider(agent)?;
    let path = directory(agent)?.join(match provider {
        "claude" => ".credentials.json",
        "kimi" => "credentials/kimi-code.json",
        _ => "auth.json",
    });
    let value: Value = serde_json::from_slice(&std::fs::read(&path).ok()?).ok()?;
    // Include refresh material so a login or refresh with an unchanged access
    // token still supersedes an older rejection. Never persist token values.
    let material = if provider == "claude" {
        value.get("claudeAiOauth")?
    } else {
        &value
    };
    let present = match provider {
        "claude" => [material.get("accessToken"), material.get("refreshToken")],
        "codex" => [
            value
                .pointer("/tokens/access_token")
                .or_else(|| value.get("OPENAI_API_KEY")),
            value.pointer("/tokens/refresh_token"),
        ],
        "grok" => {
            let auth = grok_auth(&value);
            [
                auth.and_then(|v| v.get("key")),
                auth.and_then(|v| v.get("refresh_token")),
            ]
        }
        "kimi" => [value.get("access_token"), value.get("refresh_token")],
        _ => return None,
    };
    if !present.into_iter().any(|v| {
        v.and_then(Value::as_str)
            .is_some_and(|s| !s.trim().is_empty())
    }) {
        return None;
    }
    let generation = crate::usage_cache::fingerprint(&material.to_string());
    Some(Credential {
        path,
        value,
        generation,
    })
}

fn record(dir: &Path) -> Option<Record> {
    serde_json::from_slice(&std::fs::read(dir.join(".router-acp-auth.json")).ok()?).ok()
}

fn marker(credential: &Credential, record: Option<&Record>) -> String {
    let epoch = record
        .filter(|r| r.generation == credential.generation)
        .map(|r| r.epoch.as_str())
        .unwrap_or("initial");
    format!("{}:{epoch}", credential.generation)
}

/// Capture shared repair state with the credential used by a request. A
/// sibling's successful repair invalidates this marker even if its access
/// token happens to remain unchanged.
pub fn request_generation(agent: &AgentConfig) -> Option<String> {
    let credential = read(agent)?;
    Some(marker(&credential, record(&directory(agent)?).as_ref()))
}

pub(crate) fn access_generation(agent: &AgentConfig) -> Option<String> {
    let credential = read(agent)?;
    let token = match crate::accounts::provider(agent)? {
        "claude" => credential.value.pointer("/claudeAiOauth/accessToken"),
        "codex" => credential
            .value
            .pointer("/tokens/access_token")
            .or_else(|| credential.value.get("OPENAI_API_KEY")),
        "grok" => grok_auth(&credential.value)?.get("key"),
        "kimi" => credential.value.get("access_token"),
        _ => None,
    }?
    .as_str()?;
    Some(crate::usage_cache::fingerprint(token))
}

pub(crate) async fn observe_success(agent: &AgentConfig, access: &str) {
    let Ok(_lock) = lock(agent).await else {
        return;
    };
    if access_generation(agent).as_deref() != Some(access) {
        return;
    }
    record_success(agent);
}

pub(crate) async fn observe_request_success(agent: &AgentConfig, observed: &str) {
    let Ok(_lock) = lock(agent).await else {
        return;
    };
    if request_generation(agent).as_deref() != Some(observed) {
        return;
    }
    record_success(agent);
}

pub(crate) fn record_success(agent: &AgentConfig) {
    let Some(credential) = read(agent) else {
        return;
    };
    let Some(dir) = directory(agent) else {
        return;
    };
    let previous = record(&dir).filter(|r| r.generation == credential.generation);
    // Cached positive evidence cannot undo a definitive repair rejection.
    if previous
        .as_ref()
        .is_some_and(|r| r.outcome == RepairOutcome::Rejected)
    {
        return;
    }
    let epoch = previous
        .map(|r| r.epoch)
        .unwrap_or_else(|| "initial".into());
    let _ = write_private(
        &dir.join(".router-acp-auth.json"),
        &Record {
            generation: credential.generation,
            epoch,
            outcome: RepairOutcome::Repaired,
            attempted_at: chrono::Utc::now().to_rfc3339(),
        },
    );
}

/// Native login already holds the credential lock while its CLI publishes.
/// A successful login starts a new epoch even if the provider reuses tokens.
pub(crate) fn login_succeeded(agent: &AgentConfig) -> Result<(), String> {
    let Some(credential) = read(agent) else {
        return Ok(());
    };
    let dir = directory(agent).ok_or("Credential directory unavailable")?;
    write_private(
        &dir.join(".router-acp-auth.json"),
        &Record {
            generation: credential.generation,
            epoch: uuid::Uuid::new_v4().to_string(),
            outcome: RepairOutcome::Repaired,
            attempted_at: chrono::Utc::now().to_rfc3339(),
        },
    )
}

/// Only a completed, current-generation repair can establish durable logout.
pub fn availability(agent: &AgentConfig) -> AuthAvailability {
    let Some(credential) = read(agent) else {
        return AuthAvailability::Unknown;
    };
    let Some(dir) = directory(agent) else {
        return AuthAvailability::Unknown;
    };
    match record(&dir)
        .filter(|r| r.generation == credential.generation)
        .map(|r| r.outcome)
    {
        Some(RepairOutcome::Repaired) => AuthAvailability::Authenticated,
        Some(RepairOutcome::Rejected) => AuthAvailability::Unauthenticated {
            reason: "Credential refresh was definitively rejected. Sign in again with /login."
                .into(),
        },
        _ => AuthAvailability::Unknown,
    }
}

/// Whether the canonical store contains usable credential material.
///
/// This is separate from availability: an unprobed credential is present but
/// still has unknown authentication status.
pub fn present(agent: &AgentConfig) -> bool {
    read(agent).is_some()
}

/// OS locks release on cancellation, crash and process exit. There is no
/// stale-age lock breaking that could permit two live repair owners.
pub(crate) async fn lock(agent: &AgentConfig) -> Result<File, String> {
    let dir = directory(agent).ok_or("Credential directory unavailable")?;
    lock_at(&dir).await
}

async fn lock_at(dir: &Path) -> Result<File, String> {
    std::fs::create_dir_all(dir).map_err(|_| "Credential directory unavailable")?;
    let mut options = OpenOptions::new();
    options.read(true).write(true).create(true).truncate(false);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let file = options
        .open(dir.join(".router-acp-auth.lock"))
        .map_err(|_| "Credential repair lock unavailable")?;
    tokio::time::timeout(WAIT, async {
        loop {
            match file.try_lock() {
                Ok(()) => return Ok(file),
                Err(std::fs::TryLockError::WouldBlock) => {
                    tokio::time::sleep(Duration::from_millis(25)).await
                }
                Err(_) => return Err("Credential repair lock unavailable".into()),
            }
        }
    })
    .await
    .map_err(|_| "Timed out waiting for this credential's repair".to_string())?
}

fn write_private(path: &Path, value: &impl Serialize) -> Result<(), String> {
    let bytes = serde_json::to_vec(value).map_err(|_| "Could not publish credential repair")?;
    write_private_bytes(path, &bytes)
}

fn write_private_bytes(path: &Path, bytes: &[u8]) -> Result<(), String> {
    use std::io::Write;
    let temp = path.with_extension(format!("{}.tmp", uuid::Uuid::new_v4()));
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
        std::fs::rename(&temp, path)
    })();
    let _ = std::fs::remove_file(temp);
    result.map_err(|_| "Could not publish credential repair".into())
}

struct Refresh {
    outcome: RepairOutcome,
    value: Option<Value>,
}

pub async fn repair(agent: &AgentConfig, observed: Option<&str>) -> RepairOutcome {
    let kimi_host = agent
        .env_var("KIMI_CODE_OAUTH_HOST")
        .or_else(|| agent.env_var("KIMI_OAUTH_HOST"))
        .unwrap_or_else(|| "https://auth.kimi.com".into());
    durable_repair(agent, observed, move |value, provider| async move {
        if provider == "kimi" {
            refresh_kimi(value, &kimi_host).await
        } else {
            refresh(value, provider).await
        }
    })
    .await
}

// Cancelling a session must not cancel an in-flight token rotation. The
// detached task publishes the result before releasing this credential's lock.
async fn durable_repair<F, Fut>(
    agent: &AgentConfig,
    observed: Option<&str>,
    action: F,
) -> RepairOutcome
where
    F: FnOnce(Value, String) -> Fut + Send + 'static,
    Fut: std::future::Future<Output = Refresh> + Send,
{
    let agent = agent.clone();
    let observed = observed.map(str::to_owned);
    tokio::spawn(async move { repair_with(&agent, observed.as_deref(), action).await })
        .await
        .unwrap_or(RepairOutcome::Unknown)
}

async fn repair_with<F, Fut>(
    agent: &AgentConfig,
    observed: Option<&str>,
    action: F,
) -> RepairOutcome
where
    F: FnOnce(Value, String) -> Fut,
    Fut: std::future::Future<Output = Refresh>,
{
    let Some(dir) = directory(agent) else {
        return RepairOutcome::Unknown;
    };
    let Ok(_lock) = lock_at(&dir).await else {
        return RepairOutcome::Unknown;
    };
    // Every waiter re-reads both material and result after acquiring the lock.
    let Some(credential) = read(agent) else {
        return RepairOutcome::Unknown;
    };
    let previous = record(&dir).filter(|r| r.generation == credential.generation);
    let current = marker(&credential, previous.as_ref());
    if observed.is_none() {
        return RepairOutcome::Unknown;
    }
    if observed != Some(current.as_str()) {
        return previous
            .map(|r| r.outcome)
            .unwrap_or(RepairOutcome::Repaired);
    }
    if let Some(previous) = previous {
        if previous.outcome == RepairOutcome::Rejected {
            return RepairOutcome::Rejected;
        }
        if previous.outcome == RepairOutcome::Unknown
            && chrono::DateTime::parse_from_rfc3339(&previous.attempted_at).is_ok_and(|at| {
                chrono::Utc::now()
                    .signed_duration_since(at)
                    .to_std()
                    .unwrap_or_default()
                    < RETRY_DELAY
            })
        {
            return RepairOutcome::Unknown;
        }
    }
    let provider = crate::accounts::provider(agent)
        .unwrap_or("unknown")
        .to_string();
    let result = tokio::time::timeout(
        Duration::from_secs(20),
        action(credential.value.clone(), provider),
    )
    .await;
    let mut result = result.unwrap_or(Refresh {
        outcome: RepairOutcome::Unknown,
        value: None,
    });
    // A newer provider/login write wins over either a success or a rejection.
    let Some(current) = read(agent) else {
        return RepairOutcome::Unknown;
    };
    if current.generation != credential.generation {
        return RepairOutcome::Repaired;
    }
    if let Some(value) = result.value.take()
        && write_private(&credential.path, &value).is_err()
    {
        return RepairOutcome::Unknown;
    }
    let Some(current) = read(agent) else {
        return RepairOutcome::Unknown;
    };
    let state = Record {
        generation: current.generation,
        epoch: uuid::Uuid::new_v4().to_string(),
        outcome: result.outcome.clone(),
        attempted_at: chrono::Utc::now().to_rfc3339(),
    };
    if write_private(&dir.join(".router-acp-auth.json"), &state).is_err() {
        return RepairOutcome::Unknown;
    }
    result.outcome
}

async fn refresh(mut value: Value, provider: String) -> Refresh {
    if provider == "grok" {
        return refresh_grok(value).await;
    }
    let (refresh_token, endpoint, client_id) = match provider.as_str() {
        "claude" => (
            value
                .pointer("/claudeAiOauth/refreshToken")
                .and_then(Value::as_str),
            "https://platform.claude.com/v1/oauth/token",
            "9d1c250a-e61b-44d9-88ed-5944d1962f5e",
        ),
        "codex" => (
            value
                .pointer("/tokens/refresh_token")
                .and_then(Value::as_str),
            "https://auth.openai.com/oauth/token",
            "app_EMoamEEZ73f0CkXaXp7hrann",
        ),
        _ => {
            return Refresh {
                outcome: RepairOutcome::Unknown,
                value: None,
            };
        }
    };
    let Some(token) = refresh_token.filter(|t| !t.trim().is_empty()) else {
        // A session error alone cannot prove a key-only or unreadable login
        // revoked. Leave it unknown until an authoritative validation exists.
        return Refresh {
            outcome: RepairOutcome::Unknown,
            value: None,
        };
    };
    let response = reqwest::Client::new()
        .post(endpoint)
        .json(&json!({
            "grant_type": "refresh_token", "refresh_token": token, "client_id": client_id,
        }))
        .send()
        .await;
    let Ok(response) = response else {
        return Refresh {
            outcome: RepairOutcome::Unknown,
            value: None,
        };
    };
    let status = response.status();
    let Ok(body) = response.json::<Value>().await else {
        return Refresh {
            outcome: RepairOutcome::Unknown,
            value: None,
        };
    };
    if !status.is_success() {
        // Never persist/log provider bodies: they can contain credential data.
        let error = body
            .get("error")
            .and_then(Value::as_str)
            .or_else(|| body.pointer("/error/code").and_then(Value::as_str));
        let definitive = status.is_client_error()
            && matches!(
                error,
                Some(
                    "invalid_grant"
                        | "refresh_token_expired"
                        | "refresh_token_reused"
                        | "refresh_token_invalidated"
                )
            );
        return Refresh {
            outcome: if definitive {
                RepairOutcome::Rejected
            } else {
                RepairOutcome::Unknown
            },
            value: None,
        };
    }
    let Some(access) = body
        .get("access_token")
        .and_then(Value::as_str)
        .filter(|t| !t.trim().is_empty())
    else {
        return Refresh {
            outcome: RepairOutcome::Unknown,
            value: None,
        };
    };
    if provider == "claude" {
        value["claudeAiOauth"]["accessToken"] = access.into();
        if let Some(token) = body
            .get("refresh_token")
            .and_then(Value::as_str)
            .filter(|t| !t.is_empty())
        {
            value["claudeAiOauth"]["refreshToken"] = token.into();
        }
        if let Some(seconds) = body.get("expires_in").and_then(Value::as_i64) {
            value["claudeAiOauth"]["expiresAt"] = (chrono::Utc::now()
                + chrono::Duration::seconds(seconds))
            .timestamp_millis()
            .into();
        }
    } else {
        value["tokens"]["access_token"] = access.into();
        for field in ["refresh_token", "id_token"] {
            if let Some(token) = body
                .get(field)
                .and_then(Value::as_str)
                .filter(|t| !t.is_empty())
            {
                value["tokens"][field] = token.into();
            }
        }
        value["last_refresh"] = chrono::Utc::now().to_rfc3339().into();
    }
    Refresh {
        outcome: RepairOutcome::Repaired,
        value: Some(value),
    }
}

const GROK_LOGIN: &str = "https://accounts.x.ai/sign-in";
const GROK_LOGIN_PREFIX: &str = "https://auth.x.ai::";

/// Grok 1.0.46 keys logins by issuer and principal id. Older releases used
/// one fixed URL. Prefer the newest current-format entry, then accept legacy.
pub(crate) fn grok_auth(value: &Value) -> Option<&Value> {
    value
        .as_object()?
        .iter()
        .filter(|(key, auth)| is_current_grok_auth(key, auth))
        .max_by_key(|(_, auth)| {
            auth.get("create_time")
                .and_then(Value::as_str)
                .unwrap_or("")
        })
        .map(|(_, auth)| auth)
        .or_else(|| value.get(GROK_LOGIN))
}

fn grok_auth_key(value: &Value) -> Option<String> {
    value
        .as_object()?
        .iter()
        .filter(|(key, auth)| is_current_grok_auth(key, auth))
        .max_by_key(|(_, auth)| {
            auth.get("create_time")
                .and_then(Value::as_str)
                .unwrap_or("")
        })
        .map(|(key, _)| key.clone())
        .or_else(|| value.get(GROK_LOGIN).map(|_| GROK_LOGIN.to_string()))
}

fn is_current_grok_auth(key: &str, auth: &Value) -> bool {
    key.starts_with(GROK_LOGIN_PREFIX)
        && auth.as_object().is_some()
        && ["key", "refresh_token"].into_iter().any(|field| {
            auth.get(field)
                .and_then(Value::as_str)
                .is_some_and(|value| !value.is_empty())
        })
}

async fn refresh_kimi(mut value: Value, host: &str) -> Refresh {
    let unknown = || Refresh {
        outcome: RepairOutcome::Unknown,
        value: None,
    };
    let Some(token) = value
        .get("refresh_token")
        .and_then(Value::as_str)
        .filter(|t| !t.is_empty())
    else {
        return unknown();
    };
    let Ok(response) = reqwest::Client::new()
        .post(format!("{}/api/oauth/token", host.trim_end_matches('/')))
        .form(&[
            ("grant_type", "refresh_token"),
            ("refresh_token", token),
            ("client_id", "17e5f671-d194-4dfb-9706-5516cb48c098"),
        ])
        .send()
        .await
    else {
        return unknown();
    };
    let status = response.status();
    let Ok(body) = response.json::<Value>().await else {
        return unknown();
    };
    if !status.is_success() {
        return Refresh {
            outcome: if status.is_client_error()
                && body.get("error").and_then(Value::as_str) == Some("invalid_grant")
            {
                RepairOutcome::Rejected
            } else {
                RepairOutcome::Unknown
            },
            value: None,
        };
    }
    let Some(access) = body
        .get("access_token")
        .and_then(Value::as_str)
        .filter(|t| !t.is_empty())
    else {
        return unknown();
    };
    value["access_token"] = access.into();
    for field in ["refresh_token", "scope", "token_type"] {
        if let Some(field_value) = body
            .get(field)
            .and_then(Value::as_str)
            .filter(|t| !t.is_empty())
        {
            value[field] = field_value.into();
        }
    }
    if let Some(seconds) = body
        .get("expires_in")
        .and_then(Value::as_f64)
        .filter(|s| s.is_finite() && *s > 0.0)
    {
        value["expires_in"] = seconds.into();
        value["expires_at"] = (chrono::Utc::now() + chrono::Duration::seconds(seconds as i64))
            .timestamp()
            .into();
    }
    Refresh {
        outcome: RepairOutcome::Repaired,
        value: Some(value),
    }
}

async fn refresh_grok(mut value: Value) -> Refresh {
    let unknown = || Refresh {
        outcome: RepairOutcome::Unknown,
        value: None,
    };
    let Some(auth_key) = grok_auth_key(&value) else {
        return unknown();
    };
    let auth = &value[&auth_key];
    let Some(token) = auth
        .get("refresh_token")
        .and_then(Value::as_str)
        .filter(|t| !t.is_empty())
    else {
        return unknown();
    };
    let Some(issuer) = auth
        .get("oidc_issuer")
        .and_then(Value::as_str)
        .filter(|s| !s.is_empty())
    else {
        return unknown();
    };
    let Some(client_id) = auth
        .get("oidc_client_id")
        .and_then(Value::as_str)
        .filter(|s| !s.is_empty())
    else {
        return unknown();
    };
    let client = reqwest::Client::new();
    let Ok(response) = client
        .get(format!(
            "{}/.well-known/openid-configuration",
            issuer.trim_end_matches('/')
        ))
        .send()
        .await
    else {
        return unknown();
    };
    if !response.status().is_success() {
        return unknown();
    }
    let Ok(discovery) = response.json::<Value>().await else {
        return unknown();
    };
    let Some(endpoint) = discovery.get("token_endpoint").and_then(Value::as_str) else {
        return unknown();
    };
    let Ok(response) = client
        .post(endpoint)
        .form(&[
            ("grant_type", "refresh_token"),
            ("refresh_token", token),
            ("client_id", client_id),
        ])
        .send()
        .await
    else {
        return unknown();
    };
    let status = response.status();
    let Ok(body) = response.json::<Value>().await else {
        return unknown();
    };
    if !status.is_success() {
        return Refresh {
            outcome: if status.is_client_error()
                && body.get("error").and_then(Value::as_str) == Some("invalid_grant")
            {
                RepairOutcome::Rejected
            } else {
                RepairOutcome::Unknown
            },
            value: None,
        };
    }
    let Some(access) = body
        .get("access_token")
        .and_then(Value::as_str)
        .filter(|t| !t.is_empty())
    else {
        return unknown();
    };
    value[&auth_key]["key"] = access.into();
    if let Some(token) = body
        .get("refresh_token")
        .and_then(Value::as_str)
        .filter(|t| !t.is_empty())
    {
        value[&auth_key]["refresh_token"] = token.into();
    }
    if let Some(seconds) = body.get("expires_in").and_then(Value::as_i64) {
        value[&auth_key]["expires_at"] = (chrono::Utc::now() + chrono::Duration::seconds(seconds))
            .to_rfc3339()
            .into();
    }
    Refresh {
        outcome: RepairOutcome::Repaired,
        value: Some(value),
    }
}

/// Grok's external-auth hook receives only access tokens. Each private runtime
/// keeps its own receipt because Grok does not hand credentials back to this
/// hook. Concurrent runtimes therefore report the generation they used.
pub async fn token_for_helper(
    provider: &str,
    dir: &Path,
    expired: bool,
    runtime_dir: &Path,
) -> Result<Value, String> {
    let agent = helper_agent(provider, dir)?;
    let receipt_path = runtime_dir.join(".router-acp-token.json");
    let receipt: Option<Value> = serde_json::from_slice(
        &std::fs::read(&receipt_path).map_err(|_| "Grok runtime receipt unavailable")?,
    )
    .ok();
    let observed = receipt
        .as_ref()
        .and_then(|v| v.get("generation"))
        .and_then(Value::as_str);
    if expired {
        // Grok can kill its external hook before a slow refresh finishes.
        // Run rotation in a separate process group so it can still publish
        // the rotated token and unlock, even if this hook is terminated.
        let mut command = tokio::process::Command::new(
            std::env::current_exe().map_err(|_| "Credential manager unavailable")?,
        );
        command
            .args(["credential-repair", "--provider", provider, "--directory"])
            .arg(dir)
            .arg("--observed")
            .arg(observed.unwrap_or("missing"))
            .stdin(std::process::Stdio::null())
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null());
        #[cfg(unix)]
        command.process_group(0);
        let status = command
            .spawn()
            .map_err(|_| "Credential manager unavailable")?
            .wait()
            .await
            .map_err(|_| "Credential repair unavailable")?;
        if !status.success() {
            return Err(
                "Credential repair unavailable. Use /login if the router reports rejection.".into(),
            );
        }
    }
    if matches!(
        availability(&agent),
        AuthAvailability::Unauthenticated { .. }
    ) {
        return Err("Credential refresh rejected. Sign in again with /login.".into());
    }
    let credential = read(&agent).ok_or("Grok credential unavailable")?;
    let auth = grok_auth(&credential.value).ok_or("Grok credential unavailable")?;
    let access = auth
        .get("key")
        .and_then(Value::as_str)
        .filter(|t| !t.is_empty())
        .ok_or("Grok access credential unavailable")?;
    let expires_in = crate::accounts::jwt_claims(access)
        .and_then(|claims| claims.get("exp").and_then(Value::as_i64))
        .and_then(|seconds| chrono::DateTime::from_timestamp(seconds, 0))
        .map(|expires| {
            expires
                .signed_duration_since(chrono::Utc::now())
                .num_seconds()
                .max(1)
        })
        .or_else(|| {
            auth.get("expires_at")
                .and_then(Value::as_str)
                .and_then(|at| chrono::DateTime::parse_from_rfc3339(at).ok())
                .map(|expires| {
                    expires
                        .signed_duration_since(chrono::Utc::now())
                        .num_seconds()
                        .max(1)
                })
        })
        .unwrap_or(3600);
    write_private(
        &receipt_path,
        &json!({"generation":request_generation(&agent)}),
    )?;
    Ok(json!({"access_token":access,"expires_in":expires_in,"issuer":"router-acp"}))
}

fn helper_agent(provider: &str, dir: &Path) -> Result<AgentConfig, String> {
    let (variable, value) = match provider {
        "grok" => ("GROK_HOME", dir),
        "kimi" => ("KIMI_SHARE_DIR", dir),
        _ => return Err("Unsupported credential helper".into()),
    };
    let config = json!({"agents":[{"name":provider, "command":{"type":"stdio","command":provider, "env":[{"name":variable,"value":value}]}, "model_selection":{"type":"config-option"},"models":[{"id":provider,"cost_rank":1}]}]});
    let config = crate::config::Config::from_yaml(
        &serde_yaml::to_string(&config)
            .map_err(|_| "Credential helper configuration unavailable")?,
    )
    .map_err(|_| "Credential helper configuration unavailable")?;
    Ok(config.agents[0].clone())
}

/// Internal worker for the short-lived Grok hook. It shares the same
/// per-credential repair lock and generation rules as every router session.
pub async fn repair_for_helper(
    provider: &str,
    dir: &Path,
    observed: &str,
) -> Result<RepairOutcome, String> {
    let agent = helper_agent(provider, dir)?;
    Ok(repair(&agent, Some(observed)).await)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    fn fixture(provider: &str) -> (tempfile::TempDir, AgentConfig) {
        let root = tempfile::tempdir().unwrap();
        let dir = root.path().join(format!(".{provider}"));
        std::fs::create_dir(&dir).unwrap();
        let variable = match provider {
            "claude" => "CLAUDE_CONFIG_DIR",
            "codex" => "CODEX_HOME",
            "kimi" => "KIMI_SHARE_DIR",
            _ => "HOME",
        };
        let env_dir = if provider == "grok" {
            root.path()
        } else {
            &dir
        };
        let yaml = format!(
            "agents:\n  - name: {provider}\n    command: {{type: stdio, command: mock-agent, env: [{{name: {variable}, value: {}}}]}}\n    model_selection: {{type: config-option}}\n    models: [{{id: model, cost_rank: 1}}]\n",
            env_dir.display()
        );
        let config = crate::config::Config::from_yaml(&yaml).unwrap();
        let value = match provider {
            "claude" => {
                json!({"claudeAiOauth":{"accessToken":"synthetic-access","refreshToken":"synthetic-refresh","expiresAt":4070908800000u64},"mcpOAuth":{"keep":true}})
            }
            "codex" => {
                json!({"tokens":{"access_token":"synthetic-access","refresh_token":"synthetic-refresh","id_token":"id","account_id":"account"}})
            }
            "kimi" => {
                std::fs::create_dir(dir.join("credentials")).unwrap();
                std::fs::write(
                    dir.join("config.toml"),
                    r#"
default_model = "kimi-k2"
[models.kimi-k2]
provider = "kimi"
model = "kimi-k2"
max_context_size = 256000
[providers.kimi]
type = "kimi"
base_url = "https://api.kimi.com/coding/v1"
api_key = ""
oauth = { storage = "file", key = "oauth/kimi-code" }
[services.moonshot_search]
base_url = "https://api.kimi.com/search"
api_key = ""
oauth = { storage = "keyring", key = "oauth/kimi-code" }
"#,
                )
                .unwrap();
                json!({"access_token":"synthetic-access", "refresh_token":"synthetic-refresh", "expires_at":4070908800u64})
            }
            _ => json!({GROK_LOGIN:{"key":"synthetic-access","refresh_token":"synthetic-refresh"}}),
        };
        write_private(
            &dir.join(match provider {
                "claude" => ".credentials.json",
                "kimi" => "credentials/kimi-code.json",
                _ => "auth.json",
            }),
            &value,
        )
        .unwrap();
        (root, config.agents[0].clone())
    }

    #[tokio::test]
    async fn grok_symlinked_store_keeps_helper_and_runtime_isolation() {
        let (root, agent) = fixture("grok");
        let legacy = root.path().join(".grok");
        let durable = root.path().join("durable-grok");
        std::fs::rename(&legacy, &durable).unwrap();
        #[cfg(unix)]
        std::os::unix::fs::symlink(&durable, &legacy).unwrap();
        let before = std::fs::read(durable.join("auth.json")).unwrap();
        let mut first_env = vec![("GROK_HOME".into(), durable.to_string_lossy().into())];
        let mut second_env = first_env.clone();
        let (first, second) = tokio::join!(
            runtime(&agent, &mut first_env),
            runtime(&agent, &mut second_env)
        );
        let first = first.unwrap().unwrap();
        let second = second.unwrap().unwrap();
        assert_ne!(first.root, second.root);
        for (view, env) in [(&first, &first_env), (&second, &second_env)] {
            assert_eq!(
                env.iter()
                    .filter(|(key, _)| key == "GROK_HOME")
                    .collect::<Vec<_>>(),
                vec![&("GROK_HOME".into(), view.root.to_string_lossy().into())]
            );
            let token = token_for_helper("grok", &durable, false, &view.root)
                .await
                .unwrap();
            assert_eq!(token["access_token"], "synthetic-access");
            assert!(token.get("refresh_token").is_none());
            assert!(!view.root.join("auth.json").exists());
        }
        assert_eq!(std::fs::read(durable.join("auth.json")).unwrap(), before);
        let helper = helper_agent("grok", &durable).unwrap();
        assert_eq!(directory(&agent), directory(&helper));
        let held = lock(&agent).await.unwrap();
        assert!(
            !std::process::Command::new("flock")
                .args(["--nonblock"])
                .arg(directory(&helper).unwrap().join(".router-acp-auth.lock"))
                .arg("true")
                .status()
                .unwrap()
                .success()
        );
        drop(held);
    }

    #[test]
    fn grok_reads_current_principal_key_and_retains_legacy_support() {
        let current = json!({
            "https://auth.x.ai::older": {
                "key": "older-access",
                "refresh_token": "older-refresh",
                "create_time": "2026-10-07T00:00:00Z"
            },
            "https://auth.x.ai::current": {
                "key": "current-access",
                "refresh_token": "current-refresh",
                "create_time": "2026-10-08T00:00:00Z",
                "email": "grok@example.test"
            }
        });
        assert_eq!(grok_auth(&current).unwrap()["key"], "current-access");
        assert_eq!(
            grok_auth_key(&current).as_deref(),
            Some("https://auth.x.ai::current")
        );

        let legacy = json!({GROK_LOGIN: {"key": "legacy-access"}});
        assert_eq!(grok_auth(&legacy).unwrap()["key"], "legacy-access");
        assert_eq!(grok_auth_key(&legacy).as_deref(), Some(GROK_LOGIN));

        let (_root, agent) = fixture("grok");
        std::fs::write(
            directory(&agent).unwrap().join("auth.json"),
            serde_json::to_vec(&current).unwrap(),
        )
        .unwrap();
        assert!(request_generation(&agent).is_some());
        record_success(&agent);
        assert_eq!(availability(&agent), AuthAvailability::Authenticated);
    }

    #[tokio::test]
    async fn router_runtime_config_is_private_complete_and_preserves_custom_toml() {
        for provider in ["codex", "grok"] {
            let (_root, agent) = fixture(provider);
            let canonical = directory(&agent).unwrap().join("config.toml");
            let source = match provider {
                "codex" => {
                    r#"outside = "keep"
[features]
custom = "keep"
[custom]
flag = 7
"#
                }
                "grok" => {
                    r#"outside = "keep"
[folder_trust]
enabled = true
scope = "custom"
[skills]
disabled = ["resume-codex", "custom-disabled"]
ignore = ["custom/path"]
custom_setting = "keep"
[custom]
flag = 7
"#
                }
                _ => unreachable!(),
            };
            std::fs::write(&canonical, source).unwrap();
            let before = std::fs::read(&canonical).unwrap();

            let mut first_env = vec![];
            let mut second_env = vec![];
            let first = runtime(&agent, &mut first_env).await.unwrap().unwrap();
            let second = runtime(&agent, &mut second_env).await.unwrap().unwrap();
            let first_config = first.root.join("config.toml");
            let second_config = second.root.join("config.toml");
            assert_ne!(first.root, second.root);
            assert!(
                !std::fs::symlink_metadata(&first_config)
                    .unwrap()
                    .file_type()
                    .is_symlink()
            );
            assert!(
                !std::fs::symlink_metadata(&second_config)
                    .unwrap()
                    .file_type()
                    .is_symlink()
            );
            assert_eq!(std::fs::read(&canonical).unwrap(), before);

            let config: toml::Value =
                toml::from_str(&std::fs::read_to_string(&first_config).unwrap()).unwrap();
            assert_eq!(config["outside"].as_str(), Some("keep"));
            assert_eq!(config["custom"]["flag"].as_integer(), Some(7));
            match provider {
                "codex" => {
                    assert_eq!(config["features"]["custom"].as_str(), Some("keep"));
                    assert_eq!(
                        config["features"]["default_mode_request_user_input"].as_bool(),
                        Some(true)
                    );
                }
                "grok" => {
                    assert_eq!(config["folder_trust"]["scope"].as_str(), Some("custom"));
                    assert_eq!(config["folder_trust"]["enabled"].as_bool(), Some(false));
                    assert_eq!(config["skills"]["custom_setting"].as_str(), Some("keep"));
                    let disabled = config["skills"]["disabled"].as_array().unwrap();
                    for name in [
                        "resume-codex",
                        "resume-claude",
                        "resume-cursor",
                        "custom-disabled",
                    ] {
                        assert!(disabled.iter().any(|value| value.as_str() == Some(name)));
                    }
                    assert_eq!(
                        disabled
                            .iter()
                            .filter(|value| value.as_str() == Some("resume-codex"))
                            .count(),
                        1
                    );
                    let ignored = config["skills"]["ignore"].as_array().unwrap();
                    for path in [
                        "~/.grok/bundled/skills/resume-codex",
                        "~/.grok/bundled/skills/resume-claude",
                        "~/.grok/bundled/skills/resume-cursor",
                        "custom/path",
                    ] {
                        assert!(ignored.iter().any(|value| value.as_str() == Some(path)));
                    }
                }
                _ => unreachable!(),
            }

            let second_before = std::fs::read(&second_config).unwrap();
            write_private_bytes(&first_config, b"[runtime]\nchanged = true\n").unwrap();
            assert_eq!(std::fs::read(&canonical).unwrap(), before);
            assert_eq!(std::fs::read(&second_config).unwrap(), second_before);
        }
    }

    #[tokio::test]
    async fn provider_session_state_is_private_to_each_runtime() {
        for provider in ["claude", "codex", "grok", "kimi"] {
            let (_root, agent) = fixture(provider);
            let canonical = directory(&agent).unwrap();
            for name in [
                "projects",
                "sessions",
                "archived_sessions",
                "history.jsonl",
                "session_log",
                "session.db",
                "sessions.db",
            ] {
                std::fs::create_dir_all(canonical.join(name)).unwrap();
            }

            let mut env = vec![];
            let runtime = runtime(&agent, &mut env).await.unwrap().unwrap();
            for name in [
                "projects",
                "sessions",
                "archived_sessions",
                "history.jsonl",
                "session_log",
                "session.db",
                "sessions.db",
            ] {
                assert!(
                    !runtime.root.join(name).exists(),
                    "{provider} leaked {name}"
                );
                assert!(
                    canonical.join(name).is_dir(),
                    "{provider} changed canonical {name}"
                );
            }
        }
    }

    #[test]
    fn grok_home_precedes_legacy_home_and_uses_canonical_identity() {
        let (root, mut agent) = fixture("grok");
        let custom = root.path().join("custom-store");
        std::fs::create_dir(&custom).unwrap();
        agent.command.env.push(crate::config::EnvVarConfig {
            name: "GROK_HOME".into(),
            value: custom.to_string_lossy().into(),
        });
        assert_eq!(directory(&agent), Some(custom.canonicalize().unwrap()));
    }

    #[test]
    fn credential_presence_is_independent_of_authentication_evidence() {
        let (_root, agent) = fixture("kimi");
        assert!(present(&agent));
        assert_eq!(availability(&agent), AuthAvailability::Unknown);

        std::fs::remove_file(
            directory(&agent)
                .unwrap()
                .join("credentials/kimi-code.json"),
        )
        .unwrap();
        assert!(!present(&agent));
        assert_eq!(availability(&agent), AuthAvailability::Unknown);
    }

    #[tokio::test]
    async fn concurrent_errors_share_one_successful_repair_for_every_provider() {
        for provider in ["claude", "codex", "grok", "kimi"] {
            let (_root, agent) = fixture(provider);
            let observed = request_generation(&agent).unwrap();
            let calls = AtomicUsize::new(0);
            let action = |value, _| async {
                calls.fetch_add(1, Ordering::SeqCst);
                tokio::time::sleep(Duration::from_millis(50)).await;
                Refresh {
                    outcome: RepairOutcome::Repaired,
                    value: Some(value),
                }
            };
            let (first, second) = tokio::join!(
                repair_with(&agent, Some(&observed), action),
                repair_with(&agent, Some(&observed), action)
            );
            assert_eq!(
                (first, second),
                (RepairOutcome::Repaired, RepairOutcome::Repaired)
            );
            assert_eq!(calls.load(Ordering::SeqCst), 1);
            assert_eq!(availability(&agent), AuthAvailability::Authenticated);
            // Even unchanged access tokens have a new shared repair epoch.
            assert_ne!(
                request_generation(&agent).as_deref(),
                Some(observed.as_str())
            );
            let stale = repair_with(&agent, Some(&observed), |_, _| async {
                panic!("stale error attempted another refresh")
            })
            .await;
            assert_eq!(stale, RepairOutcome::Repaired);
        }
    }

    #[tokio::test]
    async fn only_definitive_refresh_failure_rejects_and_waiters_share_it() {
        let (_root, agent) = fixture("claude");
        let observed = request_generation(&agent).unwrap();
        let calls = AtomicUsize::new(0);
        let action = |_, _| async {
            calls.fetch_add(1, Ordering::SeqCst);
            tokio::time::sleep(Duration::from_millis(50)).await;
            Refresh {
                outcome: RepairOutcome::Rejected,
                value: None,
            }
        };
        let (first, second) = tokio::join!(
            repair_with(&agent, Some(&observed), action),
            repair_with(&agent, Some(&observed), action)
        );
        assert_eq!(
            (first, second),
            (RepairOutcome::Rejected, RepairOutcome::Rejected)
        );
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert!(matches!(
            availability(&agent),
            AuthAvailability::Unauthenticated { .. }
        ));
        assert!(crate::accounts::registered(&agent));
    }

    #[tokio::test]
    async fn transient_failure_and_missing_evidence_never_reject() {
        let (_root, agent) = fixture("codex");
        assert_eq!(repair(&agent, None).await, RepairOutcome::Unknown);
        let observed = request_generation(&agent).unwrap();
        let first = repair_with(&agent, Some(&observed), |_, _| async {
            Refresh {
                outcome: RepairOutcome::Unknown,
                value: None,
            }
        })
        .await;
        assert_eq!(first, RepairOutcome::Unknown);
        assert_eq!(availability(&agent), AuthAvailability::Unknown);
        let current = request_generation(&agent).unwrap();
        let repeated = repair_with(&agent, Some(&current), |_, _| async {
            panic!("transient repair cooldown ignored")
        })
        .await;
        assert_eq!(repeated, RepairOutcome::Unknown);
    }

    #[tokio::test]
    async fn cached_success_does_not_clear_definitive_rejection() {
        let (_root, agent) = fixture("claude");
        let observed = request_generation(&agent).unwrap();
        let access = access_generation(&agent).unwrap();
        repair_with(&agent, Some(&observed), |_, _| async {
            Refresh {
                outcome: RepairOutcome::Rejected,
                value: None,
            }
        })
        .await;
        observe_success(&agent, &access).await;
        assert!(matches!(
            availability(&agent),
            AuthAvailability::Unauthenticated { .. }
        ));
        let mut newer = read(&agent).unwrap();
        newer.value["claudeAiOauth"]["accessToken"] = "replacement-access".into();
        write_private(&newer.path, &newer.value).unwrap();
        observe_success(&agent, &access_generation(&agent).unwrap()).await;
        assert_eq!(availability(&agent), AuthAvailability::Authenticated);
    }

    #[tokio::test]
    async fn newer_credential_wins_over_delayed_rejection_or_success() {
        for outcome in [RepairOutcome::Rejected, RepairOutcome::Repaired] {
            let (_root, agent) = fixture("claude");
            let observed = request_generation(&agent).unwrap();
            let current = read(&agent).unwrap();
            let result = repair_with(&agent, Some(&observed), |mut value, _| async move {
                value["claudeAiOauth"]["accessToken"] = "new-synthetic-access".into();
                write_private(&current.path, &value).unwrap();
                Refresh {
                    outcome,
                    value: Some(current.value),
                }
            })
            .await;
            assert_eq!(result, RepairOutcome::Repaired);
            assert_eq!(
                read(&agent).unwrap().value["claudeAiOauth"]["accessToken"],
                "new-synthetic-access"
            );
            assert!(!matches!(
                availability(&agent),
                AuthAvailability::Unauthenticated { .. }
            ));
        }
    }

    #[tokio::test]
    async fn different_accounts_repair_independently() {
        let (_root1, a) = fixture("claude");
        let (_root2, b) = fixture("claude");
        let _held = lock(&a).await.unwrap();
        let observed = request_generation(&b).unwrap();
        let result = tokio::time::timeout(
            Duration::from_secs(1),
            repair_with(&b, Some(&observed), |value, _| async {
                Refresh {
                    outcome: RepairOutcome::Repaired,
                    value: Some(value),
                }
            }),
        )
        .await
        .unwrap();
        assert_eq!(result, RepairOutcome::Repaired);
    }

    #[tokio::test]
    async fn cancelling_a_waiter_does_not_cancel_token_publication() {
        let (_root, agent) = fixture("claude");
        let observed = request_generation(&agent).unwrap();
        let started = std::sync::Arc::new(tokio::sync::Notify::new());
        let release = std::sync::Arc::new(tokio::sync::Notify::new());
        let waiter = tokio::spawn({
            let agent = agent.clone();
            let observed = observed.clone();
            let started = started.clone();
            let release = release.clone();
            async move {
                durable_repair(&agent, Some(&observed), move |mut value, _| async move {
                    started.notify_one();
                    release.notified().await;
                    value["claudeAiOauth"]["accessToken"] = "rotated-access".into();
                    Refresh {
                        outcome: RepairOutcome::Repaired,
                        value: Some(value),
                    }
                })
                .await
            }
        });
        started.notified().await;
        waiter.abort();
        let _ = waiter.await;
        release.notify_one();
        // A concurrent request still waits on the original repair and reuses
        // its published result. Its action must never rotate a second time.
        let result = repair_with(&agent, Some(&observed), |_, _| async {
            panic!("cancelled session released the repair lock early")
        })
        .await;
        assert_eq!(result, RepairOutcome::Repaired);
        assert_eq!(
            read(&agent).unwrap().value["claudeAiOauth"]["accessToken"],
            "rotated-access"
        );
    }

    #[tokio::test]
    async fn missing_credentials_do_not_expose_an_adapter_default_store() {
        for provider in ["claude", "codex", "grok", "kimi"] {
            let (_root, agent) = fixture(provider);
            let canonical = read(&agent).unwrap().path;
            std::fs::remove_file(&canonical).unwrap();
            let mut env = crate::accounts::AUTH_ENV
                .into_iter()
                .map(|key| (key.into(), "ambient-token".into()))
                .collect();
            let view = runtime(&agent, &mut env).await.unwrap().unwrap();
            let variable = match provider {
                "claude" => "CLAUDE_CONFIG_DIR",
                "codex" => "CODEX_HOME",
                "kimi" => "KIMI_SHARE_DIR",
                _ => "HOME",
            };
            let directory = env
                .iter()
                .find(|(key, _)| key == variable)
                .unwrap()
                .1
                .clone();
            assert!(Path::new(&directory).starts_with(&view.root));
            assert!(
                !env.iter()
                    .any(|(key, _)| crate::accounts::AUTH_ENV.contains(&key.as_str()))
            );
            assert!(!canonical.exists());
            assert_eq!(availability(&agent), AuthAvailability::Unknown);
        }
    }

    #[tokio::test]
    async fn runtime_success_cannot_confirm_a_newer_canonical_login() {
        let (_root, agent) = fixture("claude");
        let mut env = vec![];
        let view = runtime(&agent, &mut env).await.unwrap().unwrap();
        let mut current = read(&agent).unwrap();
        current.value["claudeAiOauth"]["accessToken"] = "different-access".into();
        write_private(&current.path, &current.value).unwrap();
        observe_request_success(&agent, &view.generation).await;
        assert_eq!(availability(&agent), AuthAvailability::Unknown);
    }

    #[tokio::test]
    async fn os_lock_is_shared_with_other_processes_and_released_on_drop() {
        let (_root, agent) = fixture("claude");
        let held = lock(&agent).await.unwrap();
        let path = directory(&agent).unwrap().join(".router-acp-auth.lock");
        let try_other = || {
            std::process::Command::new("flock")
                .arg("--nonblock")
                .arg(&path)
                .arg("true")
                .status()
                .unwrap()
                .success()
        };
        assert!(!try_other());
        drop(held);
        assert!(try_other());
    }

    #[tokio::test]
    async fn adapters_cannot_overwrite_or_refresh_canonical_credentials() {
        for provider in ["claude", "codex", "grok", "kimi"] {
            let (_root, agent) = fixture(provider);
            let original = read(&agent).unwrap();
            let before = std::fs::read(&original.path).unwrap();
            let mut env = vec![];
            let view = runtime(&agent, &mut env).await.unwrap().unwrap();
            if provider == "grok" {
                let helper = env
                    .iter()
                    .find_map(|(k, v)| (k == "GROK_AUTH_PROVIDER_COMMAND").then_some(v))
                    .unwrap();
                assert_eq!(
                    Path::new(helper),
                    view.root.join(".router-acp-auth-provider")
                );
                let metadata = std::fs::metadata(helper).unwrap();
                #[cfg(unix)]
                {
                    use std::os::unix::fs::PermissionsExt;
                    assert_eq!(metadata.permissions().mode() & 0o777, 0o700);
                }
                let output = std::process::Command::new(helper).output().unwrap();
                assert!(output.status.success());
                let returned: Value = serde_json::from_slice(&output.stdout).unwrap();
                assert!(returned.get("refresh_token").is_none());
                let returned =
                    token_for_helper("grok", &directory(&agent).unwrap(), false, &view.root)
                        .await
                        .unwrap();
                assert!(returned.get("refresh_token").is_none());
            } else if provider == "kimi" {
                let private = view.root.join("config.toml");
                let text = std::fs::read_to_string(&private).unwrap();
                let config: Value = toml::from_str(&text).unwrap();
                assert!(config["providers"]["kimi"].get("oauth").is_none());
                assert!(config["services"]["moonshot_search"].get("oauth").is_none());
                assert_eq!(config["providers"]["kimi"]["api_key"], "synthetic-access");
                assert_eq!(
                    config["services"]["moonshot_search"]["api_key"],
                    "synthetic-access"
                );
                assert!(!text.contains("synthetic-refresh"));
                assert!(!view.root.join("credentials").exists());
                write_private_bytes(&private, b"").unwrap();
            } else {
                let private = view.root.join(original.path.file_name().unwrap());
                let text = std::fs::read_to_string(&private).unwrap();
                assert!(!text.contains("synthetic-refresh"));
                write_private(&private, &json!({})).unwrap();
            }
            assert_eq!(std::fs::read(&original.path).unwrap(), before);
            let path = view.root.clone();
            drop(view);
            assert!(!path.exists());
        }
    }

    #[tokio::test]
    async fn shared_status_contains_fingerprints_and_private_permissions() {
        let (_root, agent) = fixture("claude");
        let observed = request_generation(&agent).unwrap();
        repair_with(&agent, Some(&observed), |_, _| async {
            Refresh {
                outcome: RepairOutcome::Rejected,
                value: None,
            }
        })
        .await;
        let path = directory(&agent).unwrap().join(".router-acp-auth.json");
        let text = std::fs::read_to_string(&path).unwrap();
        assert!(!text.contains("synthetic-access") && !text.contains("synthetic-refresh"));
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            assert_eq!(
                std::fs::metadata(path).unwrap().permissions().mode() & 0o777,
                0o600
            );
        }
    }

    #[tokio::test]
    async fn kimi_refresh_uses_native_form_protocol_and_only_rejects_invalid_grant() {
        use axum::{Form, Json, Router, http::StatusCode, routing::post};
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let host = format!("http://{}", listener.local_addr().unwrap());
        let app = Router::new().route("/api/oauth/token", post(|Form(form): Form<std::collections::HashMap<String, String>>| async move {
            assert_eq!(form["client_id"], "17e5f671-d194-4dfb-9706-5516cb48c098");
            assert_eq!(form["grant_type"], "refresh_token");
            match form["refresh_token"].as_str() {
                "invalid" => (StatusCode::BAD_REQUEST, Json(json!({"error":"invalid_grant"}))),
                "transient" => (StatusCode::UNAUTHORIZED, Json(json!({"error":"temporarily_unavailable"}))),
                _ => (StatusCode::OK, Json(json!({"access_token":"rotated-access", "refresh_token":"rotated-refresh", "expires_in":3600}))),
            }
        }));
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        for (token, expected) in [
            ("valid", RepairOutcome::Repaired),
            ("invalid", RepairOutcome::Rejected),
            ("transient", RepairOutcome::Unknown),
        ] {
            let result = refresh_kimi(json!({"refresh_token":token}), &host).await;
            assert_eq!(result.outcome, expected);
            if let Some(value) = result.value {
                assert_eq!(value["access_token"], "rotated-access");
                assert_eq!(value["refresh_token"], "rotated-refresh");
                assert!(value["expires_at"].as_i64().unwrap() > chrono::Utc::now().timestamp());
            }
        }
        server.abort();
    }

    #[tokio::test]
    async fn kimi_cannot_bypass_private_configuration_in_a_spawn_or_probe() {
        let (_root, agent) = fixture("kimi");
        for kind in 0..3 {
            let mut agent = agent.clone();
            let args = vec!["--config-file".into(), "/canonical/config.toml".into()];
            match kind {
                0 => agent.command.args = args,
                1 => {
                    agent.model_selection = crate::config::ModelSelectionConfig::SpawnConfig {
                        process_template: serde_yaml::from_str(
                            "args: [--config-file, /canonical/config.toml, acp]",
                        )
                        .unwrap(),
                    }
                }
                _ => {
                    agent.auth_probe = Some(crate::config::AuthProbeConfig {
                        command: "kimi".into(),
                        args,
                        timeout_ms: 2000,
                        unauthenticated_patterns: vec![],
                    })
                }
            }
            let result = runtime(&agent, &mut vec![]).await;
            assert!(matches!(result, Err(message) if message.contains("default config location")));
        }
    }
}
