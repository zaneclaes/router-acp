//! Machine-readable configuration commands for hosts.
//!
//! A host reads and replaces the router configuration only through
//! `config-show`, `config-apply`, `account-reserve` and `account-order`. It
//! never opens the file, its lock, or anything beside it. Account entries
//! belong to the router: a replacement keeps every account's directory,
//! membership, priority, reserve and login settings as the router last wrote
//! them, so a host's settings save cannot undo a login or a seat migration.
//! Writes take the same accounts lock as native `/login`, validate the
//! result, and replace the file atomically. A written configuration then gets
//! the router's seat migration (`accounts::adopt_seat_directories`), so a
//! host's boot write settles account ids before any session or login starts.

use std::path::Path;

use serde_json::Value;
use sha2::{Digest, Sha256};

use crate::accounts::{ConfigLock, provider, registered, save_document, set_account_fields};
use crate::config::Config;

/// Agent fields the router owns on an account entry.
const ACCOUNT_FIELDS: [&str; 10] = [
    "command",
    "accounts",
    "account_disabled",
    "account_priority",
    "account_plan",
    "reserve_capacity",
    "login_command",
    "auth_probe",
    "usage_source",
    "lineage",
];

#[derive(Debug)]
pub enum AdminError {
    /// The configuration changed, or another write holds its lock. Re-read
    /// and retry.
    Conflict(String),
    Invalid(String),
}

impl std::fmt::Display for AdminError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Conflict(m) | Self::Invalid(m) => f.write_str(m),
        }
    }
}

fn invalid(e: impl std::fmt::Display) -> AdminError {
    AdminError::Invalid(e.to_string())
}

/// Opaque identity of one configuration revision.
pub fn revision(raw: &[u8]) -> String {
    format!("{:x}", Sha256::digest(raw))
}

fn read(path: &Path) -> Result<Option<String>, AdminError> {
    crate::project_config::read(path).map_err(invalid)
}

fn shown(raw: Option<&str>) -> Result<Value, AdminError> {
    let config: Option<Value> = raw.map(serde_yaml::from_str).transpose().map_err(invalid)?;
    let effective: Option<Value> = raw
        .map(Config::effective_document)
        .transpose()
        .map_err(invalid)?;
    Ok(serde_json::json!({
        "revision": raw.map(|r| revision(r.as_bytes())),
        "config": config,
        "effective": effective,
    }))
}

/// `{revision, config, effective}`: the uninterpolated document plus provider
/// presets and section defaults for host settings surfaces, or nulls when no
/// configuration exists yet.
pub fn show(path: &Path) -> Result<Value, AdminError> {
    shown(read(path)?.as_deref())
}

fn lock(path: &Path) -> Result<ConfigLock, AdminError> {
    ConfigLock::acquire(path.to_path_buf()).map_err(AdminError::Conflict)
}

/// Replace the configuration with `proposed`, keeping the router's account
/// entries. `expect` refuses the write when the file changed since that
/// revision. `dry_run` validates and returns the result without writing.
pub fn apply(
    path: &Path,
    mut proposed: Value,
    expect: Option<&str>,
    dry_run: bool,
) -> Result<Value, AdminError> {
    if !proposed.is_object() {
        return Err(invalid("The configuration must be an object"));
    }
    let _lock = if dry_run { None } else { Some(lock(path)?) };
    let raw = read(path)?;
    if let Some(expect) = expect
        && raw.as_deref().map(|r| revision(r.as_bytes())).as_deref() != Some(expect)
    {
        return Err(AdminError::Conflict(
            "The router configuration changed; reload before saving".into(),
        ));
    }
    if let Some(raw) = raw.as_deref() {
        let live: Value = serde_yaml::from_str(raw).map_err(invalid)?;
        keep_router_accounts(&mut proposed, &live);
    }
    if dry_run {
        let yaml = serde_yaml::to_string(&proposed).map_err(invalid)?;
        Config::from_yaml(&yaml).map_err(invalid)?;
        return Ok(serde_json::json!({"revision": null, "config": proposed}));
    }
    save_document(path, &proposed).map_err(invalid)?;
    drop(_lock);
    // Seats become accounts now, under the router's own lock, rather than
    // in the middle of the next session or login that starts a router.
    match crate::accounts::adopt_seat_directories(path) {
        Ok(report) => report.iter().for_each(|line| eprintln!("{line}")),
        Err(e) => eprintln!("{e}"),
    }
    show(path)
}

/// Set an account's reserve percentages.
pub fn set_reserve(
    path: &Path,
    account: &str,
    weekly: f64,
    session: f64,
) -> Result<Value, AdminError> {
    if ![weekly, session]
        .iter()
        .all(|p| p.is_finite() && (0.0..=100.0).contains(p))
    {
        return Err(invalid("A reserve is a percentage from 0 to 100"));
    }
    edit_accounts(path, |doc, cfg| {
        if !cfg
            .agents
            .iter()
            .any(|a| a.name == account && registered(a))
        {
            return Err(AdminError::Conflict(
                "Connected accounts changed; reload before saving".into(),
            ));
        }
        let value = serde_json::json!({"weekly": weekly, "session": session});
        set_account_fields(
            doc,
            account,
            &[("reserve_capacity", value.clone())],
            &[("reserve_capacity", value)],
        )
        .map_err(invalid)
    })
}

/// Order one provider's accounts, first drained first. `accounts` must name
/// every connected account of that provider exactly once.
pub fn set_order(path: &Path, group: &str, accounts: &[String]) -> Result<Value, AdminError> {
    edit_accounts(path, |doc, cfg| {
        let mut current: Vec<&str> = cfg
            .agents
            .iter()
            .filter(|a| registered(a) && provider(a) == Some(group))
            .map(|a| a.name.as_str())
            .collect();
        let mut requested: Vec<&str> = accounts.iter().map(String::as_str).collect();
        current.sort_unstable();
        requested.sort_unstable();
        if current.is_empty() || current != requested {
            return Err(AdminError::Conflict(
                "Connected accounts changed; reload before saving their order".into(),
            ));
        }
        for (index, account) in accounts.iter().enumerate() {
            // Distinct explicit priorities 1..=N, as `/login` numbers them.
            let priority = serde_json::json!(index + 1);
            set_account_fields(
                doc,
                account,
                &[("account_priority", priority.clone())],
                &[("priority", priority)],
            )
            .map_err(invalid)?;
        }
        Ok(())
    })
}

fn edit_accounts(
    path: &Path,
    edit: impl FnOnce(&mut Value, &Config) -> Result<(), AdminError>,
) -> Result<Value, AdminError> {
    let _lock = lock(path)?;
    let raw = read(path)?.ok_or_else(|| invalid("No router configuration exists yet"))?;
    let cfg = Config::from_yaml(&raw).map_err(invalid)?;
    // Edit the uninterpolated document, so env secrets never become file values.
    let mut doc: Value = serde_yaml::from_str(&raw).map_err(invalid)?;
    edit(&mut doc, &cfg)?;
    save_document(path, &doc).map_err(invalid)?;
    show(path)
}

/// The provider an `agents[]` entry signs in to, as `accounts::provider`
/// decides it for a loaded agent.
fn entry_provider(entry: &Value) -> Option<&'static str> {
    match entry.pointer("/usage_source/type").and_then(Value::as_str) {
        Some("anthropic-oauth") => return Some("claude"),
        Some("codex-rollout") => return Some("codex"),
        _ => {}
    }
    let name = entry.get("name").and_then(Value::as_str)?;
    let base = name.split('@').next()?;
    ["claude", "codex", "grok", "kimi"]
        .into_iter()
        .find(|p| *p == base)
        .or_else(|| {
            let command = entry.pointer("/command/command").and_then(Value::as_str)?;
            match Path::new(command).file_name()?.to_str()? {
                "claude-agent-acp" | "claude-acp" => Some("claude"),
                "codex-acp" => Some("codex"),
                "grok" => Some("grok"),
                "kimi" => Some("kimi"),
                _ => None,
            }
        })
}

fn name_of(entry: &Value) -> Option<&str> {
    entry.get("name").and_then(Value::as_str)
}

/// Every live account entry survives. A proposed entry of the same name, or
/// of the name it had before it became an account (`claude` for
/// `claude@<id>`, when no live entry still has that name), supplies its
/// models and other settings; the live entry keeps its `ACCOUNT_FIELDS`. A
/// proposed account the router does not have is dropped: accounts are added
/// through `/login`. Other proposed agents are kept as proposed.
fn keep_router_accounts(proposed: &mut Value, live: &Value) {
    let Some(live_agents) = live.get("agents").and_then(Value::as_array) else {
        return;
    };
    let offered: Vec<Value> = proposed
        .get("agents")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    let live_named = |name: &str| live_agents.iter().any(|a| name_of(a) == Some(name));
    let mut agents: Vec<Value> = Vec::new();
    for current in live_agents.iter().filter(|a| entry_provider(a).is_some()) {
        let Some(name) = name_of(current) else {
            agents.push(current.clone());
            continue;
        };
        let former = name.split_once('@').map(|(base, _)| base);
        let offer = offered
            .iter()
            .find(|a| name_of(a) == Some(name))
            .or_else(|| {
                let former = former.filter(|base| !live_named(base))?;
                offered.iter().find(|a| name_of(a) == Some(former))
            });
        let Some(offer) = offer.and_then(Value::as_object) else {
            agents.push(current.clone());
            continue;
        };
        let mut next = offer.clone();
        next.insert("name".into(), Value::String(name.to_string()));
        for field in ACCOUNT_FIELDS {
            match current.get(field) {
                Some(value) => {
                    next.insert(field.into(), value.clone());
                }
                // A plan label a host supplies stands until the router knows one.
                None if field == "account_plan" => {}
                None => {
                    next.remove(field);
                }
            }
        }
        agents.push(Value::Object(next));
    }
    for offer in offered {
        let keep = entry_provider(&offer).is_none()
            && !agents
                .iter()
                .any(|a| name_of(a).is_some() && name_of(a) == name_of(&offer));
        if keep {
            agents.push(offer);
        }
    }
    proposed["agents"] = Value::Array(agents);
}

#[cfg(test)]
mod tests {
    use super::*;

    fn agent(name: &str, extra: &str) -> String {
        format!(
            "  - name: {name}\n    command: {{type: stdio, command: mock-agent{extra}}}\n    model_selection: {{type: config-option}}\n    models: [{{id: opus, cost_rank: 3}}]\n"
        )
    }

    fn fixture(agents: &str) -> (tempfile::TempDir, std::path::PathBuf) {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("router.yaml");
        std::fs::write(&path, format!("agents:\n{agents}")).unwrap();
        (dir, path)
    }

    fn doc(path: &Path) -> Value {
        show(path).unwrap()["config"].clone()
    }

    #[test]
    fn show_includes_effective_defaults_without_expanding_environment() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("router.yaml");
        std::fs::write(
            &path,
            "state_file: ${ROUTER_STATE_FILE}\nagents:\n  - name: claude\n    preference: 0.1\n",
        )
        .unwrap();

        let shown = show(&path).unwrap();
        assert!(shown["config"]["agents"][0].get("models").is_none());
        assert!(
            !shown["effective"]["agents"][0]["models"]
                .as_array()
                .unwrap()
                .is_empty()
        );
        assert_eq!(shown["effective"]["state_file"], "${ROUTER_STATE_FILE}");
        assert!(shown["effective"]["routers"].get("escalation").is_some());
    }

    #[test]
    fn an_unchanged_apply_keeps_the_file_identity_hosts_watch() {
        let (_dir, path) = fixture(&agent("helper", ""));
        // Normalize once, then reapply the same document.
        apply(&path, doc(&path), None, false).unwrap();
        let identity = |p: &Path| {
            let m = std::fs::metadata(p).unwrap();
            #[cfg(unix)]
            let inode = std::os::unix::fs::MetadataExt::ino(&m);
            #[cfg(not(unix))]
            let inode = 0;
            (inode, m.modified().unwrap())
        };
        let before = identity(&path);
        std::thread::sleep(std::time::Duration::from_millis(20));
        apply(&path, doc(&path), None, false).unwrap();
        assert_eq!(identity(&path), before);
    }

    #[test]
    fn apply_keeps_a_migrated_seat_and_takes_its_former_settings() {
        let seat = ", env: [{name: CLAUDE_CONFIG_DIR, value: /seats/claude@one}]";
        let (_dir, path) = fixture(&format!(
            "{}{}",
            agent("claude@one", seat).replace(
                "    model_selection",
                "    lineage: claude\n    account_priority: 2\n    model_selection"
            ),
            agent("helper", ""),
        ));
        let before = show(&path).unwrap()["revision"].clone();
        let mut proposed = doc(&path);
        // The host still proposes the seat under its pre-account name, with
        // a catalog update, and an account the router never added.
        proposed["agents"] = serde_json::json!([
            {"name": "claude", "command": {"type": "stdio", "command": "mock-agent"},
             "model_selection": {"type": "config-option"},
             "models": [{"id": "opus", "cost_rank": 3}, {"id": "sonnet", "cost_rank": 2}]},
            {"name": "codex@ghost", "command": {"type": "stdio", "command": "mock-agent"},
             "model_selection": {"type": "config-option"}, "models": [{"id": "x", "cost_rank": 1}]},
            {"name": "helper", "command": {"type": "stdio", "command": "mock-agent"},
             "model_selection": {"type": "config-option"}, "models": [{"id": "h", "cost_rank": 1}]},
        ]);
        let shown = apply(&path, proposed, before.as_str(), false).unwrap();
        let agents = shown["config"]["agents"].as_array().unwrap();
        let names: Vec<_> = agents.iter().filter_map(name_of).collect();
        assert_eq!(names, ["claude@one", "helper"]);
        assert_eq!(agents[0]["models"].as_array().unwrap().len(), 2);
        assert_eq!(agents[0]["account_priority"], 2);
        assert_eq!(agents[0]["lineage"], "claude");
        assert_eq!(agents[0]["command"]["env"][0]["value"], "/seats/claude@one");
        assert_eq!(agents[1]["models"][0]["id"], "h");
        assert_ne!(shown["revision"], before);
    }

    #[test]
    fn apply_settles_seat_accounts_before_any_router_starts() {
        let dir = tempfile::tempdir().unwrap();
        let home = dir.path().canonicalize().unwrap();
        std::fs::create_dir_all(home.join(".grok")).unwrap();
        std::fs::write(home.join(".grok").join("auth.json"), "{}").unwrap();
        let path = home.join("router.yaml");
        let proposed = serde_json::json!({"agents": [{"name": "grok",
            "command": {"type": "stdio", "command": "mock-agent",
                "env": [{"name": "HOME", "value": home.display().to_string()}]},
            "model_selection": {"type": "config-option"}, "models": [{"id": "m", "cost_rank": 1}]}]});
        let shown = apply(&path, proposed, None, false).unwrap();
        let seat = &shown["config"]["agents"][0];
        assert!(name_of(seat).unwrap().starts_with("grok@"), "{seat}");
        assert_eq!(seat["lineage"], "grok");
        assert_eq!(shown["revision"], revision(&std::fs::read(&path).unwrap()));
    }

    #[test]
    fn apply_refuses_a_stale_revision_and_dry_run_writes_nothing() {
        let (_dir, path) = fixture(&agent("helper", ""));
        let raw = std::fs::read(&path).unwrap();
        let proposed = doc(&path);
        assert!(matches!(
            apply(&path, proposed.clone(), Some("stale"), false),
            Err(AdminError::Conflict(_))
        ));
        let mut invalid_doc = proposed.clone();
        invalid_doc["unknown_key"] = serde_json::json!(true);
        assert!(matches!(
            apply(&path, invalid_doc, None, true),
            Err(AdminError::Invalid(_))
        ));
        apply(&path, proposed, None, true).unwrap();
        assert_eq!(std::fs::read(&path).unwrap(), raw);
    }

    #[test]
    fn reserve_and_order_edit_only_connected_accounts() {
        let metered = |name: &str| {
            agent(name, "").replace(
                "    model_selection",
                "    usage_source: {type: anthropic-oauth}\n    model_selection",
            )
        };
        let (_dir, path) = fixture(&format!(
            "{}{}{}",
            metered("claude@a"),
            metered("claude@b"),
            agent("codex@c", ""),
        ));
        let raw = std::fs::read_to_string(&path).unwrap();
        std::fs::write(&path, format!("cordon: {{enabled: true}}\n{raw}")).unwrap();
        set_reserve(&path, "claude@b", 15.0, 25.0).unwrap();
        assert_eq!(
            doc(&path)["agents"][1]["reserve_capacity"],
            serde_json::json!({"weekly": 15.0, "session": 25.0})
        );
        assert!(matches!(
            set_reserve(&path, "claude@gone", 1.0, 1.0),
            Err(AdminError::Conflict(_))
        ));
        assert!(matches!(
            set_reserve(&path, "claude@a", 101.0, 1.0),
            Err(AdminError::Invalid(_))
        ));
        set_order(&path, "claude", &["claude@b".into(), "claude@a".into()]).unwrap();
        let agents = doc(&path)["agents"].clone();
        assert_eq!(agents[0]["account_priority"], 2);
        assert_eq!(agents[1]["account_priority"], 1);
        assert!(agents[2].get("account_priority").is_none());
        assert!(matches!(
            set_order(&path, "claude", &["claude@a".into()]),
            Err(AdminError::Conflict(_))
        ));
    }
}
