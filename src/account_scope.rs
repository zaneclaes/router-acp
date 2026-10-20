//! Account membership and reserves in the system and project configuration layers.
use std::path::{Path, PathBuf};
use std::sync::Arc;

use serde_json::{Value, json};

use crate::accounts::{ConfigLock, save_document, save_private, set_account_fields};
use crate::config::{Config, ReserveCapacityConfig};
use crate::project_config;
use crate::session::Shared;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Scope {
    System,
    Project,
}

impl Scope {
    pub fn label(self) -> &'static str {
        match self {
            Self::System => "System",
            Self::Project => "Project",
        }
    }
}

pub struct Paths {
    pub system: PathBuf,
    pub project: PathBuf,
}

pub fn project_path(cwd: &Path) -> PathBuf {
    for dir in cwd.ancestors() {
        let file = dir.join(".router-acp/router.yaml");
        if file.exists() || dir.join(".git").exists() {
            return file;
        }
    }
    cwd.join(".router-acp/router.yaml")
}

pub fn active_path(shared: &Shared) -> Result<PathBuf, String> {
    shared
        .runtime_config()
        .source_path
        .ok_or("Account management needs a file-backed configuration".into())
}

impl Paths {
    pub fn for_session(shared: &Shared, sid: &str) -> Result<Self, String> {
        let original = shared
            .cfg
            .source_path
            .as_ref()
            .ok_or("Account management needs a file-backed configuration")?;
        let active = active_path(shared)?;
        let system = if project_config::is_project(original) {
            project_config::user_path()?
        } else {
            original.clone()
        };
        let project = if project_config::is_project(&active) {
            active
        } else {
            let cwd = shared
                .with_session(sid, |s| s.cwd.clone())
                .filter(|p| p.is_absolute())
                .unwrap_or(std::env::current_dir().map_err(|e| e.to_string())?);
            project_path(&cwd)
        };
        Ok(Self { system, project })
    }

    pub fn path(&self, scope: Scope) -> &Path {
        match scope {
            Scope::System => &self.system,
            Scope::Project => &self.project,
        }
    }

    pub fn scope(&self, name: &str) -> Scope {
        if Config::from_file(&self.system).ok().is_some_and(|cfg| {
            cfg.agents
                .iter()
                .any(|a| a.name == name && crate::accounts::registered(a))
        }) {
            Scope::System
        } else {
            Scope::Project
        }
    }

    pub fn reserve(&self, name: &str, scope: Scope) -> Result<ReserveCapacityConfig, String> {
        let path = self.path(scope);
        // Until there is a project file, its effective reserve is the system value.
        let path = if scope == Scope::Project && !path.exists() {
            &self.system
        } else {
            path
        };
        Ok(Config::from_file(path)
            .map_err(|e| e.to_string())?
            .agents
            .into_iter()
            .find(|a| a.name == name)
            .map(|a| a.reserve_capacity)
            .unwrap_or_default())
    }

    /// Creating a project layer is an explicit menu choice, never startup mutation.
    pub fn prepare_project(&self, shared: &Shared) -> Result<(), String> {
        if !self.project.exists() {
            let _lock = ConfigLock::acquire(self.project.clone())?;
            if !self.project.exists() {
                let user = project_config::user_path()?;
                if same_path(&self.system, &user) {
                    save_private(&self.project, b"{}\n")?;
                } else {
                    // A selected standalone file remains authoritative for this
                    // running session; retain its settings in the new layer.
                    let doc = document(&active_path(shared)?)?;
                    save_document(&self.project, &doc)?;
                }
            }
        }
        reload(shared, &self.project)
    }
}

fn same_path(a: &Path, b: &Path) -> bool {
    a == b
        || a.canonicalize()
            .ok()
            .zip(b.canonicalize().ok())
            .is_some_and(|(a, b)| a == b)
}

pub fn document(path: &Path) -> Result<Value, String> {
    project_config::read(path)?
        .map(|raw| serde_yaml::from_str(&raw).map_err(|_| "Invalid account configuration".into()))
        .unwrap_or_else(|| Ok(json!({"agents":[]})))
}

pub fn reload(shared: &Shared, path: &Path) -> Result<(), String> {
    *shared.account_config.lock().unwrap() = Config::from_file(path).map_err(|e| e.to_string())?;
    Ok(())
}

/// Copy one account, retaining the raw provider/account structure and variable references.
fn copy_account(to: &mut Value, from: &Value, name: &str) -> Result<(), String> {
    let entries = from["agents"]
        .as_array()
        .ok_or("Missing agents configuration")?;
    let mut entry = entries.iter().find(|a| a["name"] == name).cloned();
    if entry.is_none()
        && let Some((provider, account)) = name.split_once('@')
        && let Some(parent) = entries.iter().find(|a| a["name"] == provider)
        && let Some(child) = parent["accounts"]
            .as_array()
            .and_then(|a| a.iter().find(|a| a["name"] == account))
    {
        let exists = to["agents"]
            .as_array()
            .is_some_and(|a| a.iter().any(|a| a["name"] == provider));
        let mut parent = if exists {
            json!({"name":provider})
        } else {
            parent.clone()
        };
        parent["accounts"] = json!([child]);
        entry = Some(parent);
    }
    let entry = entry.ok_or("Account is absent from the selected configuration")?;
    project_config::merge(to, json!({"agents":[entry]}), "");
    Ok(())
}

fn disabled(doc: &mut Value, name: &str, value: bool) -> Result<(), String> {
    set_account_fields(
        doc,
        name,
        &[("account_disabled", json!(value))],
        &[("disabled", json!(value))],
    )
}

/// Move membership without copying, deleting or re-authenticating credentials.
pub async fn move_account(
    shared: &Arc<Shared>,
    sid: &str,
    name: &str,
    scope: Scope,
) -> Result<(), String> {
    let _guard = shared.account_write.lock().await;
    if shared.account_login.lock().unwrap().contains(name) {
        return Err("Cancel this account's login first".into());
    }
    let paths = Paths::for_session(shared, sid)?;
    if paths.scope(name) == scope {
        return Ok(());
    }
    paths.prepare_project(shared)?;
    let _lock = ConfigLock::acquire(paths.project.clone())?;
    let _system = if same_path(&paths.system, &project_config::user_path()?) {
        None
    } else {
        Some(ConfigLock::acquire(paths.system.clone())?)
    };
    let mut system = document(&paths.system)?;
    let mut project = document(&paths.project)?;
    if !Config::from_file(&paths.system)
        .ok()
        .is_some_and(|cfg| cfg.agents.iter().any(|a| a.name == name))
    {
        copy_account(&mut system, &project, name)?;
        // Membership moves never promote project reserve overrides to system defaults.
        edit_account(&mut system, name, |account| {
            account["reserve_capacity"] = json!({"weekly":0,"session":0});
        })?;
    }
    disabled(&mut system, name, scope == Scope::Project)?;
    disabled(&mut project, name, false)?;
    // Validate both documents before either file changes; restore the system
    // file if the project write fails while both configuration locks are held.
    Config::from_yaml(&serde_yaml::to_string(&system).map_err(|e| e.to_string())?)
        .map_err(|e| e.to_string())?;
    Config::from_yaml(&serde_yaml::to_string(&project).map_err(|e| e.to_string())?)
        .map_err(|e| e.to_string())?;
    let previous = std::fs::read(&paths.system).ok();
    save_document(&paths.system, &system)?;
    if let Err(error) = save_document(&paths.project, &project) {
        let restored = match previous {
            Some(bytes) => save_private(&paths.system, &bytes),
            None => std::fs::remove_file(&paths.system).map_err(|e| e.to_string()),
        };
        restored.map_err(
            |_| "Scope update failed and its system configuration could not be restored",
        )?;
        return Err(error);
    }
    reload(shared, &paths.project)
}

pub async fn set_reserve(
    shared: &Arc<Shared>,
    sid: &str,
    name: &str,
    scope: Scope,
    window: crate::accounts::ReserveWindow,
    percent: Option<u32>,
) -> Result<(), String> {
    let _guard = shared.account_write.lock().await;
    let paths = Paths::for_session(shared, sid)?;
    if scope == Scope::Project {
        paths.prepare_project(shared)?;
    }
    let path = paths.path(scope);
    // System defaults for project-only accounts need a disabled system
    // template and an explicit enabled project membership, saved together.
    let _lock = ConfigLock::acquire(paths.project.clone())?;
    let _system = if same_path(&paths.system, &project_config::user_path()?) {
        None
    } else {
        Some(ConfigLock::acquire(paths.system.clone())?)
    };
    let mut doc = document(path)?;
    let mut preserve_project = None;
    let key = window.label();
    if scope == Scope::Project && percent.is_none() {
        let mut overlay: Value =
            serde_yaml::from_str(&project_config::raw(path)?.unwrap_or_else(|| "{}".into()))
                .map_err(|_| "Invalid project configuration")?;
        let _ = edit_account(&mut overlay, name, |account| {
            if let Some(reserve) = account
                .get_mut("reserve_capacity")
                .and_then(Value::as_object_mut)
            {
                reserve.remove(key);
            }
        });
        save_private(
            path,
            serde_yaml::to_string(&overlay)
                .map_err(|e| e.to_string())?
                .as_bytes(),
        )?;
    } else {
        // A project-only account may have system reserve defaults without
        // registering it system-wide. Preserve its disabled system template.
        if !Config::from_file(path)
            .ok()
            .is_some_and(|cfg| cfg.agents.iter().any(|a| a.name == name))
        {
            let mut project = document(&active_path(shared)?)?;
            copy_account(&mut doc, &project, name)?;
            if scope == Scope::System {
                disabled(&mut project, name, false)?;
                preserve_project = Some(project);
            }
            disabled(&mut doc, name, true)?;
            edit_account(&mut doc, name, |account| {
                account["reserve_capacity"] = json!({"weekly":0,"session":0});
            })?;
        }
        edit_account(&mut doc, name, |account| {
            if !account["reserve_capacity"].is_object() {
                account["reserve_capacity"] = json!({});
            }
            account["reserve_capacity"][key] = json!(percent.unwrap_or_default());
        })?;
        let previous = std::fs::read(path).ok();
        save_document(path, &doc)?;
        if let Some(project) = preserve_project
            && let Err(error) = save_document(&paths.project, &project)
        {
            match previous {
                Some(bytes) => save_private(path, &bytes)?,
                None => std::fs::remove_file(path).map_err(|e| e.to_string())?,
            }
            return Err(error);
        }
    }
    reload(shared, &active_path(shared)?)?;
    crate::usage::refresh_after_turn(shared, name);
    Ok(())
}

fn edit_account(doc: &mut Value, name: &str, edit: impl FnOnce(&mut Value)) -> Result<(), String> {
    let entries = doc
        .get_mut("agents")
        .and_then(Value::as_array_mut)
        .ok_or("Missing agents configuration")?;
    if let Some(index) = entries.iter().position(|a| a["name"] == name) {
        edit(&mut entries[index]);
        return Ok(());
    }
    if let Some((provider, account)) = name.split_once('@')
        && let Some(parent) = entries.iter_mut().find(|a| a["name"] == provider)
        && let Some(child) = parent
            .get_mut("accounts")
            .and_then(Value::as_array_mut)
            .and_then(|a| a.iter_mut().find(|a| a["name"] == account))
    {
        edit(child);
        return Ok(());
    }
    Err("Account is absent from this configuration layer".into())
}
