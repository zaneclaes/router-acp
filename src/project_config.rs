//! Project overlays and config discovery. Hosts ask the router to resolve its
//! own configuration; account edits save only the selected layer's differences.
use std::path::{Path, PathBuf};

use serde_json::{Map, Value};

pub fn user_path() -> Result<PathBuf, String> {
    let home = std::env::var_os("HOME").ok_or("HOME is unavailable")?;
    Ok(PathBuf::from(home).join(".config/router-acp/router.yaml"))
}

pub fn select(explicit: Option<&Path>, cwd: &Path) -> Result<PathBuf, String> {
    let environment = std::env::var_os("ROUTER_ACP_CONFIG").map(PathBuf::from);
    let selected = explicit.or(environment.as_deref());
    if let Some(path) = selected {
        let text = path.to_str().ok_or("Configuration path is not UTF-8")?;
        if text.trim().is_empty() {
            return Err("ROUTER_ACP_CONFIG must name a configuration file".into());
        }
        let expanded = PathBuf::from(crate::config::expand_tilde_str(
            &crate::config::interpolate_env(text),
        ));
        return Ok(if expanded.is_absolute() {
            expanded
        } else {
            cwd.join(expanded)
        });
    }
    for dir in cwd.ancestors() {
        let path = dir.join(".router-acp/router.yaml");
        if path
            .try_exists()
            .map_err(|_| "Could not inspect project configuration")?
        {
            return Ok(path);
        }
        // A nested repository must not inherit its containing repository's policy.
        if dir.join(".git").exists() {
            break;
        }
    }
    user_path()
}

pub fn is_project(path: &Path) -> bool {
    path.file_name().is_some_and(|n| n == "router.yaml")
        && path
            .parent()
            .and_then(Path::file_name)
            .is_some_and(|n| n == ".router-acp")
        && user_path().is_ok_and(|user| {
            path != user
                && path
                    .canonicalize()
                    .ok()
                    .zip(user.canonicalize().ok())
                    .is_none_or(|(a, b)| a != b)
        })
}

pub(crate) fn raw(path: &Path) -> Result<Option<String>, String> {
    match std::fs::read_to_string(path) {
        Ok(text) => Ok(Some(text)),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(_) => Err(format!("Could not read configuration {}", path.display())),
    }
}

fn document(text: &str) -> Result<Value, String> {
    let value: Value = serde_yaml::from_str(text).map_err(|_| "Invalid configuration YAML")?;
    if !value.is_object() {
        return Err("Configuration must be a YAML mapping".into());
    }
    validate_names(&value)?;
    Ok(value)
}

fn validate_names(value: &Value) -> Result<(), String> {
    if let Some(object) = value.as_object() {
        for (key, child) in object {
            if named(key)
                && let Some(items) = child.as_array()
            {
                let mut seen = std::collections::HashSet::new();
                for item in items {
                    if let Some(name) = name(item)
                        && !seen.insert(name)
                    {
                        return Err(format!("Duplicate name in configuration {key}"));
                    }
                }
            }
            validate_names(child)?;
        }
    } else if let Some(items) = value.as_array() {
        for item in items {
            validate_names(item)?;
        }
    }
    Ok(())
}

pub(crate) fn base(path: &Path) -> Result<Option<Value>, String> {
    if !is_project(path) {
        return Ok(None);
    }
    raw(&user_path()?)?
        .map(|text| document(&text))
        .transpose()
        .map(|base| Some(base.unwrap_or_else(|| Value::Object(Map::new()))))
}

/// Return the uninterpolated effective document. No environment values or
/// expanded provider defaults are ever written back to either layer.
pub(crate) fn read(path: &Path) -> Result<Option<String>, String> {
    let Some(text) = raw(path)? else {
        return Ok(None);
    };
    let Some(mut inherited) = base(path)? else {
        return Ok(Some(text));
    };
    merge(&mut inherited, document(&text)?, "");
    serde_yaml::to_string(&inherited)
        .map(Some)
        .map_err(|_| "Could not combine configuration".into())
}

fn named(key: &str) -> bool {
    matches!(key, "agents" | "accounts")
}
fn name(value: &Value) -> Option<&str> {
    value.get("name").and_then(Value::as_str)
}

pub(crate) fn merge(base: &mut Value, overlay: Value, key: &str) {
    merge_values(base, overlay, key, true);
}

fn merge_values(base: &mut Value, overlay: Value, key: &str, remove_nulls: bool) {
    match (base, overlay) {
        (Value::Object(base), Value::Object(overlay)) => {
            for (key, value) in overlay {
                if value.is_null() && remove_nulls {
                    base.remove(&key);
                } else {
                    merge_values(
                        base.entry(key.clone()).or_insert(Value::Null),
                        value,
                        &key,
                        remove_nulls,
                    );
                }
            }
        }
        (Value::Array(base), Value::Array(overlay)) if named(key) => {
            for value in overlay {
                if let Some(existing) = base
                    .iter_mut()
                    .find(|v| name(&value).is_some() && name(v) == name(&value))
                {
                    merge_values(existing, value, "", remove_nulls);
                } else {
                    base.push(value);
                }
            }
        }
        (base, overlay) => *base = overlay,
    }
}

fn difference(base: &Value, current: &Value, key: &str) -> Option<Value> {
    if base == current {
        return None;
    }
    match (base, current) {
        (Value::Object(base), Value::Object(current)) => {
            let mut delta = Map::new();
            for (key, value) in current {
                if let Some(previous) = base.get(key) {
                    if let Some(value) = difference(previous, value, key) {
                        delta.insert(key.clone(), value);
                    }
                } else {
                    delta.insert(key.clone(), value.clone());
                }
            }
            for key in base.keys().filter(|key| !current.contains_key(*key)) {
                delta.insert(key.clone(), Value::Null);
            }
            Some(Value::Object(delta))
        }
        (Value::Array(base), Value::Array(current)) if named(key) => {
            let mut delta = Vec::new();
            for value in current {
                if let Some(previous) = base
                    .iter()
                    .find(|v| name(value).is_some() && name(v) == name(value))
                {
                    if let Some(mut changed) = difference(previous, value, "") {
                        changed["name"] = value["name"].clone();
                        delta.push(changed);
                    }
                } else {
                    delta.push(value.clone());
                }
            }
            for value in base
                .iter()
                .filter(|v| !current.iter().any(|c| name(c) == name(v)))
            {
                let disabled = if key == "agents" {
                    "account_disabled"
                } else {
                    "disabled"
                };
                delta.push(serde_json::json!({"name": value["name"], disabled: true}));
            }
            Some(Value::Array(delta))
        }
        (_, current) => Some(current.clone()),
    }
}

pub(crate) fn for_write(path: &Path, effective: &Value) -> Result<Value, String> {
    let Some(mut before) = base(path)? else {
        return Ok(effective.clone());
    };
    let mut overlay = raw(path)?
        .map(|text| document(&text))
        .transpose()?
        .unwrap_or_else(|| Value::Object(Map::new()));
    merge(&mut before, overlay.clone(), "");
    if let Some(changed) = difference(&before, effective, "") {
        // Preserve explicit existing overrides, even when they currently equal
        // user defaults. Store null tombstones instead of removing them here.
        merge_values(&mut overlay, changed, "", false);
    }
    Ok(overlay)
}

pub(crate) fn inherited_names(path: &Path) -> Result<Vec<String>, String> {
    Ok(base(path)?
        .and_then(|v| v.get("agents").and_then(Value::as_array).cloned())
        .unwrap_or_default()
        .iter()
        .filter_map(|v| name(v).map(str::to_owned))
        .collect())
}

pub(crate) fn lock_paths(path: &Path) -> Result<Vec<PathBuf>, String> {
    let mut paths = Vec::new();
    if is_project(path) {
        paths.push(user_path()?);
    }
    paths.push(path.to_path_buf());
    Ok(paths)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn overlays_merge_accounts_and_settings_without_freezing_inherited_values() {
        let base = json!({"router":"auto", "agents":[
            {"name":"claude@shared", "reserve_capacity":{"weekly":10,"session":5}},
            {"name":"codex@shared"}], "routers":{"auto":{"allowed_candidates":["*"]}}});
        let overlay = json!({"router":"planner", "agents":[
            {"name":"claude@shared", "reserve_capacity":{"weekly":40}},
            {"name":"codex@project"}]});
        let mut effective = base.clone();
        merge(&mut effective, overlay.clone(), "");
        assert_eq!(
            effective["agents"][0]["reserve_capacity"],
            json!({"weekly":40,"session":5})
        );
        assert_eq!(effective["agents"].as_array().unwrap().len(), 3);
        assert_eq!(difference(&base, &effective, "").unwrap(), overlay);
        let mut updated_base = base;
        updated_base["agents"][0]["reserve_capacity"]["session"] = json!(15);
        merge(&mut updated_base, overlay, "");
        assert_eq!(updated_base["agents"][0]["reserve_capacity"]["session"], 15);
    }

    #[test]
    fn nested_accounts_merge_but_model_lists_replace_and_null_resets_settings() {
        let mut base = json!({"history":"90d", "agents":[{"name":"claude", "models":[1,2],
            "accounts":[{"name":"work", "reserve_capacity":{"weekly":10,"session":5}}, {"name":"personal"}]}]});
        let overlay = json!({"history":null, "agents":[{"name":"claude", "models":[3],
            "accounts":[{"name":"work", "reserve_capacity":{"weekly":80}}, {"name":"project"}]}]});
        merge(&mut base, overlay, "");
        assert!(base.get("history").is_none());
        assert_eq!(base["agents"][0]["models"], json!([3]));
        assert_eq!(base["agents"][0]["accounts"].as_array().unwrap().len(), 3);
        assert_eq!(
            base["agents"][0]["accounts"][0]["reserve_capacity"]["session"],
            5
        );
    }
}
