//! Built-in agent defaults, so a configuration states only what it changes.
//!
//! An agent's `models:` come from `data/providers/<provider>.yaml`, the one catalog of the
//! models router-acp supports: every row whose `provider` is the agent's
//! provider. An entry's own `models:` merge over those rows by `id` (its
//! fields win) and may add models the catalog lacks; they never replace the
//! list.
//!
//! An agent entry names its provider with `provider:`, or implies it by its
//! name (`claude`, `claude@<id>`) or its command (`claude-agent-acp`). Every key
//! the entry does not set comes from that provider's preset: mappings merge
//! recursively, while lists and scalars in the entry replace the preset's. An
//! account seat `<provider>@<id>` also implies its directory,
//! `~/.config/router-acp/accounts/<name>`, through the provider's directory
//! variable. Presets apply when a configuration loads; the file on disk is
//! never expanded, so the router's own edits (logins, seat adoption) keep it
//! small.
//!
//! [`minimize`] is the inverse: it drops every value a configuration shares
//! with the presets and the built-in section defaults.

use serde_yaml::{Mapping, Value};
use std::path::Path;

/// Keys of a `data/providers/<provider>.yaml` file that are not agent settings:
/// the model catalog and what the model updater reads.
const CATALOG_KEYS: [&str; 4] = ["provider", "models", "cache_ratio", "discovery"];

/// Each provider's file of `data/providers/`, by provider name.
fn providers() -> Mapping {
    crate::candidate::BUILTIN_PROVIDERS
        .iter()
        .map(|(name, text)| {
            let document: Value = serde_yaml::from_str(text).expect("data/providers is valid YAML");
            (Value::from(*name), document)
        })
        .collect()
}

/// The names of the providers `data/providers/` defines.
pub fn provider_names() -> Vec<String> {
    providers()
        .keys()
        .filter_map(Value::as_str)
        .map(str::to_string)
        .collect()
}

/// The catalog rows of `provider` in `data/providers/<provider>.yaml`, as `models:` entries.
pub fn catalog_models(provider: &str) -> Vec<Value> {
    crate::candidate::ScoreTable::builtin()
        .rows()
        .iter()
        .filter(|row| row.provider.as_deref() == Some(provider))
        .map(|row| Value::Mapping(row.catalog.clone()))
        .collect()
}

/// `catalog` with `own` merged over it by `id`: an entry naming a catalog
/// model overrides that row's fields, and one naming another model is added
/// after the catalog's.
fn merge_models(own: Vec<Value>, catalog: Vec<Value>) -> Vec<Value> {
    let id = |v: &Value| v.get("id").and_then(Value::as_str).map(str::to_string);
    let mut models = catalog;
    for entry in own {
        match models
            .iter_mut()
            .find(|m| id(m).is_some() && id(m) == id(&entry))
        {
            Some(Value::Mapping(row)) => {
                if let Value::Mapping(mut mine) = entry {
                    merge(&mut mine, row);
                    *row = mine;
                }
            }
            _ => models.push(entry),
        }
    }
    models
}

/// The default agent settings for `provider` in `data/providers/`, when
/// router-acp ships them, its `models:` the provider's catalog rows.
pub fn preset(provider: &str) -> Option<Value> {
    let mut preset = providers().get(provider)?.as_mapping()?.clone();
    for key in CATALOG_KEYS {
        preset.shift_remove(key);
    }
    preset.insert("models".into(), Value::Sequence(catalog_models(provider)));
    Some(Value::Mapping(preset))
}

/// A router config's `providers:` section: per provider, the settings it
/// changes. Each entry merges over router-acp's defaults for that provider
/// (`data/providers/<name>.yaml`; mappings merge, lists and scalars replace,
/// and its `models:` merge over the catalog by `id`), and names a provider of
/// its own when router-acp ships none.
pub type ProviderOverrides = Mapping;

/// The providers a configuration knows: router-acp's and its own.
fn known_providers(overrides: &ProviderOverrides) -> Vec<String> {
    let mut names = provider_names();
    for name in overrides.keys().filter_map(Value::as_str) {
        if !names.iter().any(|n| n == name) {
            names.push(name.to_string());
        }
    }
    names
}

/// The settings an agent of `provider` inherits: router-acp's defaults with
/// the configuration's `providers.<provider>` merged over them.
pub fn effective_preset(provider: &str, overrides: &ProviderOverrides) -> Option<Value> {
    let builtin = preset(provider);
    let own = overrides.get(provider).and_then(Value::as_mapping).cloned();
    match (builtin, own) {
        (None, None) => None,
        (Some(builtin), None) => Some(builtin),
        (None, Some(own)) => Some(Value::Mapping(own)),
        (Some(Value::Mapping(mut builtin)), Some(mut own)) => {
            let catalog = match builtin.shift_remove("models") {
                Some(Value::Sequence(models)) => models,
                _ => Vec::new(),
            };
            let own_models = match own.shift_remove("models") {
                Some(Value::Sequence(models)) => models,
                _ => Vec::new(),
            };
            merge(&mut own, &builtin);
            own.insert(
                "models".into(),
                Value::Sequence(merge_models(own_models, catalog)),
            );
            Some(Value::Mapping(own))
        }
        (Some(other), Some(_)) => Some(other),
    }
}

fn text<'a>(map: &'a Mapping, key: &str) -> Option<&'a str> {
    map.get(key).and_then(Value::as_str)
}

/// The provider an agent entry names or implies.
pub fn provider_of(agent: &Mapping) -> Option<String> {
    provider_of_among(agent, &provider_names())
}

/// The provider an agent entry names or implies, among `known`.
fn provider_of_among(agent: &Mapping, known: &[String]) -> Option<String> {
    let known = known.to_vec();
    if let Some(explicit) = text(agent, "provider") {
        return known.into_iter().find(|p| p == explicit);
    }
    let base = text(agent, "name").and_then(|n| n.split('@').next());
    if let Some(p) = known.iter().find(|p| Some(p.as_str()) == base) {
        return Some(p.clone());
    }
    let usage = agent
        .get("usage_source")
        .and_then(Value::as_mapping)
        .and_then(|u| text(u, "type"));
    match usage {
        Some("anthropic-oauth") => return Some("claude".into()),
        Some("codex-rollout") => return Some("codex".into()),
        _ => {}
    }
    let command = agent
        .get("command")
        .and_then(Value::as_mapping)
        .and_then(|c| text(c, "command"))?;
    let program = Path::new(command).file_name()?.to_str()?;
    // A command runs a provider when it runs that provider's default program.
    let provider = providers().into_iter().find_map(|(name, entry)| {
        let default = entry.get("command")?.get("command")?.as_str()?;
        let default = Path::new(default).file_name()?.to_str()?.to_string();
        (default == program)
            .then(|| name.as_str().map(str::to_string))
            .flatten()
    });
    provider.or_else(|| (program == "claude-acp").then(|| "claude".to_string()))
}

/// Fill `target` with every key of `defaults` it lacks, merging nested
/// mappings. Lists and scalars already in `target` win.
fn merge(target: &mut Mapping, defaults: &Mapping) {
    for (key, value) in defaults {
        match (target.get_mut(key), value) {
            (Some(Value::Mapping(mine)), Value::Mapping(theirs)) => merge(mine, theirs),
            (Some(_), _) => {}
            (None, _) => {
                target.insert(key.clone(), value.clone());
            }
        }
    }
}

/// The directory an account seat `<provider>@<id>` uses when its entry names
/// none, as the `(variable, path)` its adapter reads.
fn implied_directory(agent: &Mapping, provider: &str) -> Option<(&'static str, String)> {
    let name = text(agent, "name")?;
    let (base, id) = name.split_once('@')?;
    if base != provider || id.is_empty() {
        return None;
    }
    let home = std::env::var("HOME").ok()?;
    let path = format!("{home}/.config/router-acp/accounts/{name}");
    Some((crate::accounts::directory_variable(provider), path))
}

fn env_list(agent: &mut Mapping) -> Option<&mut Vec<Value>> {
    let command = agent
        .entry("command".into())
        .or_insert_with(|| Value::Mapping(Mapping::new()))
        .as_mapping_mut()?;
    command
        .entry("env".into())
        .or_insert_with(|| Value::Sequence(Vec::new()))
        .as_sequence_mut()
}

fn names_variable(env: &[Value], variable: &str) -> bool {
    env.iter().any(|e| {
        e.as_mapping()
            .and_then(|m| text(m, "name"))
            .is_some_and(|n| n == variable)
    })
}

/// The first `PATH` entry holding the bare program `name`.
fn on_path(name: &str) -> Option<String> {
    let path = std::env::var_os("PATH")?;
    std::env::split_paths(&path)
        .map(|dir| dir.join(name))
        .find(|candidate| candidate.is_file())
        .map(|found| found.display().to_string())
}

/// Resolve the preset's bare program names (`claude-agent-acp`, `codex`) to
/// the `PATH` entry they run, so the loaded configuration names the binary it
/// spawns. A name `PATH` lacks stays bare and fails at spawn as before.
fn resolve_programs(preset: &mut Mapping) {
    for section in ["command", "auth_probe"] {
        if let Some(program) = preset
            .get_mut(section)
            .and_then(Value::as_mapping_mut)
            .and_then(|m| m.get_mut("command"))
            && let Some(name) = program.as_str().filter(|n| !n.contains('/'))
            && let Some(found) = on_path(name)
        {
            *program = found.into();
        }
    }
}

/// The program an entry runs, when it names one (`command.command`).
fn own_program(agent: &Mapping) -> Option<&Value> {
    agent
        .get("command")
        .and_then(Value::as_mapping)
        .and_then(|c| c.get("command"))
}

/// Apply the presets and implied seat directories to a parsed configuration,
/// and give a configuration that lists no `agents:` one agent per provider.
/// An entry inherits its preset when it names `provider:` or runs no program
/// of its own; one with its own `command` and no `provider:` stays exactly as
/// written, as every configuration did before presets.
pub fn apply(document: &mut Value) {
    // `providers:` changes router-acp's provider defaults; it is folded into
    // the agents here and is no setting of the loaded configuration.
    let overrides = document
        .as_mapping_mut()
        .and_then(|map| map.shift_remove("providers"))
        .and_then(|value| value.as_mapping().cloned())
        .unwrap_or_default();
    let known = known_providers(&overrides);
    // A configuration that lists no agents (accounts) runs one per provider.
    if let Some(map) = document.as_mapping_mut()
        && !map.contains_key("agents")
    {
        let defaults = known
            .clone()
            .into_iter()
            .map(|name| {
                let mut entry = Mapping::new();
                entry.insert("name".into(), name.into());
                Value::Mapping(entry)
            })
            .collect();
        map.insert("agents".into(), Value::Sequence(defaults));
    }
    let Some(agents) = document
        .as_mapping_mut()
        .and_then(|d| d.get_mut("agents"))
        .and_then(Value::as_sequence_mut)
    else {
        return;
    };
    for agent in agents.iter_mut().filter_map(Value::as_mapping_mut) {
        let Some(provider) = provider_of_among(agent, &known) else {
            continue;
        };
        let explicit = agent.shift_remove("provider").is_some();
        if !explicit && own_program(agent).is_some() {
            continue;
        }
        if let Some((variable, path)) = implied_directory(agent, &provider)
            && let Some(env) = env_list(agent)
            && !names_variable(env, variable)
        {
            let mut entry = Mapping::new();
            entry.insert("name".into(), variable.into());
            entry.insert("value".into(), path.into());
            env.push(Value::Mapping(entry));
        }
        if let Some(Value::Mapping(mut defaults)) = effective_preset(&provider, &overrides) {
            resolve_programs(&mut defaults);
            let catalog = match defaults.shift_remove("models") {
                Some(Value::Sequence(models)) => models,
                _ => Vec::new(),
            };
            let own = match agent.shift_remove("models") {
                Some(Value::Sequence(models)) => models,
                _ => Vec::new(),
            };
            merge(agent, &defaults);
            agent.insert("models".into(), Value::Sequence(merge_models(own, catalog)));
        }
    }
}

/// A configured program equals the preset's: the same text, or the binary a
/// bare preset name resolves to through `PATH`.
fn same_program(configured: &Value, preset: &Value) -> bool {
    if configured == preset {
        return true;
    }
    let (Some(configured), Some(preset)) = (configured.as_str(), preset.as_str()) else {
        return false;
    };
    let configured = crate::config::expand_tilde_str(configured);
    if preset.contains('/') {
        return configured == crate::config::expand_tilde_str(preset);
    }
    on_path(preset).is_some_and(|found| found == configured)
}

/// Equal values, with numbers compared numerically (`200` equals `200.0`).
fn same_value(a: &Value, b: &Value) -> bool {
    match (a, b) {
        (Value::Number(x), Value::Number(y)) => x.as_f64() == y.as_f64(),
        _ => a == b,
    }
}

/// Keys the router reads and ignores (the removed per-request cost policy).
const IGNORED_KEYS: [(&str, &str); 4] = [
    ("llm_proxy", "routine_streak"),
    ("llm_proxy", "minimum_dwell_requests"),
    ("llm_proxy", "verdict_ttl_requests"),
    ("llm_proxy", "verdict_ttl_secs"),
];

/// Remove from `target` every value equal to the one in `defaults`. Mappings
/// are compared key by key; a mapping left empty is removed.
fn subtract(target: &mut Mapping, defaults: &Mapping) {
    let keys: Vec<Value> = target.keys().cloned().collect();
    for key in keys {
        let Some(default) = defaults.get(&key) else {
            continue;
        };
        let program = key.as_str() == Some("command");
        let remove = match (target.get_mut(&key), default) {
            (Some(Value::Mapping(mine)), Value::Mapping(theirs)) => {
                subtract(mine, theirs);
                mine.is_empty()
            }
            (Some(mine), theirs) if program => same_program(mine, theirs),
            (Some(mine), theirs) => same_value(mine, theirs),
            (None, _) => false,
        };
        if remove {
            target.shift_remove(&key);
        }
    }
}

/// Drop every agent value the presets supply and every value equal to the
/// built-in default. `defaults` is a serialized default configuration and
/// `agent_defaults` a serialized agent with only its required keys set.
pub fn minimize(document: &Value, defaults: &Value, agent_defaults: &Value) -> Value {
    let mut out = document.clone();
    let Some(map) = out.as_mapping_mut() else {
        return out;
    };
    let overrides = map
        .get("providers")
        .and_then(Value::as_mapping)
        .cloned()
        .unwrap_or_default();
    for (section, key) in IGNORED_KEYS {
        if let Some(section) = map.get_mut(section).and_then(Value::as_mapping_mut) {
            section.shift_remove(key);
        }
    }
    if let Some(defaults) = defaults.as_mapping() {
        let keys: Vec<Value> = map.keys().cloned().collect();
        for key in keys {
            if key.as_str() == Some("agents") {
                continue;
            }
            let Some(default) = defaults.get(&key) else {
                continue;
            };
            let remove = match (map.get_mut(&key), default) {
                (Some(Value::Mapping(mine)), Value::Mapping(theirs)) => {
                    subtract(mine, theirs);
                    mine.is_empty()
                }
                (Some(mine), theirs) => same_value(mine, theirs),
                (None, _) => false,
            };
            if remove {
                map.shift_remove(&key);
            }
        }
    }
    if let Some(agents) = map.get_mut("agents").and_then(Value::as_sequence_mut) {
        for agent in agents.iter_mut().filter_map(Value::as_mapping_mut) {
            minimize_agent_with(agent, agent_defaults, &overrides);
        }
    }
    // Exactly one bare agent per provider is what an omitted `agents:` means.
    let bare: Vec<Value> = known_providers(&overrides)
        .into_iter()
        .map(|name| {
            let mut entry = Mapping::new();
            entry.insert("name".into(), name.into());
            Value::Mapping(entry)
        })
        .collect();
    if map.get("agents").and_then(Value::as_sequence) == Some(&bare) {
        map.shift_remove("agents");
    }
    out
}

/// Drop from one agent entry every value its provider preset, its implied
/// seat directory or the agent defaults (`agent_defaults`, a serialized agent
/// with only its required keys) already supply.
pub fn minimize_agent(agent: &mut Mapping, agent_defaults: &Value) {
    minimize_agent_with(agent, agent_defaults, &Mapping::new());
}

/// [`minimize_agent`] against the defaults a configuration's `providers:`
/// section changes.
pub fn minimize_agent_with(
    agent: &mut Mapping,
    agent_defaults: &Value,
    overrides: &ProviderOverrides,
) {
    // Field defaults, for the keys a preset (when one applies) does not set.
    let field_defaults = |agent: &mut Mapping, preset: Option<&Mapping>| {
        if let Some(defaults) = agent_defaults.as_mapping() {
            let mut defaults = defaults.clone();
            for required in ["name", "command", "model_selection", "models"] {
                defaults.shift_remove(required);
            }
            for key in preset.into_iter().flat_map(Mapping::keys) {
                defaults.shift_remove(key);
            }
            subtract(agent, &defaults);
        }
    };
    let preset = provider_of_among(agent, &known_providers(overrides)).and_then(|p| {
        let defaults = effective_preset(&p, overrides)?;
        Some((p, defaults))
    });
    let Some((provider, Value::Mapping(preset))) = preset else {
        field_defaults(agent, None);
        return;
    };
    // Only an entry that inherits the preset once reduced loses the preset's
    // values: one naming `provider:`, running no program of its own, or
    // running the preset's program.
    let inherits = agent.contains_key("provider")
        || match (own_program(agent), own_program(&preset)) {
            (None, _) => true,
            (Some(mine), Some(theirs)) => same_program(mine, theirs),
            (Some(_), None) => false,
        };
    if !inherits {
        field_defaults(agent, None);
        return;
    }
    field_defaults(agent, Some(&preset));
    if let Some((variable, path)) = implied_directory(agent, &provider)
        && let Some(env) = agent
            .get_mut("command")
            .and_then(Value::as_mapping_mut)
            .and_then(|c| c.get_mut("env"))
            .and_then(Value::as_sequence_mut)
    {
        env.retain(|e| {
            let entry = e.as_mapping();
            !(entry.and_then(|m| text(m, "name")) == Some(variable)
                && entry
                    .and_then(|m| text(m, "value"))
                    .map(crate::config::expand_tilde_str)
                    == Some(path.clone()))
        });
    }
    let mut preset = preset;
    if let Some(Value::Sequence(catalog)) = preset.shift_remove("models") {
        minimize_models(agent, &catalog);
    }
    subtract(agent, &preset);
    if let Some(command) = agent.get_mut("command").and_then(Value::as_mapping_mut) {
        if command
            .get("env")
            .and_then(Value::as_sequence)
            .is_some_and(Vec::is_empty)
        {
            command.shift_remove("env");
        }
        if command.is_empty() {
            agent.shift_remove("command");
        }
    }
}

/// Drop from an entry's `models:` every field its catalog row already
/// supplies, every entry left naming only its `id`, and the list once empty.
fn minimize_models(agent: &mut Mapping, catalog: &[Value]) {
    let Some(Value::Sequence(models)) = agent.get_mut("models") else {
        return;
    };
    models.retain_mut(|entry| {
        let Some(mine) = entry.as_mapping_mut() else {
            return true;
        };
        let row = catalog
            .iter()
            .filter_map(Value::as_mapping)
            .find(|row| row.get("id").is_some() && row.get("id") == mine.get("id"));
        let Some(row) = row else {
            return true;
        };
        let id = mine.shift_remove("id");
        subtract(mine, row);
        let keep = !mine.is_empty();
        if let Some(id) = id {
            mine.insert("id".into(), id);
        }
        keep
    });
    if models.is_empty() {
        agent.shift_remove("models");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn yaml(text: &str) -> Value {
        serde_yaml::from_str(text).unwrap()
    }

    #[test]
    fn a_bare_seat_inherits_its_providers_preset_and_directory() {
        let mut doc = yaml("agents:\n- name: claude@abc\n  preference: 0.1\n");
        apply(&mut doc);
        let agent = &doc["agents"][0];
        // The bare preset name, or the PATH entry it resolves to.
        assert!(
            agent["command"]["command"]
                .as_str()
                .unwrap()
                .ends_with("claude-agent-acp")
        );
        assert_eq!(
            agent["models"].as_sequence().unwrap(),
            &catalog_models("claude"),
            "a seat inherits its provider's catalog rows"
        );
        assert_eq!(agent["preference"], 0.1);
        let env = &agent["command"]["env"][0];
        assert_eq!(env["name"], "CLAUDE_CONFIG_DIR");
        assert!(
            env["value"]
                .as_str()
                .unwrap()
                .ends_with("/.config/router-acp/accounts/claude@abc")
        );
    }

    #[test]
    fn entry_values_win_and_models_merge_over_the_catalog_by_id() {
        let mut doc = yaml(
            "agents:\n- name: work\n  provider: codex\n  models:\n  - { id: gpt-6.1-sol, cost_rank: 3 }\n  - { id: gpt-7-nova, cost_rank: 5 }\n  mode_map: { auto: agent }\n",
        );
        apply(&mut doc);
        let agent = &doc["agents"][0];
        let models = agent["models"].as_sequence().unwrap();
        let catalog = catalog_models("codex");
        assert_eq!(
            models.len(),
            catalog.len() + 1,
            "an override adds nothing; a new id is added"
        );
        let sol = models.iter().find(|m| m["id"] == "gpt-6.1-sol").unwrap();
        assert_eq!(sol["cost_rank"], 3, "the entry's field wins");
        assert_eq!(
            sol["display_name"], "GPT-6.1 Sol",
            "the row's other fields stay"
        );
        assert_eq!(models.last().unwrap()["id"], "gpt-7-nova");
        assert_eq!(agent["mode_map"]["auto"], "agent");
        assert_eq!(agent["mode_map"]["chat"], "read-only");
        assert!(agent.get("provider").is_none());
        assert!(
            agent["command"].get("env").is_none(),
            "a non-seat name implies no directory"
        );
    }

    #[test]
    fn grok_frontier_versions_share_the_documented_cost_rank() {
        let mut doc = yaml("agents:\n- name: grok@seat\n");
        apply(&mut doc);
        let models = doc["agents"][0]["models"].as_sequence().unwrap();
        for id in ["grok-4.7", "grok-4.6"] {
            let model = models.iter().find(|m| m["id"] == id).unwrap();
            assert_eq!(
                model["cost_rank"], 5,
                "{id} must not become a cheaper demotion target"
            );
        }
    }

    #[test]
    fn a_bare_kimi_agent_inherits_the_builtin_preset() {
        let mut doc = yaml("agents:\n- name: kimi\n");
        apply(&mut doc);
        let agent = &doc["agents"][0];
        // The bare preset name, or the PATH entry it resolves to on this
        // machine (`resolve_programs`).
        assert!(
            agent["command"]["command"]
                .as_str()
                .is_some_and(|command| command == "kimi" || command.ends_with("/kimi")),
            "{:?}",
            agent["command"]["command"]
        );
        assert_eq!(agent["command"]["type"], "stdio");
        assert_eq!(agent["lineage"], "moonshot");
        assert_eq!(agent["models"][0]["id"], "kimi-k2");
        assert_eq!(agent["model_selection"]["type"], "spawn-config");
    }

    #[test]
    fn an_entry_with_its_own_command_stays_as_written() {
        let text = "agents:\n- name: claude\n  command: { type: stdio, command: /opt/mock-agent }\n  model_selection: { type: config-option }\n  models: [{ id: m1, cost_rank: 1 }]\n";
        let mut doc = yaml(text);
        apply(&mut doc);
        assert_eq!(
            doc,
            yaml(text),
            "existing configurations keep their meaning"
        );
    }

    #[test]
    fn an_explicit_provider_applies_the_preset_under_a_custom_command() {
        let mut doc = yaml(
            "agents:\n- name: claude\n  provider: claude\n  command: { command: /opt/claude-agent-acp }\n",
        );
        apply(&mut doc);
        let agent = &doc["agents"][0];
        assert_eq!(agent["command"]["command"], "/opt/claude-agent-acp");
        assert_eq!(agent["usage_source"]["type"], "anthropic-oauth");
    }

    #[test]
    fn an_explicit_directory_is_kept() {
        let mut doc = yaml(
            "agents:\n- name: grok@x\n  command:\n    env:\n    - { name: GROK_HOME, value: /elsewhere }\n",
        );
        apply(&mut doc);
        let env = doc["agents"][0]["command"]["env"].as_sequence().unwrap();
        assert_eq!(env.len(), 1);
        assert_eq!(env[0]["value"], "/elsewhere");
        assert_eq!(doc["agents"][0]["command"]["command"], "~/.grok/bin/grok");
    }

    #[test]
    fn minimize_drops_preset_values_and_keeps_overrides() {
        let mut full = yaml("agents:\n- name: claude@abc\n  preference: 0.1\n");
        apply(&mut full);
        let small = minimize(&full, &yaml("{}"), &yaml("{}"));
        assert_eq!(
            small,
            yaml("agents:\n- name: claude@abc\n  preference: 0.1\n")
        );
    }

    #[test]
    fn a_configuration_without_agents_runs_one_agent_per_provider() {
        let mut doc = yaml("router: auto\n");
        apply(&mut doc);
        let names: Vec<&str> = doc["agents"]
            .as_sequence()
            .unwrap()
            .iter()
            .map(|a| a["name"].as_str().unwrap())
            .collect();
        assert_eq!(names, provider_names());
        assert!(names.contains(&"claude") && names.contains(&"kimi"));
        assert_eq!(
            minimize(&doc, &yaml("{}"), &yaml("{}")),
            yaml("router: auto\n"),
            "the default agents minimize back to an omitted `agents:`"
        );
        let mut listed = yaml("agents:\n- name: codex\n");
        apply(&mut listed);
        assert_eq!(
            listed["agents"].as_sequence().unwrap().len(),
            1,
            "listed agents run exactly"
        );
    }

    /// `providers:` changes a provider's defaults for every agent of it; `agents:` are accounts.
    #[test]
    fn a_providers_entry_merges_over_the_defaults_for_every_agent_of_it() {
        let text = "providers:\n  claude:\n    command:\n      command: npx\n      args: [\"-y\", \"@agentclientprotocol/claude-agent-acp\"]\n    preference: 0.1\n";
        let mut doc = yaml(text);
        apply(&mut doc);
        assert!(doc.get("providers").is_none(), "folded into the agents");
        let claude = doc["agents"]
            .as_sequence()
            .unwrap()
            .iter()
            .find(|a| a["name"] == "claude")
            .unwrap();
        assert!(
            claude["command"]["command"]
                .as_str()
                .unwrap()
                .ends_with("npx")
        );
        assert_eq!(
            claude["command"]["type"], "stdio",
            "the rest of the default stays"
        );
        assert_eq!(claude["preference"], 0.1);
        assert_eq!(
            claude["models"].as_sequence().unwrap(),
            &catalog_models("claude")
        );
        assert_eq!(
            minimize(&doc_raw(text), &yaml("{}"), &yaml("{}")),
            yaml(text)
        );
        // A seat inherits the override too.
        let mut seats = yaml(&format!("{text}agents:\n- name: claude@a\n"));
        apply(&mut seats);
        assert_eq!(seats["agents"][0]["preference"], 0.1);
        assert_eq!(seats["agents"].as_sequence().unwrap().len(), 1);
    }

    fn doc_raw(text: &str) -> Value {
        let mut doc = yaml(text);
        let providers = doc["providers"].clone();
        apply(&mut doc);
        doc.as_mapping_mut()
            .unwrap()
            .insert("providers".into(), providers);
        doc
    }

    #[test]
    fn a_provider_is_known_by_its_default_program() {
        for (program, provider) in [
            ("/opt/bin/codex-acp", "codex"),
            ("/usr/local/bin/claude-agent-acp", "claude"),
            ("kimi", "kimi"),
        ] {
            let entry = yaml(&format!("name: x\ncommand: {{ command: {program} }}\n"));
            assert_eq!(
                provider_of(entry.as_mapping().unwrap()).as_deref(),
                Some(provider)
            );
        }
        assert!(
            preset("claude").unwrap().get("discovery").is_none(),
            "catalog keys are not agent settings"
        );
    }

    #[test]
    fn minimize_keeps_only_model_overrides_and_additions() {
        let text = "agents:\n- name: codex\n  models:\n  - { id: gpt-6.1-sol, cost_rank: 3 }\n  - { id: gpt-7-nova, cost_rank: 5 }\n";
        let mut full = yaml(text);
        apply(&mut full);
        assert_eq!(minimize(&full, &yaml("{}"), &yaml("{}")), yaml(text));
    }

    #[test]
    fn minimize_drops_the_kimi_preset_and_keeps_project_overrides() {
        let mut full = yaml("agents:\n- name: kimi\n  preference: 0.2\n");
        apply(&mut full);
        let small = minimize(&full, &yaml("{}"), &yaml("{}"));
        assert_eq!(small, yaml("agents:\n- name: kimi\n  preference: 0.2\n"));
    }

    #[test]
    fn minimize_drops_section_defaults_and_keeps_changed_keys() {
        let doc = yaml("cordon: { enabled: true, poll_secs: 300 }\nrouter: auto\nagents: []\n");
        let defaults = yaml("cordon: { enabled: true, poll_secs: 60 }\nrouter: auto\n");
        assert_eq!(
            minimize(&doc, &defaults, &yaml("{}")),
            yaml("cordon: { poll_secs: 300 }\nagents: []\n")
        );
    }
}
