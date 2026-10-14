//! Built-in agent defaults, so a configuration states only what it changes.
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

const CLAUDE: &str = include_str!("presets/claude.yaml");
const CODEX: &str = include_str!("presets/codex.yaml");
const GROK: &str = include_str!("presets/grok.yaml");

/// The preset for `provider`, when router-acp ships one.
pub fn preset(provider: &str) -> Option<Value> {
    let text = match provider {
        "claude" => CLAUDE,
        "codex" => CODEX,
        "grok" => GROK,
        _ => return None,
    };
    Some(serde_yaml::from_str(text).expect("built-in preset is valid YAML"))
}

fn text<'a>(map: &'a Mapping, key: &str) -> Option<&'a str> {
    map.get(key).and_then(Value::as_str)
}

/// The provider an agent entry names or implies.
pub fn provider_of(agent: &Mapping) -> Option<&'static str> {
    const KNOWN: [&str; 4] = ["claude", "codex", "grok", "kimi"];
    if let Some(explicit) = text(agent, "provider") {
        return KNOWN.into_iter().find(|p| *p == explicit);
    }
    let base = text(agent, "name").and_then(|n| n.split('@').next());
    if let Some(p) = KNOWN.into_iter().find(|p| Some(*p) == base) {
        return Some(p);
    }
    let usage = agent
        .get("usage_source")
        .and_then(Value::as_mapping)
        .and_then(|u| text(u, "type"));
    match usage {
        Some("anthropic-oauth") => return Some("claude"),
        Some("codex-rollout") => return Some("codex"),
        _ => {}
    }
    let command = agent
        .get("command")
        .and_then(Value::as_mapping)
        .and_then(|c| text(c, "command"))?;
    match Path::new(command).file_name()?.to_str()? {
        "claude-agent-acp" | "claude-acp" => Some("claude"),
        "codex-acp" => Some("codex"),
        "grok" => Some("grok"),
        "kimi" => Some("kimi"),
        _ => None,
    }
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

/// Apply the presets and implied seat directories to a parsed configuration.
/// An entry inherits its preset when it names `provider:` or runs no program
/// of its own; one with its own `command` and no `provider:` stays exactly as
/// written, as every configuration did before presets.
pub fn apply(document: &mut Value) {
    let Some(agents) = document
        .as_mapping_mut()
        .and_then(|d| d.get_mut("agents"))
        .and_then(Value::as_sequence_mut)
    else {
        return;
    };
    for agent in agents.iter_mut().filter_map(Value::as_mapping_mut) {
        let Some(provider) = provider_of(agent) else {
            continue;
        };
        let explicit = agent.shift_remove("provider").is_some();
        if !explicit && own_program(agent).is_some() {
            continue;
        }
        if let Some((variable, path)) = implied_directory(agent, provider)
            && let Some(env) = env_list(agent)
            && !names_variable(env, variable)
        {
            let mut entry = Mapping::new();
            entry.insert("name".into(), variable.into());
            entry.insert("value".into(), path.into());
            env.push(Value::Mapping(entry));
        }
        if let Some(Value::Mapping(mut defaults)) = preset(provider) {
            resolve_programs(&mut defaults);
            merge(agent, &defaults);
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
            minimize_agent(agent, agent_defaults);
        }
    }
    out
}

/// Drop from one agent entry every value its provider preset, its implied
/// seat directory or the agent defaults (`agent_defaults`, a serialized agent
/// with only its required keys) already supply.
pub fn minimize_agent(agent: &mut Mapping, agent_defaults: &Value) {
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
    let preset = provider_of(agent).and_then(|p| Some((p, preset(p)?)));
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
    if let Some((variable, path)) = implied_directory(agent, provider)
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
        assert_eq!(agent["models"].as_sequence().unwrap().len(), 4);
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
    fn entry_values_win_lists_replace_and_mappings_merge() {
        let mut doc = yaml(
            "agents:\n- name: work\n  provider: codex\n  models:\n  - { id: gpt-6.1-sol, cost_rank: 5 }\n  mode_map: { auto: agent }\n",
        );
        apply(&mut doc);
        let agent = &doc["agents"][0];
        assert_eq!(agent["models"].as_sequence().unwrap().len(), 1);
        assert_eq!(agent["mode_map"]["auto"], "agent");
        assert_eq!(agent["mode_map"]["chat"], "read-only");
        assert!(agent.get("provider").is_none());
        assert!(
            agent["command"].get("env").is_none(),
            "a non-seat name implies no directory"
        );
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
    fn minimize_drops_section_defaults_and_keeps_changed_keys() {
        let doc = yaml("cordon: { enabled: true, poll_secs: 300 }\nrouter: auto\nagents: []\n");
        let defaults = yaml("cordon: { enabled: true, poll_secs: 60 }\nrouter: auto\n");
        assert_eq!(
            minimize(&doc, &defaults, &yaml("{}")),
            yaml("cordon: { poll_secs: 300 }\nagents: []\n")
        );
    }
}
