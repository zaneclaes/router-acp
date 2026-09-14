//! Session-id rewriting and `_meta` handling for relayed messages.
//!
//! The router forwards raw [`UntypedMessage`]s in both directions, rewriting
//! only the `sessionId` field. Forwarding raw JSON preserves `_meta` and any
//! extension fields untouched; router-owned metadata lives under
//! `_meta.router_acp`.

use agent_client_protocol::{Error, UntypedMessage};
use serde_json::Value;

/// Extract the `sessionId` param, if present.
pub fn session_id_of(msg: &UntypedMessage) -> Option<String> {
    msg.params()
        .get("sessionId")
        .and_then(|v| v.as_str())
        .map(str::to_string)
}

/// Return a copy of `msg` with `sessionId` replaced.
pub fn with_session_id(msg: &UntypedMessage, session_id: &str) -> Result<UntypedMessage, Error> {
    let mut params = msg.params().clone();
    if let Value::Object(map) = &mut params {
        map.insert(
            "sessionId".to_string(),
            Value::String(session_id.to_string()),
        );
    }
    UntypedMessage::new(msg.method(), params)
}

/// Split a Grok-style `terminal/create` command line into executable + args.
///
/// Grok's CLI puts the entire `/bin/bash -lc '…'` invocation in `command`
/// and omits `args`. Spec-compliant clients (`spawn(command, args)`) then
/// look for an executable whose name is that whole string and fail with
/// ENOENT. When `args` is empty/absent and `command` contains whitespace,
/// split it the way a shell would. Already-split requests (Claude, Codex,
/// the router's own `background_start`) are left untouched. Unclosed quotes
/// fail open — better to forward the original than invent argv.
pub fn normalize_terminal_create(msg: &UntypedMessage) -> Result<UntypedMessage, Error> {
    if msg.method() != "terminal/create" {
        return UntypedMessage::new(msg.method(), msg.params().clone());
    }
    let mut params = msg.params().clone();
    let Some(map) = params.as_object_mut() else {
        return UntypedMessage::new(msg.method(), params);
    };
    let Some(command) = map.get("command").and_then(Value::as_str) else {
        return UntypedMessage::new(msg.method(), params);
    };
    if command.is_empty() || !command.contains(char::is_whitespace) {
        return UntypedMessage::new(msg.method(), params);
    }
    let args_empty = match map.get("args") {
        None => true,
        Some(Value::Array(items)) if items.is_empty() => true,
        Some(Value::Array(_)) => false,
        _ => true,
    };
    if !args_empty {
        return UntypedMessage::new(msg.method(), params);
    }
    let Some(parts) = split_argv(command) else {
        return UntypedMessage::new(msg.method(), params);
    };
    if parts.len() < 2 {
        return UntypedMessage::new(msg.method(), params);
    }
    map.insert("command".to_string(), Value::String(parts[0].clone()));
    map.insert(
        "args".to_string(),
        Value::Array(parts.into_iter().skip(1).map(Value::String).collect()),
    );
    UntypedMessage::new(msg.method(), params)
}

/// POSIX-like argv split: whitespace separates, single/double quotes group,
/// backslash escapes outside single quotes. `None` on unclosed quotes.
fn split_argv(input: &str) -> Option<Vec<String>> {
    let mut parts = Vec::new();
    let mut buf = String::new();
    let mut chars = input.chars().peekable();
    let mut in_single = false;
    let mut in_double = false;
    while let Some(c) = chars.next() {
        match c {
            '\'' if !in_double => in_single = !in_single,
            '"' if !in_single => in_double = !in_double,
            c if c.is_whitespace() && !in_single && !in_double => {
                if !buf.is_empty() {
                    parts.push(std::mem::take(&mut buf));
                }
            }
            '\\' if !in_single => match chars.next() {
                Some(next) => buf.push(next),
                None => buf.push('\\'),
            },
            _ => buf.push(c),
        }
    }
    if in_single || in_double {
        return None;
    }
    if !buf.is_empty() {
        parts.push(buf);
    }
    if parts.is_empty() { None } else { Some(parts) }
}

/// Return a copy of `msg` with `details` merged in under `_meta.router_acp`,
/// preserving any existing `_meta` keys.
pub fn with_router_meta(msg: &UntypedMessage, details: Value) -> Result<UntypedMessage, Error> {
    let mut params = msg.params().clone();
    if let Value::Object(map) = &mut params {
        let meta = map
            .entry("_meta".to_string())
            .or_insert_with(|| Value::Object(Default::default()));
        if let Value::Object(meta_map) = meta {
            meta_map.insert("router_acp".to_string(), details);
        }
    }
    UntypedMessage::new(msg.method(), params)
}

/// True when `msg` is a tool-call frame (`tool_call` or its later updates).
/// These are the frames a client attributes to a model, so they carry the
/// per-request routing metadata.
pub fn is_tool_call_update(msg: &UntypedMessage) -> bool {
    matches!(
        msg.params()
            .get("update")
            .and_then(|update| update.get("sessionUpdate"))
            .and_then(|kind| kind.as_str()),
        Some("tool_call") | Some("tool_call_update")
    )
}

/// True when `msg` is an `agent_message_chunk` carrying text.
pub fn is_agent_text_chunk(msg: &UntypedMessage) -> bool {
    let Some(update) = msg.params().get("update") else {
        return false;
    };
    update.get("sessionUpdate").and_then(|k| k.as_str()) == Some("agent_message_chunk")
        && update
            .get("content")
            .and_then(|c| c.get("type"))
            .and_then(|t| t.as_str())
            == Some("text")
}

/// Prepend `prefix` to the text of an `agent_message_chunk`. Used to ride the
/// routing disclosure on the model's own first response chunk, because goose
/// (and similar clients) drop separate router-originated `session/update`s.
/// Returns `msg` unchanged if it is not a text chunk.
pub fn prepend_agent_text(msg: &UntypedMessage, prefix: &str) -> Result<UntypedMessage, Error> {
    if !is_agent_text_chunk(msg) {
        return UntypedMessage::new(msg.method(), msg.params().clone());
    }
    let mut params = msg.params().clone();
    if let Some(text) = params
        .get_mut("update")
        .and_then(|u| u.get_mut("content"))
        .and_then(|c| c.get_mut("text"))
        && let Value::String(s) = text
    {
        *s = format!("{prefix}{s}");
    }
    UntypedMessage::new(msg.method(), params)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn msg(params: Value) -> UntypedMessage {
        UntypedMessage::new("session/update", params).unwrap()
    }

    #[test]
    fn rewrites_session_id_only() {
        let m = msg(json!({
            "sessionId": "down-1",
            "update": {"sessionUpdate": "agent_message_chunk", "content": {"type": "text", "text": "hi"}},
            "_meta": {"downstream": true}
        }));
        let out = with_session_id(&m, "router-1").unwrap();
        assert_eq!(out.params()["sessionId"], "router-1");
        assert_eq!(out.params()["_meta"]["downstream"], true);
        assert_eq!(out.params()["update"]["content"]["text"], "hi");
    }

    fn terminal(params: Value) -> UntypedMessage {
        UntypedMessage::new("terminal/create", params).unwrap()
    }

    #[test]
    fn grok_combined_shell_command_splits_into_executable_and_args() {
        let m = terminal(json!({
            "sessionId": "s",
            "command": "/bin/bash -lc 'echo hello-repro'",
        }));
        let out = normalize_terminal_create(&m).unwrap();
        assert_eq!(out.params()["command"], "/bin/bash");
        assert_eq!(out.params()["args"], json!(["-lc", "echo hello-repro"]));
        assert_eq!(out.params()["sessionId"], "s");
    }

    #[test]
    fn empty_args_array_still_splits() {
        let m = terminal(json!({
            "command": "/bin/bash -lc 'pwd'",
            "args": []
        }));
        let out = normalize_terminal_create(&m).unwrap();
        assert_eq!(out.params()["command"], "/bin/bash");
        assert_eq!(out.params()["args"], json!(["-lc", "pwd"]));
    }

    #[test]
    fn already_split_command_is_left_alone() {
        let m = terminal(json!({
            "command": "/bin/bash",
            "args": ["-lc", "pwd"]
        }));
        let out = normalize_terminal_create(&m).unwrap();
        assert_eq!(out.params()["command"], "/bin/bash");
        assert_eq!(out.params()["args"], json!(["-lc", "pwd"]));
    }

    #[test]
    fn non_terminal_methods_are_unchanged() {
        let m = msg(json!({"sessionId": "s", "command": "/bin/bash -lc 'pwd'"}));
        let out = normalize_terminal_create(&m).unwrap();
        assert_eq!(out.params()["command"], "/bin/bash -lc 'pwd'");
        assert!(out.params().get("args").is_none());
    }

    #[test]
    fn unclosed_quotes_fail_open() {
        let m = terminal(json!({"command": "/bin/bash -lc 'unterminated"}));
        let out = normalize_terminal_create(&m).unwrap();
        assert_eq!(out.params()["command"], "/bin/bash -lc 'unterminated");
        assert!(out.params().get("args").is_none());
    }

    #[test]
    fn attaches_router_meta_preserving_existing() {
        let m = msg(json!({"sessionId": "s", "_meta": {"keep": 1}}));
        let out = with_router_meta(&m, json!({"candidate": "claude/sonnet"})).unwrap();
        assert_eq!(out.params()["_meta"]["keep"], 1);
        assert_eq!(
            out.params()["_meta"]["router_acp"]["candidate"],
            "claude/sonnet"
        );
    }

    #[test]
    fn prepends_only_to_text_chunks() {
        let chunk = msg(json!({
            "sessionId": "s",
            "update": {"sessionUpdate": "agent_message_chunk",
                       "content": {"type": "text", "text": "hello"}}
        }));
        assert!(is_agent_text_chunk(&chunk));
        let out = prepend_agent_text(&chunk, "> router-acp\n\n").unwrap();
        assert_eq!(
            out.params()["update"]["content"]["text"],
            "> router-acp\n\nhello"
        );
        // A tool-call update is untouched and not a text chunk.
        let tool = msg(json!({
            "sessionId": "s",
            "update": {"sessionUpdate": "tool_call", "toolCallId": "t", "title": "x"}
        }));
        assert!(!is_agent_text_chunk(&tool));
        let out = prepend_agent_text(&tool, "> x\n\n").unwrap();
        assert_eq!(out.params()["update"]["sessionUpdate"], "tool_call");
    }

    #[test]
    fn session_id_extraction() {
        assert_eq!(
            session_id_of(&msg(json!({"sessionId": "x"}))),
            Some("x".into())
        );
        assert_eq!(session_id_of(&msg(json!({"other": 1}))), None);
    }
}
