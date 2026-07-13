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
