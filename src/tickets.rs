//! Ticket-reference context loading.
//!
//! When a prompt mentions a ticket id matching a configured prefix (e.g.
//! `HAI-1234` for `prefix: "HAI-"`), the router runs the rule's command (e.g.
//! `linear issue view $TICKET`) and prepends the ticket's content to the prompt
//! **before** classification and orchestration detection. "Fix HAI-1234" thus
//! routes on the ticket's actual scope — and a ticket whose body is a work list
//! can trigger auto-orchestration.
//!
//! Fail-open by design: a missing command, non-zero exit, timeout, or empty
//! output leaves the prompt untouched. Each ticket is injected at most once per
//! session (re-mentions don't re-bloat the context), with a short global fetch
//! cache so concurrent sessions don't re-run the command.

use std::sync::Arc;
use std::time::{Duration, Instant};

use agent_client_protocol::schema::v1::{ContentBlock, PromptRequest};

use crate::config::TicketRule;
use crate::session::{Shared, notify_user, sid_str};

/// Max tickets fetched per prompt (a prompt spamming ids shouldn't fan out).
const MAX_TICKETS_PER_PROMPT: usize = 3;
/// Fetch command timeout.
const FETCH_TIMEOUT: Duration = Duration::from_secs(20);
/// Cap on injected ticket text (chars) — keeps a pathological ticket from
/// blowing up the prompt.
const MAX_TICKET_CHARS: usize = 16_000;
/// Global fetch-cache TTL.
const CACHE_TTL: Duration = Duration::from_secs(300);

/// Ticket ids referenced in `text` that match any configured rule: the prefix
/// at a word start, followed by 1+ digits. Deduplicated, prompt order, capped.
pub fn find_ticket_refs(rules: &[TicketRule], text: &str) -> Vec<(usize, String)> {
    let mut out: Vec<(usize, String)> = Vec::new();
    for (rule_idx, rule) in rules.iter().enumerate() {
        let prefix = rule.prefix.as_str();
        if prefix.is_empty() {
            continue;
        }
        let mut from = 0;
        while let Some(pos) = text[from..].find(prefix) {
            let start = from + pos;
            let end = start + prefix.len();
            from = end;
            // Word start: beginning of text or a non-alphanumeric before.
            let word_start = start == 0
                || !text[..start]
                    .chars()
                    .next_back()
                    .is_some_and(|c| c.is_alphanumeric());
            if !word_start {
                continue;
            }
            let digits: String = text[end..]
                .chars()
                .take_while(char::is_ascii_digit)
                .collect();
            if digits.is_empty() {
                continue;
            }
            let id = format!("{prefix}{digits}");
            if !out.iter().any(|(_, existing)| existing == &id) {
                out.push((rule_idx, id));
            }
        }
    }
    out.truncate(MAX_TICKETS_PER_PROMPT);
    out
}

/// Frame fetched ticket content for prompt injection.
pub fn frame_ticket(id: &str, body: &str) -> String {
    let mut body = body.trim().to_string();
    if body.chars().count() > MAX_TICKET_CHARS {
        body = body.chars().take(MAX_TICKET_CHARS).collect();
        body.push_str("\n[… ticket truncated …]");
    }
    format!(
        "[Ticket {id} — loaded automatically because the prompt references it]\n{body}\n[End of ticket {id}. The user's message follows.]"
    )
}

/// Run the rule's command with `$TICKET` substituted; stdout is the ticket
/// content. Any failure → `Err` (the caller fails open).
async fn fetch_ticket(rule: &TicketRule, id: &str) -> Result<String, String> {
    let argv: Vec<String> = rule
        .command
        .iter()
        .map(|a| a.replace("$TICKET", id))
        .collect();
    let (prog, args) = argv.split_first().ok_or("empty command")?;
    let fut = tokio::process::Command::new(prog)
        .args(args)
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .output();
    let out = tokio::time::timeout(FETCH_TIMEOUT, fut)
        .await
        .map_err(|_| format!("ticket fetch timed out after {}s", FETCH_TIMEOUT.as_secs()))?
        .map_err(|e| format!("cannot run `{prog}`: {e}"))?;
    if !out.status.success() {
        return Err(format!(
            "`{prog}` failed ({}): {}",
            out.status,
            String::from_utf8_lossy(&out.stderr).trim()
        ));
    }
    let body = String::from_utf8_lossy(&out.stdout).trim().to_string();
    if body.is_empty() {
        return Err("ticket command produced no output".to_string());
    }
    Ok(body)
}

/// If the prompt references configured tickets, fetch them (cache + per-session
/// dedup) and prepend their framed content. Returns the (possibly enriched)
/// request; on any failure the original prompt passes through unchanged.
pub async fn enrich_prompt(
    shared: &Arc<Shared>,
    router_sid: &str,
    req: PromptRequest,
) -> PromptRequest {
    let rules = &shared.cfg.ticket_context;
    if rules.is_empty() {
        return req;
    }
    let text: String = req
        .prompt
        .iter()
        .filter_map(|b| match b {
            ContentBlock::Text(t) => Some(t.text.as_str()),
            _ => None,
        })
        .collect::<Vec<_>>()
        .join("\n");
    let refs = find_ticket_refs(rules, &text);
    if refs.is_empty() {
        return req;
    }

    let mut blocks: Vec<ContentBlock> = Vec::new();
    for (rule_idx, id) in refs {
        // Once per session: a re-mention doesn't re-inject.
        let already = shared
            .with_session(router_sid, |s| !s.injected_tickets.insert(id.clone()))
            .unwrap_or(true);
        if already {
            continue;
        }
        // Short global cache so concurrent sessions share one fetch.
        let cached = {
            let cache = shared.ticket_cache.lock().unwrap();
            cache
                .get(&id)
                .filter(|(at, _)| at.elapsed() < CACHE_TTL)
                .map(|(_, body)| body.clone())
        };
        let body = match cached {
            Some(body) => body,
            None => match fetch_ticket(&rules[rule_idx], &id).await {
                Ok(body) => {
                    shared
                        .ticket_cache
                        .lock()
                        .unwrap()
                        .insert(id.clone(), (Instant::now(), body.clone()));
                    body
                }
                Err(err) => {
                    tracing::warn!(ticket = %id, %err, "ticket fetch failed; continuing without");
                    notify_user(
                        shared,
                        router_sid,
                        format!("router-acp · could not load ticket {id} ({err}); continuing"),
                    );
                    // Allow a later prompt to retry.
                    shared.with_session(router_sid, |s| s.injected_tickets.remove(&id));
                    continue;
                }
            },
        };
        notify_user(
            shared,
            router_sid,
            format!(
                "router-acp · loaded ticket {id} into context ({} chars)",
                body.len()
            ),
        );
        blocks.push(ContentBlock::from(frame_ticket(&id, &body)));
    }
    if blocks.is_empty() {
        return req;
    }
    blocks.extend(req.prompt.clone());
    let sid = sid_str(&req.session_id);
    PromptRequest::new(sid, blocks).meta(req.meta.clone())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rules() -> Vec<TicketRule> {
        vec![TicketRule {
            prefix: "HAI-".to_string(),
            command: vec!["echo".to_string(), "$TICKET".to_string()],
        }]
    }

    #[test]
    fn finds_prefixed_ticket_ids() {
        let refs = find_ticket_refs(&rules(), "Fix HAI-1234 and also HAI-99.");
        let ids: Vec<_> = refs.iter().map(|(_, id)| id.as_str()).collect();
        assert_eq!(ids, vec!["HAI-1234", "HAI-99"]);
    }

    #[test]
    fn dedups_and_caps() {
        let text = "HAI-1 HAI-1 HAI-2 HAI-3 HAI-4 HAI-5";
        let refs = find_ticket_refs(&rules(), text);
        assert_eq!(refs.len(), MAX_TICKETS_PER_PROMPT);
        assert_eq!(refs[0].1, "HAI-1");
    }

    #[test]
    fn requires_word_start_and_digits() {
        assert!(find_ticket_refs(&rules(), "XHAI-12 is not a ticket").is_empty());
        assert!(find_ticket_refs(&rules(), "HAI- alone is not a ticket").is_empty());
        // Punctuation before the prefix is fine.
        assert_eq!(find_ticket_refs(&rules(), "(HAI-7)").len(), 1);
    }

    #[test]
    fn frame_includes_id_and_truncates() {
        let framed = frame_ticket("HAI-1", "body text");
        assert!(framed.contains("[Ticket HAI-1"));
        assert!(framed.contains("body text"));
        let long = "x".repeat(MAX_TICKET_CHARS + 100);
        assert!(frame_ticket("HAI-1", &long).contains("ticket truncated"));
    }
}
