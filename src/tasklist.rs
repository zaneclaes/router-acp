//! Detects when a user prompt is a multi-part task list — a set of discrete
//! things to do — so the router can hand it to its orchestration flow
//! (decompose → route each part → cross-lineage review) instead of answering it
//! as a single turn.
//!
//! Detection is deliberately permissive across the common ways people enumerate
//! work: markdown ordered / unordered lists, inline `(1) … (2) …` numbering, and
//! ordered discourse markers ("first … then … finally …"). It errs toward
//! recall — the orchestration trigger it feeds is itself soft (it only steers to
//! a planner model and injects instructions; the model decides how to
//! decompose), and an explicit `[router: …]` directive always suppresses it.

use std::collections::HashSet;

/// Clause-initial words that signal an ordered, multi-step instruction.
const SEQUENCE_MARKERS: &[&str] = &[
    "first",
    "firstly",
    "then",
    "next",
    "second",
    "secondly",
    "third",
    "thirdly",
    "fourth",
    "fourthly",
    "fifth",
    "finally",
    "lastly",
    "afterward",
    "afterwards",
    "after that",
    "to begin",
    "to start",
    "begin by",
    "start by",
];

/// If `text` reads as a decomposable multi-part task, return the number of
/// parts detected (always `>= 2`); otherwise `None`. The caller applies its own
/// minimum-item threshold on top of this.
pub fn detect_task_list(text: &str) -> Option<usize> {
    let n = [
        markdown_numbered(text),
        markdown_bullets(text),
        inline_numbered(text),
        semantic_ordering(text),
    ]
    .into_iter()
    .max()
    .unwrap_or(0);
    (n >= 2).then_some(n)
}

/// Count lines that begin (after leading whitespace) with `N.` or `N)` where N
/// is a 1–2 digit number followed by whitespace and real content. The digit cap
/// and the required trailing whitespace keep `3.14` and `1)` inside prose from
/// matching.
fn markdown_numbered(text: &str) -> usize {
    text.lines()
        .filter(|line| {
            let t = line.trim_start();
            let digits: usize = t.bytes().take_while(u8::is_ascii_digit).count();
            if digits == 0 || digits > 2 {
                return false;
            }
            let rest = &t[digits..];
            let mut chars = rest.chars();
            if !matches!(chars.next(), Some('.') | Some(')')) {
                return false;
            }
            match chars.next() {
                Some(c) if c.is_whitespace() => !rest[1..].trim().is_empty(),
                _ => false,
            }
        })
        .count()
}

/// Count lines that begin with a `-`, `*`, or `+` bullet marker followed by
/// whitespace and real content. The required whitespace excludes `---` rules and
/// `**bold**`.
fn markdown_bullets(text: &str) -> usize {
    text.lines()
        .filter(|line| {
            let t = line.trim_start();
            let mut chars = t.chars();
            if !matches!(chars.next(), Some('-') | Some('*') | Some('+')) {
                return false;
            }
            match chars.next() {
                Some(c) if c.is_whitespace() => !t[1..].trim().is_empty(),
                _ => false,
            }
        })
        .count()
}

/// Count inline `(N)` enumerations, e.g. `… (1) do this (2) do that`.
fn inline_numbered(text: &str) -> usize {
    let bytes = text.as_bytes();
    let mut count = 0;
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'(' {
            let mut j = i + 1;
            while j < bytes.len() && bytes[j].is_ascii_digit() {
                j += 1;
            }
            if j > i + 1 && j < bytes.len() && bytes[j] == b')' {
                count += 1;
                i = j + 1;
                continue;
            }
        }
        i += 1;
    }
    count
}

/// Count clause-initial sequence markers ("first", "then", "finally", …). To
/// avoid firing on incidental prose (a lone "Then I noticed…"), this requires at
/// least two *distinct* markers; it then returns the total occurrence count as
/// the item estimate.
fn semantic_ordering(text: &str) -> usize {
    let lower = text.to_lowercase();
    let mut occurrences = 0usize;
    let mut distinct: HashSet<&str> = HashSet::new();

    // Single-word markers: any standalone-token occurrence is a sequencing
    // signal — this covers both clause-initial "First, …" and inline "… then …".
    for tok in lower
        .split(|c: char| !c.is_alphanumeric())
        .filter(|t| !t.is_empty())
    {
        if let Some(m) = SEQUENCE_MARKERS
            .iter()
            .find(|m| !m.contains(' ') && **m == tok)
        {
            occurrences += 1;
            distinct.insert(*m);
        }
    }
    // Multi-word markers ("after that", "begin by"): boundary-checked substring.
    for m in SEQUENCE_MARKERS.iter().filter(|m| m.contains(' ')) {
        let mut from = 0;
        while let Some(pos) = lower[from..].find(*m) {
            let start = from + pos;
            let end = start + m.len();
            let before_ok = start == 0
                || !lower[..start]
                    .chars()
                    .next_back()
                    .unwrap()
                    .is_alphanumeric();
            let after_ok = lower[end..]
                .chars()
                .next()
                .is_none_or(|c| !c.is_alphanumeric());
            if before_ok && after_ok {
                occurrences += 1;
                distinct.insert(*m);
            }
            from = end;
        }
    }

    // Require at least two DISTINCT markers so incidental prose ("… then I
    // noticed …") doesn't read as an ordered task list.
    if distinct.len() >= 2 { occurrences } else { 0 }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn markdown_numbered_list() {
        let text = "There are some things to do.\n1. Do this.\n2. Do that\n3. Something else.";
        assert_eq!(detect_task_list(text), Some(3));
    }

    #[test]
    fn markdown_numbered_with_parens() {
        assert_eq!(detect_task_list("1) alpha\n2) beta"), Some(2));
    }

    #[test]
    fn markdown_bullets_list() {
        let text = "There are some things.\n- Do this.\n- Do that\n- Something else.";
        assert_eq!(detect_task_list(text), Some(3));
    }

    #[test]
    fn markdown_star_and_plus_bullets() {
        assert_eq!(detect_task_list("* one\n* two"), Some(2));
        assert_eq!(detect_task_list("+ one\n+ two\n+ three"), Some(3));
    }

    #[test]
    fn inline_numbering() {
        let text = "Some tasks... (1) do this (2) do that (3) something else.";
        assert_eq!(detect_task_list(text), Some(3));
    }

    #[test]
    fn semantic_ordering_first_then_finally() {
        let text = "First, do this. Then, do that. Finally, XYZ.";
        assert_eq!(detect_task_list(text), Some(3));
    }

    #[test]
    fn semantic_first_then_pair() {
        assert_eq!(
            detect_task_list("First set up the DB then wire the API."),
            Some(2)
        );
    }

    // ---- negatives: single tasks and incidental prose must NOT trigger ----

    #[test]
    fn single_sentence_not_a_list() {
        assert_eq!(
            detect_task_list("Please fix the login bug in auth.rs."),
            None
        );
    }

    #[test]
    fn decimal_number_not_a_list() {
        assert_eq!(
            detect_task_list("The value of pi is 3.14 and e is 2.71."),
            None
        );
    }

    #[test]
    fn horizontal_rule_and_bold_not_bullets() {
        assert_eq!(
            detect_task_list("Section one\n---\n**note** this matters"),
            None
        );
    }

    #[test]
    fn lone_then_not_semantic_list() {
        assert_eq!(
            detect_task_list("I ran the tests. Then everything passed and I was happy."),
            None
        );
    }

    #[test]
    fn single_numbered_item_not_a_list() {
        assert_eq!(detect_task_list("Only one thing:\n1. do the thing"), None);
    }

    #[test]
    fn single_parenthetical_not_a_list() {
        assert_eq!(detect_task_list("See the docs (1) for details."), None);
    }
}
