# Durable planner progress

All new fields default when loading older runs. Ordinary loading retains its policy
snapshot. Explicit `implement` after a completed drain and ended attempts refreshes
skills, retaining prior snapshots. Adoption preserves releases, recovery, observations
and policy history; retrying a partial adoption preserves newer destination progress.
An explicit `implement` in a new session may also adopt a paused run after every
attempt ended and its owner stopped. Ordinary session loading never resumes it.
No API grants deployment authority.

Adoption and explicit implementation after a completed drain mark unfinished allocated works
`reconciliation.required`. The affected assignment cannot dispatch before this custody pass;
independent admissions, capacity wakes and release/audit wakes continue. Source ownership,
dependency and hold checks still apply; a missing recovery workspace never freezes the whole
queue. Parent `reconcile {key, work, source_revision, evidence}` supplies
the complete current WorkSpec (same admitted plan), a full source commit already in its
restored workspace HEAD, and evidence paths. It requires an ended/dead attempt, preserves prior
specs, fences the old child and invalidates stale acceptance. Dispatch and artifacts check the
source ancestry again. Both fresh and continued prompts include current scope and custody.
Never treat a checkpoint note or copied files as an updated assignment.

Parent `retire {key, work_id, reason, evidence}` is a terminal disposition for obsolete work,
not successful integration. It requires an ended attempt, no frozen release membership and a
host response `{"verified_action":"retire"}` after checking the trusted assignment and parent
takeover evidence. Chordzy permits only obsolete release/prod-smoke workers. The router retains
source/evidence, clears retry/recovery wakes and never creates an integration receipt or deletes
the worktree. Release proof obligations remain with the parent.

WorkSpec `owned_paths` accepts literal relative files/directories without globs or
traversal; active overlaps are refused at admission/retry. `dependencies` requires
integration. `ready_dependencies` maps work IDs to exact accepted SHAs, which callers
merge, qualify and carry in the release. Artifact `integration_requirements` maps
receipt keys to release-owned recipes. Acceptance may precede these proofs; integration
requires every key. `unresolved` blocks acceptance. Identical packet replay preserves
review/receipts; changes invalidate them.

Worker dispatch and follow-up context, and targeted `status` reads, expose
`related_ready_sources`: other same-plan accepted or finished heads whose review
matches the artifact revision and whose attempt has ended. Scope, changed paths
and evidence let a repair discover already-qualified work before recreating it.
The `selected` flag reflects the assignment's existing `ready_dependencies`;
discovery never selects or merges a head. Unselected applicable source still needs
parent reconciliation. Queue reads stay compact; unrelated, unreviewed, active and
integrated work is excluded.

Parent `finalize {key, work_id, receipt}` requires an ended accepted attempt and clean
exact worktree. The configured integrate-check host receives the trusted assignment,
verifies custody/resources and returns exit 0 with `{"verified_action":"finalize"}`.
An old host's bare exit 0 is refused. Otherwise use the child finished path.

## Release checkpoints

Parent `release {key, release_id, checkpoint}` records stage, full revision and evidence.
Selection also contains nonempty works (work IDs → finished accepted SHAs); a repair
selection may name replaces (an unreplaced failed release). Later stages omit those fields.

| Ordered stage | Evidence key |
|---|---|
| selected | selection |
| candidate-passed | candidate-build |
| staging-deployed | staging-rollout |
| staging-passed | staging-smoke |
| production-merged | production-pr |
| production-deployed | production-rollout |
| production-passed | production-smoke |
| disposed | disposition-commit |

A nonterminal stage can become failed with failure evidence. Failed candidates never
advance; select a repair. Dropped heads remain owed and still require passing proof.
A candidate not yet merged to production may be abandoned with abandonment evidence
to free staging for an isolated repair if preceding production fails. Its work stays
unfinished. A production-merged candidate cannot be abandoned.
The staging lane reopens at production merge; the next production merge waits for the
previous pass. Revision changes only at production merge. Disposed requires all selected
integration receipts. Each transition needs the integrate-check host, which receives
release_record including guide paths, checks real repository proof and returns exit 0
with `{"verified_action":"release"}`. Missing, crashed and old hosts cannot record it.
Replays are idempotent; selected work integration names its passing release-round.

## Observations and progress

### Messages from a local monitor

`router-acp planner-input --session <router-id> --cwd <checkout> --id <stable-id>
--message-file <UTF-8-file>` queues a correction for a running parent. Use `-` for
stdin, or `--status` instead of `--cwd`/`--message-file` to read its receipt. The
optional `--state` points to the normal state-layout root, not a shard. The command
uses StateStore and revision-checked writes to the existing FIFO input queue; it
does not launch an adapter, take over ownership, interrupt a turn, or read provider
credentials. Existing routers that support queued parent input deliver it at the
next idle opportunity, ahead of ordinary wakes. Approval waits remain effective.
The active router tries to steer queued agent input into its own running turn. If
the adapter declines, it uses normal preemption to create that idle opportunity,
preserving the interrupted wakes and prioritizing any queued operator input.
Once delivery starts, an agent-origin correction is router-owned: live operator
input can steer it, or cancel it when steering is unavailable. An interrupted
correction stays queued for redelivery after the operator's turn. Actual operator
turns remain protected and preserve their FIFO queue behavior.

Messages are explicitly agent-origin and cannot authorize new scope, change phase,
resume a paused run, or acknowledge an operator decision. Sending requires an exact
checkout match and a running run with a live owner. Reuse an id when retrying an
uncertain submission: identical content returns its existing receipt even after
delivery; changed content with that id fails. Send a new correction with a new id.
Output contains only metadata, never the message or other conversation contents.
`queued`, `delivered`, and `acknowledged` are separate facts; acknowledgment does
not prove the requested code or release completed. A dead owner after enqueue
leaves the message durable for the normal adoption path. Maximum message size is
64 KiB. The command relies on local state-file permissions, like `adopt-run`.

Audit requests carry releases and previous observations. Hosts may return up to 128
observations: stable IDs → `{revision, reference, state, evidence}` with a full SHA and
pending/passed/failed/unknown state. Unchanged values produce no new wake. Changes
coalesce by target. Operator input leads delivery, then release/observations, recovery,
other work and routine reminders. Conversation history remains intact. Observations
never accept work or pass gates. Fresh `ci:<work_id>` observations can park explicitly
matching waiting handoffs; failed/stale observers cannot silently hold work forever.

Hosts may return completed_guides, the actual landed Completed inventory. Progress
reports that separately from work counts, with new paths relative to the first audit.
It does not certify independent review. Missing inventory stays unknown. Ready age is
measured only for new finishing receipts. Existing Kory wake text and targeted status
show releases, proof keys and recovery without adding a scheduler.
