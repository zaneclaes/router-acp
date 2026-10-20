# Planner capacity enforcement

A planner parent that leaves a worker slot empty while admissible work exists
has failed. The router enforces this mechanically, not through prompt text
(`src/planner_capacity.rs`). It sits beside `routers.planner.workspace`
(ROUTERS.md, "Planner workflow") and uses the same launch contract.

## Configuration

```yaml
routers:
  planner:
    capacity:
      command: planner-capacity-host   # required when the block is present
      args: []
      timeout_ms: 30000                # default 30000
      wake_after_secs: 60              # default 60
      auto_admit_after_wakes: 2        # default 2
      auto_admit_after_secs: 300       # default 300
      inflight_wake_secs: 300          # default 300
```

Without the block, worker resumption and in-flight wakes still run on these
default timings; only the host query, capacity wakes, auto-admission and the
completion check need it.

## Host contract

The command launches like `workspace`: no shell, cwd is the session
repository. While the parent is below `delegation.max_concurrent` (and on
every `complete`), the router writes one JSON object to its stdin:

```json
{"event":"capacity","cwd":"<repo>","free":3,"max":20,
 "works":[{"work_id":"…","plan_id":"…","status":"running","external_id":"…"}],
 "holds":[{"plan_id":"…","reason":"dependency","detail":"…","expires_at_ms":0}]}
```

The host answers on stdout with the admissible candidates, in priority order:

```json
{"candidates":[{"work":{"work_id":"…","plan_id":"…","scope":"…","external_id":"…"},
                "task":"<the worker brief>"}]}
```

`work` is exactly the `WorkSpec` that `planner_workflow` `admit` takes. A
non-zero exit, a timeout or invalid JSON is logged as an error and recorded as
**unknown**, never as "no work": the parent is woken with the host error, no
candidate is auto-admitted, and `complete` is refused until the host answers.
The router asks at most every 30 s with a free slot, including during an active
coordination turn. Parent tool activity neither postpones the check nor triggers
another host process before that interval. Pinning, an approval wait, or queued
parent input still defers the check.

A candidate still needs a slot when its `work_id` is not admitted, is admitted
but was never started, or was interrupted before it got a workspace.

## Holds

`planner_workflow {action: "hold", key, plan_id, reason, detail}` keeps one
candidate (matched by `plan_id` or `work_id`) unadmitted for 30 minutes. The
`reason` must be exactly one of:

| reason | `detail` must |
| --- | --- |
| `source-conflict` | name the live `work_id` it conflicts with |
| `dependency` | name the unmet work or plan |
| `disk-floor` | carry the measured free bytes |

Anything else is rejected with an error that lists the three. A hold is the
only way a candidate may stay out of a free slot. Holds are passed to the host
and expire on their own.

## Capacity wakes

Parent wake delivery gives staging/production build, failed-job and smoke observations
and release checkpoints priority over ordinary WIP build observations, reviews and refill.
An unfinished release (including a failed deploy, corrective selection and owed disposition)
selects `integrate-plan` for implementation wakes and explicit `implement` commands,
including the first turn after adoption or a drained restart. Superseded, abandoned and
disposed rounds do not. Command admission and wake delivery share the same role selector;
the operator's arguments and scope are preserved. Explicit planning still selects
`create-plan`, and implementation without integration work selects `select-plan`.
When a new session adopts the old run during command admission, it refreshes that
same command's role before sending the first ACP prompt; this creates no second
operator input and leaves transferred work reconciliation and queued inputs intact.
Reconciliation gates each adopted worker's resumption, rather than imposing a
whole-inventory prerequisite ahead of deployment recovery.
An unresolved failed staging/production build also selects integration on later wakes,
even when its failure notification was acknowledged and no release record exists yet.
Acknowledgment clears delivery, not the failed build or a stopped repair's ownership request;
fresh environment build evidence clears that recovery priority. Ordinary WIP failures and
advisory job notifications do not acquire this persistent environment priority.
Host work summaries include `attempt_active`, derived from the current attempt rather
than a stale work-status label, so a repository audit can identify a broken environment
with no active repair workers. Missing metadata in older callers remains unknown.
Finished source waiting for integration also selects the integration role before routine
review, even before the parent records a release selection. The role still verifies exact
qualification, corrective isolation and dependencies; role selection never selects a candidate
or substitutes for a release gate. Accepted or review-pending work alone does not trigger it.
The integration role advances the release first or reconciles its concrete prerequisite,
then handles independent work while that prerequisite is pending. This changes only parent
role selection and wake ordering: child execution, admission, holds and release gates remain
unchanged. A repository's unrelated primary-checkout edits must not be mistaken for a defect
in its qualified isolated release candidate; the repository host validates that boundary.

An unfilled slot with an unheld candidate gets a `capacity:` wake after
`wake_after_secs`, and another every
`wake_after_secs` while that holds, with no limit on deliveries. The wake
lists the free slot count and each candidate (`work_id`, `external_id`, plan)
and tells the parent to admit up to the free count now, in order, or record a
`hold` for each one it skips. Breadth is always permitted: depth-first
ordering never leaves a slot empty. While the host lists unheld candidates or
cannot answer, these wakes replace the 20-minute heartbeat; the heartbeat
remains for "no candidates", so blockers are still re-checked.

The unfilled-condition clock is independent of parent tool activity and resets
when slots fill or every candidate is held/closed. A busy router-owned parent
receives the pending capacity wake through the existing mid-turn delivery path:
one delivery per wake per turn, sharing its 60-second rate limit with urgent
release and failed-job wakes. Routine branch observations remain deferred.
This supports ongoing coordination; implementation and diagnostic execution
remain worker responsibilities under the host's role contract.

## Auto-admission

After `auto_admit_after_wakes` delivered capacity wakes for the same unfilled
condition, or `auto_admit_after_secs` of parent idleness with a free slot, the
router, when the parent is idle, admits and starts up to the free count of unheld candidates, in host
order. It uses the parent's own path: `admit` with idempotency key
`auto-admit:<work_id>:<round>`, workspace allocation through the workspace
host, then the delegate started as a background planner job whose seat the
active strategy's delegate routing chooses, with the host's `task` as its
brief. A refused start (a workspace host refusal, unmet dependencies) becomes
a recorded hold (`disk-floor` carrying the host's message, or `dependency`),
never a silent drop. The parent then gets an `auto-admit:<round>` wake:
"router admitted X, Y, Z to fill N free slots; review them as normal."
Auto-admission needs a pinned (loaded) parent session.

## Completion

`complete` is refused while any work is not integrated (the error lists each
work with its status: interrupted, corrections, admitted, running, …), while
the host lists an unheld candidate, or while the host is unavailable.

## Worker resumption

A worker whose turn ends with no artifact, no finishing receipt and no
handoff that asks the parent for something (`commit_paths_ready`,
`lease_requested`, `ownership_requested`, `staging_gate_request`,
`round_verified`, `round_failed`, `reassignment_required`; a `waiting` handoff
is not one) is resumed with:

> Your work is not finished: continue until its required checks pass and you
> record the finished receipt; a wait is not a stop—poll the build or deploy
> yourself.

It resumes on its open sub-session when that is still live, otherwise as a
fresh attempt in the same workspace, after 30 s, then 2 min, then every 5 min,
whenever a slot is free. Three identical failure observations suspend retry and emit
one reconciliation wake. Missing workspaces and unmet prerequisites suspend before
any model attempt. The fingerprint includes reason, prerequisite, artifact, workspace
lease and review. Changed conditions rearm recovery; explicit parent retry remains
possible. Paused work never auto-resumes. Accepted unchanged packets resume finish-work.
An `ownership_requested` or `reassignment_required` handoff also fences the old brief through
the existing reconciliation state. Once the attempt stops, the parent resolves its scope and
source ownership with `planner_workflow reconcile` before dispatching again. Merely restarting
the same assignment cannot satisfy the request or count as active failed-gate repair progress.

A parent's background dispatch claims the work before returning its job id. Claim errors
are synchronous tool failures, not orphaned background results. Later background failures
record a parent wake and remain readable through `delegate_result`, even without a child
session row. `delegate_await` collects completed failures without waiting on live planner
jobs. A rejected review still needs explicit dispatch of its correction; verify the owning
attempt and actual activity instead of repeating the initial background acknowledgment.

Adopted assignments with `reconciliation.required` stay fenced until the parent reconciles
their scope and source. Their recovery wakes do not stop capacity checks, independent
auto-admission, or release/audit reminders. Ownership conflicts and dependencies still block
the affected candidates through the normal admission and dispatch checks.

A waiting handoff can name `ci:<work_id>@<full SHA>` as a whitespace-delimited token.
A fresh matching pending observation parks it without a model turn. Terminal, missing,
stale (two minutes) or failed observations restore normal reconciliation. Observations
never supply branch qualification themselves.

## In-flight wakes

While a durable release checkpoint, a legacy `release` work, or any running
work is mid-flight, an idle parent gets an `inflight:` wake after
`wake_after_secs`, then at most every `inflight_wake_secs`. It must not sit
idle after a staging deploy.

## Visibility

`planner_workflow status` carries a `capacity` block: `live`, `max`, `free`,
`host_configured`, the last host answer (`at_ms`, `ok`, `error`, and each
candidate's `open` and `held` state), `unheld`, active `holds`,
`unfilled_wakes`, `last_auto_admission` and `resumes`. The census line on
every wake repeats the host state, the holds and the last auto-admission.

`configured_at_start` and `effective_limit` distinguish file settings from running
permits. Direct ACP `router.worker_limit` acknowledges immediate increases; decreases need
a configured restart. Kory's current Goose config handler does not forward this custom
option; use the configured limit on restart there, and never infer live capacity from
a file edit. Targeted `status {work_id}` gives compact capacity. Progress
separates work counts from host-observed landed guides. See [durable progress](planner-throughput.md).

Router-owned `capacity:`, `auto-admit:`, `inflight:`, `audit:`, `gate:` and
`adopt-native:` wakes are acknowledged once delivered; the router recreates
them while their condition holds.

# The native watcher's guarantees

A native implement-all run had a watcher (an audit, a Stop guard, verified
holds, checked dispositions, a graceful shutdown). Under a planner run the
router enforces the same guarantees (`src/planner_guard.rs`,
`src/planner_shutdown.rs`). Each host below launches like `workspace` and
`capacity`: no shell, cwd is the session repository, one JSON object on stdin.
A host that fails, times out or answers malformed JSON is never read as a
clean or valid answer.

## Configuration

```yaml
routers:
  planner:
    audit:
      command: planner-audit-host       # required when the block is present
      args: []
      timeout_ms: 120000                # default 120000 (one audit fetches)
      interval_secs: 60                 # default 60
    integrate_check:
      command: planner-integrate-check  # required when the block is present
      args: []
      timeout_ms: 30000                 # default 30000
    shutdown:
      command: planner-shutdown-host    # optional: without it nothing is saved
      args: []
      timeout_ms: 120000                # default 120000
      drain_timeout_secs: 900           # default 900
```

Every block is optional. Without `audit`, the gate below still enforces
capacity candidates, and holds and completion are not audited.

## Audit

The capacity tick sends, at most every `interval_secs` and again after every
parent turn end, whatever the free slots:

```json
{"event":"audit","cwd":"<repo>","session_id":"…","free":0,"max":20,
 "run_status":"running","completing":false,
 "works":[{"work_id":"…","plan_id":"…","status":"running","external_id":"…","revision":"…"}],
 "holds":[{"plan_id":"…","reason":"…","detail":"…","expires_at_ms":0}]}
```

The host answers `{"defects":[{"code":"STAGING_IDLE","detail":"…"}]}`. Any
defect, or a host that cannot answer ("audit host UNAVAILABLE"), wakes the
idle parent with an `audit:` wake listing them, after `wake_after_secs`
(capacity timing) and again every `wake_after_secs` while they remain, with no
limit. `planner_workflow status` carries the last answer in its `capacity`
block (`audit`, `audit_findings`, `audit_configured`), and every wake's census
names the defect codes. While the audit has defects, full worker slots no
longer excuse the 20-minute heartbeat.

## Turn-end gate

When the parent's turn ends while the audit (taken after that turn ended) has
findings, or while a slot is free and the capacity host lists an unheld
candidate, the router re-prompts the parent with a `gate:` wake listing them:
at once, then 30 s, 2 min and every 5 min after each further turn end on the
same condition, with no cap. A clean turn end resets the backoff
(`capacity.gate` in `status`). This is the native Stop guard; it does not use
`max_continuations`.

## Wakes never give up

An unacknowledged planner wake is redelivered 30 s after its first delivery,
2 min after its second and every 5 min after that, with no limit; nothing
drops it after three deliveries.

## Planner children never escape unfinished

For a planner child (a delegate started for a `work_id`):

- a lifecycle host's `delegate_turn_end` continuation (exit 2) is sent with no
  cap, immediately and then after 30 s, 2 min and every 5 min, ignoring
  `delegation.lifecycle_hook.max_continuations`; it stops only when the run
  stops, the work is paused, the attempt ends or a drain cancels the worker;
- a lifecycle hook that fails, times out or exits 2 without a message
  continues the child instead of releasing it;
- a `delegate_stop` the host refuses with exit 2 means the worker stopped
  with dirty or unpushed source: the work is set `interrupted` with that
  reason (its handoff cleared), worker resumption brings it back to commit and
  push, and the event is accepted instead of redelivered forever. Any other
  refused event stays queued as before.

## Completion, holds, integration

- `complete` also sends the audit with `"completing":true` and is refused while
  it reports any defect (including `CONTINUE_NOT_FINISHED`) or cannot answer.
- `hold` first passes the router's own reason and evidence rules, then sends
  the audit with `"hold_check":{"plan_id","reason","detail","expires_at_ms"}`.
  The hold is refused, quoting `reason`, when the answer has
  `"hold_valid":false`, and refused as retryable when the host cannot answer.
- `integrate`, and a `disposition` to blocked or watching, send the operation
  JSON (`{"action":"integrate","key","work_id","receipt":{"revision","evidence"}}`
  or `{"action":"disposition","key","work_id","status","reason"}`) to
  `integrate_check` after the router's own checks. Exit 0 accepts; exit 2
  rejects with its stderr; anything else rejects with a message saying it is
  retryable. A rejected operation records nothing.

## Native adoption

On a run's first `planner_allocate` (once per run, before that allocation),
the workspace host gets `{"event":"adopt-native","session_id":"…"}` and
answers `{"adopted":null}` or
`{"adopted":{"run","session_id","runtime","cancelled","workers":[…]}}`. A
non-zero exit is retried (four tries, 2 s apart). The answer, or the failure,
goes to the parent in an `adopt-native:` wake and to `status`
(`capacity.native_adoption`). A workspace host must therefore answer this
event as well as `planner_allocate`.

## Shutdown drain

kory's shutdown command sends the ACP extension request
`_router/planner_shutdown` with `{"sessionId"}`. The router answers
`{"accepted":true}` at once (an error when the session has no planner run; a
second request joins the running drain), then reports each step as the
notification `_router/shutdown_progress` with
`{"sessionId","phase","message","saved":[…],"workersTotal","workersStopped","workers":[{"workId","status"}]}`
(`phase` is `saving`, `saved`, `draining`, `drained` or `failed`; a worker's
`status` is `running`, `notified`, `stopped` or `cancelled`):

1. The run becomes `draining`: admissions (`admit`, `hold` and `complete` are
   refused with "run is shutting down"), capacity and audit wakes,
   auto-admission, worker resumption, in-flight wakes and the turn-end gate
   all stop. Workers and the parent can still record artifacts, reviews,
   receipts and dispositions.
2. `saving`, then the shutdown host gets
   `{"event":"shutdown","session_id","cwd","run_status","works","holds"}`
   (`run_status` is the status before the drain) and answers
   `{"saved":[paths],"summary":"…"}`: `saved` carries the paths, with the
   summary as its message. A failure is reported as `failed` with the error,
   and the drain goes on.
3. The parent, if loaded, is prompted once: "Shutdown requested by the
   operator: rewrite Docs/CONTINUE.md and your registers now with the exact
   resume point; admit nothing; end your turn." The router waits for that
   turn, at most 10 minutes in all. A busy router-owned turn is cancelled through
   the same preemption path as operator input, then the shutdown prompt runs.
   Only an operator turn is allowed to finish naturally. While waiting for either
   cancellation, an operator turn or the shutdown turn, `shutdown_progress`
   reports progress every 30 seconds.
4. Every live worker (a turn running, or an open sub-session) is told:
   "Shutdown requested: commit and push your branch now, record your status
   notes and exact resume point, then hand off `waiting` with that resume
   point and stop." A mid-turn worker gets it at its turn end; an idle one as
   a follow-up turn. Each gets `drain_timeout_secs` to stop and is then
   cancelled. `draining` reports every change.
5. The run becomes `paused` (`drained`); the next `implement` command resumes
   it.

A router that dies mid-drain leaves the run `draining`; the next `implement`
command (or a person's follow-up) resumes it exactly like `paused`. While a
drain is live in this router, the `implement` command is refused until the
drain reports `drained`.

If a replacement router receives `implement` before the previous owner exits,
it records `capacity.pending_adoption` and an `adoption:handoff` wake. The normal
capacity cycle retries that exact handoff at most every 15 seconds; no second
command or monitor prompt is needed. Only new admissions wait: worker resumption,
audits, release observations and existing-work mutations continue. The wake records
the live owner, attempt or wake claim blocking adoption, with one user notice per
changed reason. If the source becomes terminal, is superseded by another session,
or its run/session disappears, the handoff clears with that reason and admission
continues in the same tick. Successful adoption wakes the parent to reconcile the
transferred assignments. Merely loading a session does not create an adoption request.

# Operator input preempts router-owned turns

`_router/planner_status` with `{"sessionId":"…"}` returns the existing read-only
capacity census as `{"status":"running","live":2,"admissible":7}`. An unknown
capacity-host answer gives `admissible:null`. Kory uses it after a turn cancellation
and before exit: cancelling a parent turn leaves the run's admission and recovery
loops active. Shutdown drains; exit disconnects and cuts workers off.

A turn that delivers wakes (child, heartbeat, capacity, audit, in-flight,
gate) is router-owned (`src/planner_preempt.rs`). Operator input never waits
behind one:

Router-generated steering is limited to queued agent corrections, `release:*`
transitions and `observation:ci-job-failures*`, at most once per session per 60
seconds across turn boundaries. Routine `observation:ci:<work>` wakes wait for
the boundary. Every injected block explicitly identifies itself as router text,
not operator input; its durable wake still requires acknowledgment at delivery.
If an adapter declines steering a queued agent correction, the existing preemption
path cancels only the router-owned turn and delivers the correction from the queue;
the interrupted wakes remain owed. The router rechecks withdrawal, acknowledgment,
delivery claims and queued operator input before cancellation. Operator turns stay
protected. Routine wakes do not acquire this cancellation fallback.

Planner `delegate_task` and `planner_workflow` permission callbacks are answered
with the adapter's allow option even on operator-owned turns. Other tools on
those turns retain normal client permission handling. Wakes and delegates keep
their existing automatic permission handling.

Adopted human inputs already delivered by an earlier parent carry `replayed_from`
and a `HISTORICAL` header. They reconcile missing receipts; the parent acknowledges
them in the delivery turn without answering or acting on the old text again.
Acknowledging the `input:` wake closes both records. A genuinely undelivered queued
request retains its first delivery instead of becoming an acknowledgment-only replay.

- A client prompt that arrives while a router-owned turn holds the parent is
  first steered into it with `_session/steering`
  (`_meta.steering.idleBehavior: "promptRequired"`). claude-agent-acp and
  codex-acp support it (they advertise `_meta.steering.supported` in their
  initialize response); grok does not. An `injected` outcome answers the
  prompt at once: the parent sees the words at its next step, and the input
  gets a delivery receipt for the parent to acknowledge.
- A mode command, or a steer the adapter does not take (an error,
  `declined`, `promptRequired`, no answer within 15 s), cancels the
  router-owned turn (`session/cancel` downstream) and runs the prompt as the
  very next turn, on its own request. The cancelled turn's wakes are not
  acknowledged, including router-owned ones, and go again right after the
  operator's turn. If the cancelled turn does not end within 60 s, the prompt
  is queued as before.
- A client `_session/steering` the downstream declines during a router-owned
  turn also cancels that turn, so the client's follow-up prompt runs at once.
- An operator turn in flight keeps the queue: the next prompt is retained for
  the next parent turn, and the wake loop always delivers queued operator
  input before any wake.
