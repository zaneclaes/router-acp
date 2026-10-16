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
repository. While the parent is idle below `delegation.max_concurrent` (and on
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
The router asks at most every 30 s while the parent is idle with a free slot,
and again after any parent activity.

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

An idle parent (its turn ended) with a free slot and an unheld candidate gets
a `capacity:` wake after `wake_after_secs`, and another every
`wake_after_secs` while that holds, with no limit on deliveries. The wake
lists the free slot count and each candidate (`work_id`, `external_id`, plan)
and tells the parent to admit up to the free count now, in order, or record a
`hold` for each one it skips. Breadth is always permitted: depth-first
ordering never leaves a slot empty. While the host lists unheld candidates or
cannot answer, these wakes replace the 20-minute heartbeat; the heartbeat
remains for "no candidates", so blockers are still re-checked.

## Auto-admission

After `auto_admit_after_wakes` delivered capacity wakes for the same unfilled
condition, or `auto_admit_after_secs` of parent idleness with a free slot, the
router admits and starts up to the free count of unheld candidates, in host
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
with no cap, whenever a slot is free. Interrupted works with a live workspace
(for example after a router restart) are retried the same way. A paused work
or a run outside the implementation phase is never resumed.

## In-flight wakes

While a `release` work (any work whose `plan_id` is `release`) or any running
work is mid-flight, an idle parent gets an `inflight:` wake after
`wake_after_secs`, then at most every `inflight_wake_secs`. It must not sit
idle after a staging deploy.

## Visibility

`planner_workflow status` carries a `capacity` block: `live`, `max`, `free`,
`host_configured`, the last host answer (`at_ms`, `ok`, `error`, and each
candidate's `open` and `held` state), `unheld`, active `holds`,
`unfilled_wakes`, `last_auto_admission` and `resumes`. The census line on
every wake repeats the host state, the holds and the last auto-admission.

Router-owned `capacity:`, `auto-admit:` and `inflight:` wakes are acknowledged
once delivered; the router recreates them while their condition holds.
