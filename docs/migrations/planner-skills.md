# Planner skills migration

This document defines planner schema v1 and the consumer migration templates.
It matches the current `src/config.rs`, `src/planner_skills.rs`, `src/planner_workflow.rs`, `src/delegate_mcp.rs`, and `src/delegate_hook.rs` implementation on this branch.

The Chordzy inventory compares its earlier review baseline with current Tuneality staging `bb9d0c4058f12f18b4988b913c68c764c365c502`.
The planner, release, smoke, review, roadmap, and hook sources have changed since that earlier review. This document uses the current staging receipt and does not treat baseline blobs as current-source evidence.
It does not prove router execution, live hook delivery, deployed release behavior, native consumer parity, or end-to-end parity.

Runtime register contents, capacity, queue state, active runs, and watcher state are current-at-audit inputs.
They must be reread at invocation and must not become baked queue policy in this document, the YAML, or the role templates.
The current `Docs/CONTINUE.md` receipt records one legacy run with only its authorized existing watcher.
Keep that watcher until explicit migration or drain authorization and native parity evidence exist.

## Decisions I made

1. Chordzy maps all six roles explicitly through `routers.planner.<hyphen-role>.skill`.
2. `create-plan` maps to `plan`, and `implement-work` maps to the narrowed `implement` skill.
3. `select-plan`, `review-work`, `finish-work`, and `integrate-plan` map to exact repository skills with those names.
4. An explicit mapped skill must resolve. A missing, unreadable, invalid, or ambiguous mapping fails visibly.
5. Without a mapping, the resolver checks the exact role name, then the bundled generic Markdown asset.
6. Generic router assets contain no Chordzy, Linear, deployment, or product-specific policy.
7. A run stores the complete resolved policy snapshot. Resume does not reread changed skill files.
8. `planner_workflow` owns typed lifecycle state. Repository skills own policy and repository checks.
9. `delegate_task.work_id` is the durable child entrypoint. Omitted `work_id` keeps legacy lightweight delegation.
10. The lifecycle hook and outbox remain the one hook delivery owner. A delivered wake still needs parent acknowledgement.
11. The router owns idle-parent turns. The optional wake extension exposes notifications without adding a scheduler.
12. Fleet YAML stays disabled until a compatible router SHA is merged and pinned.

## Schema v1

The neutral configuration is copyable YAML. The six role objects are optional. Each role object accepts only the `skill` field in the current module.

```yaml
routers:
  planner:
    profile: markdown
    roadmap: ROADMAP.md
    workspace:
      command: planner-workspace-adapter
      args: []
      timeout_ms: 120000
```

The minimum configuration is:

```yaml
routers:
  planner: {}
```

`profile` accepts `markdown` or a repository-relative Markdown path.
The default is `markdown`.

Do not copy role mappings into a generic repository unless those exact repository skills exist.
An explicit mapping is an assertion that the named repository skill is present.

`roadmap` is optional and stores a repository-relative path for policy use.
The core does not interpret roadmap contents.

The six role keys are `create-plan`, `select-plan`, `implement-work`, `review-work`, `finish-work`, and `integrate-plan`.

The workspace adapter is optional.
The default creates an isolated local clone.

### Skill resolution

The resolver applies these steps for every role.

1. If the YAML role has `skill`, resolve that name and fail if it is missing, unreadable, invalid, or ambiguous.
2. If the YAML role has no mapping, use the exact role name as the lookup name.
3. Search `.agents/skills/<name>/SKILL.md`, `.claude/skills/<name>/SKILL.md`, and `.codex/skills/<name>/SKILL.md`.
4. Canonical aliases count as one file. Distinct files with the same name are ambiguous and fail.
5. If no repository skill exists for the lookup name, use the bundled generic Markdown asset for the role.

Skill names contain only ASCII letters, digits, hyphens, and underscores.
The explicit profile path is joined to the session repository and canonicalized.

Each resolved skill stores `name`, `source`, `hash`, and full `text`.
The resolved planner stores `schema`, `identity`, `policy`, all six `roles`, `roadmap`, `host_instructions`, and `workspace`.
The planner identity hashes the serialized resolved planner.

The run stores this resolved planner in `PlannerRun.policy`.
Changing a repository skill after a run starts does not reinterpret that run.

### Workspace adapter

The configured command runs with the session repository as its current directory.
The router writes one JSON assignment to standard input.

```json
{
  "event": "planner_allocate",
  "session_id": "router-session-id",
  "work": {
    "work_id": "work-001",
    "plan_id": "plan-001",
    "scope": "docs/migrations/planner-skills.md",
    "dependencies": [],
    "required_checks": ["markdown-check"],
    "required_integration_evidence": ["integration-receipt"],
    "external_id": "opaque-consumer-id"
  },
  "child_id": "child-opaque-id"
}
```

The adapter returns JSON that deserializes as `Workspace`.

```json
{
  "path": "/isolated/planner/child-opaque-id",
  "lease": "lease-opaque-id",
  "environment": {},
  "mcp_servers": []
}
```

`environment` and `mcp_servers` are optional and default to empty values.
The router requires an accessible path and a nonempty lease.
The path must differ from the parent repository.
The router verifies that the path is a Git worktree root.

When `workspace` is omitted, the router clones into the state file parent under `planner-workspaces/<child_id>`.
The clone uses `git clone --no-hardlinks --quiet`.
The clone origin is then changed to the parent repository origin.
The parent checkout is never used for child writes.

## Neutral lifecycle contract

`/plan` selects `create-plan`.
`/implement` selects `select-plan`.
A direct human request beginning with `implement`, such as `implement ROADMAP.md`, selects the same implementation workflow.
Quoted examples and internal role or agent-origin messages do not authorize execution.
An implementation command records the authentic execution request before child admission.

Only the original leading human prompt is eligible for mode parsing.
The current parser reads the first text block after leading whitespace and checks its first token.
Expanded skill text, child text, tool output, and prompts marked with `router_acp.planner_role` or `agent_origin` do not select a mode.

The router preserves the original command and attachments in an `InputReceipt`.
The internal role prompt carries `planner_role` and `planner_input_id` markers.
An explicit repository `plan` or `implement` skill is included once when it is not already the resolved role source.

The coordinator flag is separate from phase.
An EPIC coordinator remains a coordinator on its planning model when `/implement` starts child execution.
The parent does not become an implementation worker.

```text
human command -> phase and input receipt -> resolved role -> typed lifecycle result
                                      |                         |
                                      v                         v
                              parent selects work       isolated child attempt
                                                              |
                                                              v
                                                   artifact and revision receipt
                                                              |
                                            parent review -> corrections or accept
                                                              |
                                             finish child -> parent integration
                                                              |
                                                   disposition -> refill or complete
```

### Roles and receipts

| Role | Owner | Required result |
| --- | --- | --- |
| `create-plan` | Parent | Plan, requirements, dependencies, and policy receipt. |
| `select-plan` | Parent | Stable plan and work identity, bounded scope, capacity result, and workspace decision. |
| `implement-work` | Child | Artifact revision, changed paths, required checks, evidence, and unresolved requirements. |
| `review-work` | Parent | Independent evidence and revision-bound acceptance or corrections. |
| `finish-work` | Child | Accepted revision, handoff, custody, and resource-close receipts. |
| `integrate-plan` | Parent | Integration evidence, disposition, register updates, and refill or completion result. |

The `-plan` roles govern the whole plan.
The `-work` roles govern one assigned child.

## Typed `planner_workflow` operations

The public tool name is `planner_workflow`.
The current operation enum uses kebab-case action names.

| Action | Current fields | Owner |
| --- | --- | --- |
| `status` | optional `offset` | Parent or worker read |
| `admit` | `key`, `work` | Parent |
| `artifact` | `key`, `work_id`, `attempt_id`, `artifact` | Assigned child |
| `review` | `key`, `work_id`, `revision`, `accepted`, `evidence`, `corrections` | Parent |
| `finished` | `key`, `work_id`, `attempt_id`, `receipt` | Assigned child |
| `integrate` | `key`, `work_id`, `receipt` | Parent |
| `disposition` | `key`, `work_id`, `status`, `reason` | Parent |
| `acknowledge-input` | `key`, `input_id`, `owner` | Parent |
| `acknowledge-wake` | `key`, `wake_id` | Parent |
| `complete` | `key`, `queue_evidence` | Parent |

Every mutation uses an idempotency `key`.
Reusing a key with different content fails.

`WorkSpec` currently contains `work_id`, `plan_id`, `scope`, `dependencies`, `required_checks`, `required_integration_evidence`, and opaque `external_id`.

`Artifact` currently contains `revision`, `changed_paths`, `checks`, `evidence`, and `unresolved`.

`Receipt` contains `revision` and an evidence map.

The current work statuses are `admitted`, `allocating`, `running`, `review-pending`, `corrections`, `accepted`, `finished`, `integrated`, `blocked`, `watching`, and `interrupted`.

A child actor can use only `artifact` and `finished`.
The actor identity must match `work_id`, `attempt_id`, router process identity, and an active attempt.
The child cannot admit, review, integrate, dispose, acknowledge, or complete through the tool.

Review checks the current workspace revision.
Acceptance requires an artifact with the same revision and usable independent evidence.
Unresolved artifact requirements prevent acceptance.
Rejected review requires bounded corrections.

Finishing requires accepted review for the same revision.
Finishing also requires usable handoff and custody evidence.

Integration requires the accepted finished revision, an ended child attempt, verified revision state, and all required integration evidence.

Completion requires every admitted work item to be integrated.
It also requires every input and wake to be acknowledged.
Blocked or watching work remains pending and therefore cannot satisfy the current `complete` operation.

### Durable child delegation

`delegate_task` has an optional `work_id`.
Omit it for the existing lightweight delegation path.

When present, the router allocates or reuses the isolated workspace, creates an attempt, sets `keep_open`, and binds the child identity to that attempt.
The child receives its own worker MCP tools: `worker_whoami`, `worker_handoff`, and `planner_workflow`.

The child prompt carries an internal `router_acp` marker with `planner_role: implement-work`, `work_id`, `attempt_id`, and `agent_origin: true`.
This marker prevents child prose from being parsed as a new human mode command.

The child reports its artifact before parent review.
The parent sends corrections through the same durable work identity.
A replacement process receives a new attempt for the same work identity after the old attempt ends.

## Wake and recovery

The existing delegated lifecycle hook and SQLite outbox carry the `planner_wake` event.
The event includes `wake_id`, parent router session, `work_id`, `attempt_id`, revision, and router process identity.

The outbox retries hook delivery.
Successful hook delivery does not acknowledge the parent wake.

The parent acknowledges through `planner_workflow` `acknowledge-wake` or a prompt metadata field named `router_acp.planner_wake_ack`.

If the upstream client advertises `_meta.router_acp.planner_wake: true`, the router also sends `router-acp/planner_wake`.
The router's idle-parent loop delivers reconciliation turns without requiring that optional notification capability.
Delivery requires a running router and the exact parent session loaded.
Active turns, pauses, cancellation, and unanswered approvals suppress automatic delivery.
The loop fences each delivery to its owning process and bounds delivery attempts to three.
Original queued parent requests run separately in arrival order and retain their own controls and attachments.
Queued and reconciliation turns apply the same repository skill routing as ordinary turns.
After router exit, the client must load or resume the exact parent before automatic delivery can continue.

```text
child ends -> durable wake -> router-owned idle-parent turn -> acknowledgement
                  |                     |
                  v                     v
             hook outbox           one fenced lifecycle action
                  |
                  v
        optional client notification
```

Do not add a second wake scheduler in a consumer.
Do not treat an outbox delivery receipt as a parent resume receipt.
Do not infer a parent session from the most recent session.

The existing delegated events remain the hook contract.
They include `delegate_start`, `delegate_turn_end`, `delegate_stop`, and `parent_repinned`.
Worker handoff kinds remain `commit_paths_ready`, `waiting`, `lease_requested`, `ownership_requested`, `staging_gate_request`, `round_verified`, `round_failed`, and `reassignment_required`.

## Neutral generic example

This example is for a repository with local Markdown plans and repository tests.
It has no Linear, Kory, Chordzy, deployment, or product-specific requirement.

```yaml
routers:
  planner:
    profile: markdown
    roadmap: ROADMAP.md
```

1. The user sends `/plan add a bounded parser improvement`.
2. The parent runs `create-plan` and records a plan path and required checks.
3. The user sends `/implement` for the authorized plan.
4. The parent runs `select-plan` and admits one `WorkSpec`.
5. The parent calls `delegate_task` with the admitted `work_id`.
6. The child reports `artifact` with the current Git revision and test evidence.
7. The parent sends `review` with acceptance or corrections.
8. The child sends `finished` after the accepted revision and handoff checks.
9. The parent sends `integrate` after repository integration checks.
10. The parent sends `complete` only after all receipts and wakes are reconciled.

The generic profile must require repository-declared checks.
It must leave deployment evidence pending when the repository declares deployment as required.

## Consumer examples

The following examples are consumer-specific.
They do not belong in the bundled generic assets.

### Chordzy

Chordzy uses the Markdown profile plus its repository-owned skills.

```yaml
routers:
  planner:
    profile: docs/router-planner-policy.md
    roadmap: Docs/ROADMAP.md
    create-plan:
      skill: plan
    select-plan:
      skill: select-plan
    implement-work:
      skill: implement
    review-work:
      skill: review-work
    finish-work:
      skill: finish-work
    integrate-plan:
      skill: integrate-plan
    workspace:
      command: chordzy-planner-workspace
      args: []
      timeout_ms: 120000
```

This mapping requires these exact files before enablement:

```text
.agents/skills/plan/SKILL.md
.agents/skills/select-plan/SKILL.md
.agents/skills/implement/SKILL.md
.agents/skills/review-work/SKILL.md
.agents/skills/finish-work/SKILL.md
.agents/skills/integrate-plan/SKILL.md
```

The consumer profile and allocator are also required:

```text
docs/router-planner-policy.md
chordzy-planner-workspace
```

The migration templates for these artifacts are `templates/chordzy/policy.md` and `templates/chordzy/workspace-adapter.md`.

The explicit `plan` and `implement` entrypoints execute once under their owning role.
The role marker prevents an internal invocation from being parsed as a second user command.

Chordzy `implement` must become bounded implementation work.
It must retain assigned plan reading, exact path ownership, focused checks, migration rehearsal, branch evidence, status notes, and `COMMIT PATHS READY`.

Chordzy `implement` must remove roadmap selection, delegation, staging integration, deployment, full release smoke, Completed moves, roadmap retirement, and final disposition.
Those responsibilities move to `select-plan`, `integrate-plan`, and the other role templates below.

`implement-all` remains the public operator wrapper.
It retains live side questions, `Bug: ...`, `report`, original-owner routing, approved scope, capacity, custody, and terminal checks.
It calls router lifecycle operations instead of starting a second scheduler.

Chordzy must preserve exact source contracts at these audited paths and sections:

1. `.agents/skills/plan/SKILL.md`, `plan`, `Ground the idea`, `Decide and write`, and `Finish`.
2. `.agents/skills/implement/SKILL.md`, lines 19–53 for moved admission, lines 55–100 for bounded implementation, lines 102–130 for moved release, and lines 132–180 for moved disposition.
3. `.agents/skills/implement-all/SKILL.md`, lines 8–85, 87–127, 138–202, 204–228, 230–266, 268–393, 395–645, 646–799, 801–940, 943–1000, and 1002–1035.
4. `.agents/skills/implement-all/Supervisor.cs`, `init`, `rebind`, `heartbeat`, `status`, `tick`, `watch`, `stop`, and `self-test`.
5. `.agents/skills/implement-all/CliAdapter.cs`, fenced `probe` and `wake` handling.
6. `.agents/skills/implement-all/references/supervisor.md`, parent-only watcher reload and receipt census.
7. `.agents/hooks/ImplementAllGuard.cs`, activation, capacity, worker ownership, audit, custody, cleanup, `complete`, and `cancel`.
8. `.agents/hooks/RepoGuard.cs`, source and shell policy.
9. `.agents/skills/deploy/SKILL.md`, Steps 0–10.
10. `.agents/skills/smokescreen/SKILL.md`, Steps 0–7 and its `Chrome.cs`, `DevTools.cs`, `App.cs`, `Unity.cs`, and `ServerLogs.cs` helpers.
11. `.agents/skills/review/SKILL.md`, `Consolidate verification`, `Verify`, and `Disposition`.
12. `.agents/skills/roadmap/SKILL.md`, `Audit scope`, `Read and recheck`, `Rank`, and `Write and verify`.
13. `Docs/Plans/README.md`, `Docs/WATCH.md`, `Docs/BLOCKED.md`, `Docs/CONTINUE.md`, `Docs/Completed/README.md`, and `Docs/Reviewed/README.md`.

The [source-to-test inventory](chordzy-preservation-inventory.md) remains the receipt for this migration.
Existing source and fixture tests are evidence only.
Runtime parity remains pending until router consumer tests and live workflow tests pass.

### Chordzy invocation examples

The examples below keep the operator command at the original prompt boundary.
They show the expected role, workspace, authorization, and receipt.

#### `/plan <context>`

```text
/plan Reopen the guide for the MIDI import regression.
```

The parent runs `create-plan`.
The existing `.agents/skills/plan/SKILL.md` contract owns source grounding, architect review, and roadmap review.
No child workspace is allocated yet.
The result is a plan path, Why/Do/Done, dependency receipt, and roadmap receipt.

#### `/implement <plan>`

```text
/implement Docs/Plans/midi-import-regression.md
```

The parent runs `select-plan` against that plan.
The authentic command supplies implementation authorization for the selected scope.
The parent admits a stable `WorkSpec` and allocates an isolated Chordzy worktree under repository policy.
The child runs `implement-work` and returns an artifact revision.
The parent reviews that revision before `finish-work` or `integrate-plan`.
The command does not authorize production deployment by itself.

#### `/implement` without arguments

```text
/implement
```

The parent calls `select-plan` once.
It reads approved scope, dependencies, repair priority, capacity, and current receipts.
It returns one actionable, waiting, blocked, or complete result.
It does not create a duplicate coordinator after resume or retry.

#### `implement-all` with live `Bug:` and `report` input

```text
implement-all Docs/ROADMAP.md
Bug: MIDI import drops the first note after the active child changed TuneMidi.
report
```

The router stores each message as an `InputReceipt`.
The Chordzy policy identifies the existing guide and work owner.
The parent sends a bounded correction to that owner.
The parent rescales the owning guide and keeps unrelated approved work running.
The `report` response reconciles Production, Staging, and WIP evidence without starting another smoke.
The parent acknowledges the inputs before terminal completion.

The expected evidence is the original owner, scope, source or screenshot evidence, bounded correction, roadmap update, report output, and input acknowledgement.

## Chordzy source-to-test preservation matrix

This matrix expands the inventory into migration receipts.
“Pending” means the staging source or fixture is evidence only.

| Responsibility | Profile or guard owner | Receipt after migration | Required test or current status |
| --- | --- | --- | --- |
| Grounded plan and roadmap ownership | `plan/SKILL.md`, `Docs/Plans/README.md`, `roadmap/SKILL.md` | Plan path, Why/Do/Done, architect receipt, roadmap receipt | Existing contracts. Router mapping and once-only execution are pending. |
| No-argument selection, approved subsets, dependencies, supersession | `select-plan`, `implement-all` lines 395–645 | Stable plan/work identity, approved scope, dependency heads, selection result | Guard fixtures exist. Router once-only and stable identity tests are pending. |
| Capacity and underfilled waits | `implement-all` lines 268–285 and 403–413, `ImplementAllGuard.cs` | Capacity census, worker assignment, waiting reason | Fixture coverage exists. Live census and model behavior are pending. |
| One worker, worktree, branch, and path ownership | `implement-all` lines 415–446 and 646–733 | Workspace lease, branch SHA, exact path set, custody owner | Guard fixtures exist. Router allocation and duplicate child tests are pending. |
| Focused checks and branch readiness | `implement-work`, `implement-all` lines 646–733 | Artifact revision, changed paths, check map, evidence | Hook tests exist. Child no-deploy and no-retire enforcement is pending. |
| Live operator steering and `Bug:` | `implement-all` lines 48–49 and 204–228 | Input owner, attribution, bounded correction, acknowledgement | Direct active-run test is pending. |
| `report` | `implement-all` lines 138–202 | Production, Staging, WIP report and unknowns | Direct busy-lane report test is pending. |
| Fenced supervisor activation and wake | `Supervisor.cs`, `CliAdapter.cs`, supervisor reference, `ImplementAllGuard.cs` | One lifecycle owner, wake id, session fence | Self-tests exist. Live wake, restart, and delivery remain pending. |
| Lifecycle hook identity | `ci/agent-hooks.json` and generated hook outputs | Router pid, start time, worker and parent identities | Source-rendered checks exist. Runtime delivery remains pending. |
| Stop, handoff, custody, and cleanup | `ImplementAllGuard.cs`, `finish-work` | Handoff, resource close, custody hash, safe cleanup | Guard and cleanup fixtures exist. Live NAS and stop delivery remain pending. |
| Datadog three-stream inspection | `implement-all` lines 329–376 and `smokescreen/ServerLogs.cs` | Cursor, overlap, retry, raw hash, unknown classification | Fixture coverage exists. Live provider coverage is pending. |
| Bounded release batch and release worker | `integrate-plan`, `implement-all` lines 632–645 and 801–940 | Ready-head batch, release worker, exact candidate | Direct orchestration and staging runtime tests are pending. |
| Deploy identity and rollout | `deploy/SKILL.md` Steps 1–10 | Commit, tag, Buildkite, rollout, DataReady, Lighthouse | Unit fixtures exist. Deployment runtime is pending. |
| Full web and app smoke | `smokescreen/SKILL.md` Steps 0–7 and helpers | Staging and production smoke, log, cleanup, client identity | Helper tests exist. Exact-candidate live smoke is pending. |
| Parent review and same-child correction | `review-work`, `implement-all` lines 943–975 | Revision-bound verdict or correction to same `work_id` | Direct stale-revision and process-replacement tests are pending. |
| Every-Done and Completed move | `integrate-plan`, `Docs/Completed/README.md` | `git mv`, preserved history, links, counts, proof | End-to-end disposition test is pending. |
| Missing proof and failed assessment | `integrate-plan`, owning guide | `VERIFICATION PENDING` outside Completed | Missing-proof test is pending. |
| Roadmap and register refresh | `roadmap/SKILL.md` and planning registers | Reconciled scores, links, counts, Immediate or Deferred rows | Register reconciliation test is pending. |
| Final planning disposition | Release worker and `integrate-plan` | Integrated disposition receipt | Final disposition integration test is pending. |
| Separate independent review | `review/SKILL.md` | Completed-to-Reviewed receipt | Existing source contract. Router must not substitute parent review. |
| Unattended blockers | `implement-all`, `Docs/WATCH.md`, `Docs/BLOCKED.md` | Prepared handoff, narrow blocked state, continued independent work | Scoped blocker test is pending. |
| Monitoring, custody, and recovery | `Docs/CONTINUE.md`, guard, supervisor | Atomic checkpoint, census, orphan recovery, disk floor, custody hash | Fixture coverage exists. Restart and orphan runtime checks are pending. |
| Newer staging audits | Staging `implement-all`, guard, server logs | Queue pin, Done receipt, drift, telemetry, three-stream inspection | Final-baseline runtime audit is pending. |
| Terminal completion and cancellation | `implement-all` lines 1002–1035 and guard `complete`/`cancel` | Honest disposition, no active worker or release, checkpoint-before-cancel | Router terminal receipt and cancellation tests are pending. |

## Validation plan and acceptance gate

Run these checks in order after the consumer edits exist.

1. Validate the exact Chordzy YAML with the compatible `router-acp check-config` binary. Confirm all six mapped `SKILL.md` files resolve, and confirm a deliberately missing explicit mapping fails.
2. Exercise resolver tests for explicit mapping, exact-role lookup, bundled fallback, canonical aliases, distinct-file ambiguity, repository-boundary rejection, and stored policy identity.
3. Exercise command tests for `/plan`, `/implement <plan>`, and `/implement` without arguments. Confirm one mode selection, one mapped skill execution, preserved arguments and attachments, internal-marker suppression, coordinator retention, and resume deduplication.
4. Exercise lifecycle tests for one `WorkSpec`, isolated `Workspace`, required checks, bounded changed paths, stale revision rejection, same-child corrections, accepted-revision finishing, ended-attempt integration, and terminal fencing.
5. Exercise Chordzy consumer tests for `implement-work` deployment and retirement refusal, `Bug: ...` and `report` while lanes are busy, original-owner routing, restart recovery, one recovery owner, and no duplicate coordinator.
6. Retain and rerun the existing Chordzy guard, supervisor, hook-generation, smoke-cleanup, server-log, CI-policy, and release tests named in the inventory.
7. Exercise the complete release path through WIP qualification, staging smokescreen, the exact production candidate gate, production smokescreen, every-Done assessment, Completed or verification-pending disposition, roadmap/register refresh, and final disposition integration.
8. Exercise failure re-formation, outage hotfix priority, shared staging changes, custody and disk-floor handling, scoped WATCH/BLOCKED work, cancellation, and final completion.

Mock or fixture tests can prove router state transitions and receipt validation.
They cannot prove external Chordzy or Goose parity.
Native Goose runs, Chordzy consumer runs, live hook delivery, exact-candidate release evidence, and deployed smoke remain required acceptance evidence.

Do not retire `ImplementAllGuard`, `Supervisor`, or `CliAdapter` new-run behavior until the full gate passes.
Record the exact failed matrix row and keep the legacy activation path when a gate fails.

### Hickory

Hickory uses an explicit repository policy file and keeps Linear and ticket binding in consumer policy.

```yaml
routers:
  planner:
    profile: kory-code/profiles/hickory.md
    create-plan: { skill: create-plan }
    select-plan: { skill: select-plan }
    implement-work: { skill: implement-work }
    review-work: { skill: review-work }
    finish-work: { skill: ship-pr }
    integrate-plan: { skill: integrate-plan }
    workspace:
      command: node
      args: [kory-code/relay/planner-allocator.mjs]
      timeout_ms: 120000
```

The Hickory workspace adapter must allocate pool checkouts only.
It must return the checkout path and a durable lease.
It must reject a shared checkout, a parent checkout, or an untracked lease.

The Hickory policy keeps Linear ticket binding, exact handoff labels, the existing EPIC parent and child presentation, and explicit `/ship-pr` human gates.
The child cannot manufacture merge or production deploy authority from prompt text.

Hickory role wrappers reuse existing repository skills.
`finish-work` maps to `ship-pr`.
The wrapper passes the accepted artifact revision, ticket identity, branch identity, and existing authorization receipt.

Hickory children keep a 1:1:1 ticket, child session, and PR mapping.
The EPIC parent owns coordination and has no implementation PR.

Hickory's relay settings generator supplies this configuration automatically when
`kory_code.planner_workflow` is enabled in its settings overrides.
The relay removes that relay-only key before router validation.
The generator also removes retired `orchestration` settings and clears the old
planning protocol so it cannot contradict the resolved repository policy.
It removes `pre_classifier.orchestrate_min_confidence` from defaults and overrides.
Explicit role, profile, workspace, and effort overrides remain authoritative.

The current fleet YAML and pin stay paired until the router change is merged.
After pin adoption and runtime checks, enable the workflow through the existing
settings API without replacing unrelated overrides.
Boot and save both validate the generated YAML with the installed router.
Only a successful configuration publication enables router ownership for new sessions.
A validation failure retains the prior configuration and does not enable new ownership.
Existing sessions retain their persisted owner while they drain.

## Migration ownership

The router owns durable planner state, child identity, attempts, revision-bound review, finish and integration receipts, input acknowledgements, wake ownership, and recovery fencing.

The consumer owns repository policy.
This includes scope eligibility, dependency meaning, authorization predicates, repository checks, deployment rules, disposition, and roadmap rules.

Chordzy keeps `RepoGuard`, pre-commit, pre-push, smokescreen, custody, and policy checks.
`ImplementAllGuard` and `Supervisor` remain available for existing runs during drain.
They must not allocate a second coordinator or second durable ledger.

Disable old new-run scheduling only after single activation, same-child correction, restart recovery, terminal fencing, generated-hook checks, guard self-tests, supervisor self-tests, and complete consumer parity tests pass.

## Responsibility to receipt to test matrix

| Responsibility | Receipt | Test or evidence |
| --- | --- | --- |
| Profile selection | `ResolvedPlanner.identity` and `policy` snapshot | Resolver precedence, explicit missing mapping, ambiguity, and restart tests. |
| Mode selection | `InputReceipt`, `PlannerRun.phase`, `planner_role` marker | Leading-token, quoted/example, internal-prompt, and attachment tests. |
| Plan creation | Plan path, requirements, dependency receipt | Chordzy `plan` contract and router once-only mapping test. |
| Work admission | `WorkSpec`, stable `work_id`, `child_id`, and status | No-argument selection once, dependency, subset, supersession, and capacity tests. |
| Workspace allocation | `Workspace.path` and `Workspace.lease` | Default clone isolation and consumer adapter lease tests. |
| Child implementation | `Artifact` with revision, paths, checks, evidence | Required-check, clean-revision, bounded-path, and no-deploy/no-retire tests. |
| Parent review | `Review.revision`, evidence, acceptance or corrections | Stale revision rejection and same-child correction tests. |
| Child finish | `Receipt.revision` and handoff/custody evidence | Accepted-revision, ended-attempt, custody, and cleanup tests. |
| Integration | `Work.integration` receipt and `integrated` status | Release worker, exact candidate, smoke, and disposition integration tests. |
| Operator input | `InputReceipt.owner` and `acknowledged` | Live `Bug: ...`, report, side question, attribution, and busy-lane tests. |
| Wake delivery | Outbox payload and `Wake.acknowledged` | Duplicate event, retry, native wake negotiation, and explicit resume tests. |
| Terminal completion | `Complete.queue_evidence` and complete run status | Empty queue, blocked work, unacknowledged wake, cancellation, and final disposition tests. |
| Chordzy release | Existing deploy and smokescreen receipts | Full staging, exact-candidate production, full smoke, and failure re-form tests. |
| Chordzy disposition | Completed or verification-pending guide plus roadmap/register receipt | Every-Done, links/counts, roadmap, custody, and final disposition integration tests. |
| Hickory finish | Existing ship-pr authorization and PR receipt | Ticket binding, exact handoff labels, human merge/deploy gates, and 1:1:1 tests. |

## Rollout, drain, and rollback

Use this order.

1. Merge router schema, resolver, typed lifecycle, role markers, workspace contract, and wake tests.
2. Add all six Chordzy role skills and the repository workspace allocator.
3. Validate the exact effective YAML with the compatible router binary.
4. Keep legacy scheduling available for existing runs.
5. Drain or explicitly migrate existing runs while preserving their recorded owner, scope, and pending receipts.
6. Enable one router lifecycle and wake owner.
7. Run the full source-to-test matrix and runtime consumer checks.
8. Disable legacy new-run activation only after parity passes.

Fleet YAML cannot be enabled before a compatible router SHA is merged and pinned.
The pin and configuration must be deployed as a compatible pair.

Rollback follows these steps.

1. Stop admitting new router-owned runs.
2. Keep existing router runs bound to their stored policy and identities.
3. Restore legacy activation only if it is the sole active coordinator owner.
4. Do not run legacy scheduling and router scheduling for the same run.
5. Reconcile outbox events, work attempts, leases, review revisions, and pending inputs before retrying.
6. Resume or migrate each run explicitly.

A lifecycle hook delivery retry is not a second owner.
A native wake notification is not a second owner.
The router remains the one lifecycle and wake owner after enablement.

## Pending checks and omissions

The current Rust module provides the schema and lifecycle primitives described above.
It does not prove consumer parity by itself.

The current module does not interpret Linear, Hickory, Chordzy, Unity, Buildkite, Datadog, Completed, Reviewed, WATCH, or BLOCKED policy.
Those remain profile responsibilities.

The current profile resolver rejects absolute profile paths and parent-directory components before canonicalization.
Keep the resolver test that exercises those repository-boundary checks.

The current external workspace path check rejects the parent path and requires a Git root.
It does not independently verify pool ownership or lease uniqueness.
The Hickory adapter must enforce those checks until the router adds them.

Protocol tests exercise idle-parent delivery and process fencing with mock adapters.
They do not prove live Goose or Kory wake behavior after router process exit.
Every client must load or resume the exact parent after router exit.

The Chordzy inventory does not prove live hook delivery, router execution, deployed release behavior, exact-candidate staging or production smoke, NAS custody, or end-to-end parity.

The pending runtime tests include crash and restart recovery, duplicate wake suppression, same-child correction, no duplicate coordinator, no-argument selection once, child deployment and retirement refusal, full release and disposition flow, failed-candidate reformation, report and Bug input during busy lanes, and final disposition integration.

Your decision: keep router core neutral, bind consumer policy through the six roles, and enable fleet YAML only with a compatible pinned router.
