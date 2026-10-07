# Chordzy implementation inventory

Read-only audit of `Tuneality/Tuneality` staging. The audit used authenticated `gh api` reads at one fixed SHA. It did not modify the checkout, repository, ticket, branch, or runtime.

## Decisions I made

- I pin every source claim below to staging SHA `6cbc9e04dc83f49c1f663f969edf5568d3c53992`.
- I classify source and fixture coverage as existing evidence. I classify live hook delivery, router execution, deployed release behavior, and end-to-end parity as pending unless a source test directly proves them.
- I apply the approved Chordzy change. `implement` becomes bounded implementation work. Selection, coordination, release, disposition, and roadmap ownership move to the six planner roles.

## Baseline and instruction inventory

Staging resolves to:

```text
Repository: Tuneality/Tuneality
Branch: staging
SHA: 6cbc9e04dc83f49c1f663f969edf5568d3c53992
Commit: docs: carry final parent receipt reconciliation
Commit date: 2026-10-07T05:38:26Z
Parent: 01262fdda722fac3f1bcf93f800b5bb6f63dd1ac
```

The source links below all use this SHA:

`https://github.com/Tuneality/Tuneality/tree/6cbc9e04dc83f49c1f663f969edf5568d3c53992`

The root contract is [`AGENTS.md`](https://github.com/Tuneality/Tuneality/blob/6cbc9e04dc83f49c1f663f969edf5568d3c53992/AGENTS.md), blob `63dcdb5a20fe72c427d1d49dd369f47cf15d4ef6`. It defines the staging branch model, worktree ownership, release train, plan registers, hook installation, safe-change checks, deployment, and smokescreen gates.

The nested `AGENTS.md` files form two content groups:

- Blob `42061c01a1c70097d1e4579f29a5adf40abdec95` is shared by `Assets/`, `ChordzyGame/`, `JUCE/`, `TuneClient/`, `TuneCore/`, `TuneData/`, `TuneGpt/`, `TuneML/`, `TuneMidi/`, `TuneSvgs/`, `TuneTests/`, `TuneWeb/`, their listed server subtrees, and `ci/`. These files provide area maps and shared implementation constraints.
- `Smokescreen/AGENTS.md`, blob `9e486f5d492e90007c2f984186f7e9d1aa0a08c2`, is the separate Smokescreen area contract.

The relevant planning registers are `Docs/Plans/README.md`, `Docs/ROADMAP.md`, `Docs/WATCH.md`, `Docs/BLOCKED.md`, and `Docs/CONTINUE.md`. `Docs/Completed/README.md` and `Docs/Reviewed/README.md` define the later disposition states. The plan contract preserves every Why/Do/Done and forbids treating Completed or Reviewed as an implementation queue.

## Exact current source ownership

### Public planning and implementation entrypoints

- [`.agents/skills/plan/SKILL.md`](https://github.com/Tuneality/Tuneality/blob/6cbc9e04dc83f49c1f663f969edf5568d3c53992/.agents/skills/plan/SKILL.md), sections `plan` and `Review findings`, lines 6–28, grounds ideas or reopened guides. `Ground the idea`, lines 30–45, reads code, data, plans, and gates. `Decide and write`, lines 47–75, writes plans and register outcomes. `Finish`, lines 77–112, invokes `architect` and `roadmap`.
- [`.agents/skills/implement/SKILL.md`](https://github.com/Tuneality/Tuneality/blob/6cbc9e04dc83f49c1f663f969edf5568d3c53992/.agents/skills/implement/SKILL.md), lines 3–6, claims the entire current workflow. `Establish scope and dependency batches`, lines 19–53, performs no-argument roadmap selection, dependency ordering, worker delegation, path ownership, and worktree creation. `Implement and verify`, lines 55–100, owns implementation, branch checks, batch merging, `commit-changes`, plan retention, and cleanup. `Deploy and verify`, lines 102–130, owns staging and production release and full smokescreen proof. `Close honestly`, lines 132–180, owns dispositions, `Completed`, roadmap refresh, and pending proof.
- [`.agents/skills/implement-all/SKILL.md`](https://github.com/Tuneality/Tuneality/blob/6cbc9e04dc83f49c1f663f969edf5568d3c53992/.agents/skills/implement-all/SKILL.md) is the larger public workflow. The key sections are:
  - lines 8–85: unattended operator-input policy, current-document release rule, evidence custody, worker authorization, worktree ownership, and nested supervision;
  - lines 87–127: invocation activation, bootstrap, staging reconciliation, and the exact-candidate release invariant;
  - lines 138–202: `report` and its Production, Staging, and WIP output contract;
  - lines 204–228: `Bug: ...` intake, original-plan repair ownership, roadmap rescore, and continued independent work;
  - lines 230–266: closed scope, approved subset rules, Backlog handling, and no validation substitution;
  - lines 268–393: parent model role, `Docs/CONTINUE.md`, Datadog inspection, supervisor, watcher, and wake ownership;
  - lines 395–645: admission, capacity, worktree and receipt reconciliation, leases, failure handling, outage priority, and bounded release-batch selection;
  - lines 646–799: implementation worker brief, focused checks, branch readiness, handoffs, ownership requests, Unity lease, and wait rules;
  - lines 801–940: release worker integration, staging, production, full smoke, failure attribution, fix-forward, and evidence;
  - lines 943–1000: parent evidence review, `Completed` move, verification pending, register ownership, and final disposition integration;
  - lines 1002–1035: terminal completion and cancellation; lines 1036 onward contain parent-only Unity recovery.

### Supervisor, guard, and hook contracts

- [`.agents/skills/implement-all/Supervisor.cs`](https://github.com/Tuneality/Tuneality/blob/6cbc9e04dc83f49c1f663f969edf5568d3c53992/.agents/skills/implement-all/Supervisor.cs) owns ephemeral fenced supervisor state under `.scratch/implement-all-supervisor`. Its commands include `init`, `rebind`, `heartbeat`, `status`, `tick`, `watch`, `stop`, and `self-test`. The watcher probes or wakes the configured adapter and reports `HEALTHY`, `RUNNING_AUDIT`, `WAKE_REQUESTED`, or `UNSAFE`.
- [`.agents/skills/implement-all/CliAdapter.cs`](https://github.com/Tuneality/Tuneality/blob/6cbc9e04dc83f49c1f663f969edf5568d3c53992/.agents/skills/implement-all/CliAdapter.cs) is the generic process adapter. It accepts fenced `probe` and `wake` requests, rejects stale fences, passes opaque session handles as process arguments, and returns `running`, `idle`, `finished`, or `unavailable`.
- [`.agents/skills/implement-all/references/supervisor.md`](https://github.com/Tuneality/Tuneality/blob/6cbc9e04dc83f49c1f663f969edf5568d3c53992/.agents/skills/implement-all/references/supervisor.md) is the portable adapter contract. It forbids most-recent-session selection, transcript parsing, shell-built handles, and fresh coordinator creation. It defines the parent-only watcher reload and receipt-census reconciliation.
- [`.agents/hooks/ImplementAllGuard.cs`](https://github.com/Tuneality/Tuneality/blob/6cbc9e04dc83f49c1f663f969edf5568d3c53992/.agents/hooks/ImplementAllGuard.cs), lines 20–46, dispatches `bootstrap`, `activate`, `capacity`, `status`, `complete`, `cancel`, `audit`, `own`, receipt reconciliation, scratch GC, and self-test. Lines 49–127 activate or rebind one fenced run. Lines 130–249 handle worker start, worker stop, wait refusal, and parent stop refusal. Lines 252 onward validate trusted capacity and admitted scope. Later guard code owns live-lane audits, receipt custody, disk-floor checks, source-free retirement, and verified ignored-evidence archival.
- [`.agents/hooks/RepoGuard.cs`](https://github.com/Tuneality/Tuneality/blob/6cbc9e04dc83f49c1f663f969edf5568d3c53992/.agents/hooks/RepoGuard.cs) is the edit and shell policy guard. The root contract identifies its Python, deployed-MySQL, publication, and site-host write protections. It is independent of planner scheduling.
- [`ci/agent-hooks.json`](https://github.com/Tuneality/Tuneality/blob/6cbc9e04dc83f49c1f663f969edf5568d3c53992/ci/agent-hooks.json), lines 4–55, binds `RepoGuard` to `PreToolUse`, `ImplementAllGuard` to `UserPromptSubmit`, `SubagentStart`, `SubagentStop`, wait-tool `PreToolUse`, and `Stop`. Lifecycle hooks have a 120-second timeout. The generated `.claude`, `.codex`, and `.grok` files are installer outputs, not independent source contracts.
- [`ci/hooks/pre-commit`](https://github.com/Tuneality/Tuneality/blob/6cbc9e04dc83f49c1f663f969edf5568d3c53992/ci/hooks/pre-commit) regenerates generated outputs and runs the pending-migration guard. It does not run the full solution, Unity compile, or full `TuneTests` suite.
- [`ci/hooks/pre-push`](https://github.com/Tuneality/Tuneality/blob/6cbc9e04dc83f49c1f663f969edf5568d3c53992/ci/hooks/pre-push) enforces the engine submodule push ordering and Git LFS handoff.
- [`ci/install-hooks.sh`](https://github.com/Tuneality/Tuneality/blob/6cbc9e04dc83f49c1f663f969edf5568d3c53992/ci/install-hooks.sh) and the engine installer are the generated-hook path. The source tests require `./ci/install-hooks.sh --check` to report no drift.

### Release and disposition contracts

- [`.agents/skills/commit-changes/SKILL.md`](https://github.com/Tuneality/Tuneality/blob/6cbc9e04dc83f49c1f663f969edf5568d3c53992/.agents/skills/commit-changes/SKILL.md), sections `Preconditions`, `Step 1`, `Step 1b`, `Step 2`, `Step 3`, and `Step 4`, owns the normal commit checks and report. The current implement-all release worker intentionally uses `git commit` in its release worktree and does not invoke this skill there.
- [`.agents/skills/deploy/SKILL.md`](https://github.com/Tuneality/Tuneality/blob/6cbc9e04dc83f49c1f663f969edf5568d3c53992/.agents/skills/deploy/SKILL.md), Steps 0–10, owns commit, game tags, branch movement, Buildkite publish, rollout identity, DataReady, Lighthouse, and deployed verification. It requires full `smokescreen staging` before a production PR and full `smokescreen production` after rollout. Production reaches the branch only through a PR from staging.
- [`.agents/skills/smokescreen/SKILL.md`](https://github.com/Tuneality/Tuneality/blob/6cbc9e04dc83f49c1f663f969edf5568d3c53992/.agents/skills/smokescreen/SKILL.md), Steps 0–7, owns the browser, build-skew, route, signed-in, responsive, piano, lesson, app, logs, and cleanup checks. Its helper sources are `Chrome.cs`, `DevTools.cs`, `App.cs`, `Unity.cs`, and `ServerLogs.cs` in the same directory.
- [`.agents/skills/review/SKILL.md`](https://github.com/Tuneality/Tuneality/blob/6cbc9e04dc83f49c1f663f969edf5568d3c53992/.agents/skills/review/SKILL.md), `Consolidate verification`, `Verify`, and `Disposition`, lines 49–169, remains the separate Completed-to-Reviewed review. It must not be replaced by parent-child review.
- [`.agents/skills/roadmap/SKILL.md`](https://github.com/Tuneality/Tuneality/blob/6cbc9e04dc83f49c1f663f969edf5568d3c53992/.agents/skills/roadmap/SKILL.md), `Audit scope`, `Read and recheck`, `Rank`, and `Write and verify`, lines 13–151, owns scoring, Immediate/Deferred rows, dependency order, and roadmap rewrite.
- [`.agents/skills/architect/SKILL.md`](https://github.com/Tuneality/Tuneality/blob/6cbc9e04dc83f49c1f663f969edf5568d3c53992/.agents/skills/architect/SKILL.md) remains the plan-shape and dependency review invoked by `plan`.

## Responsibility map for the six planner roles

| New role | Chordzy source and responsibility after migration | Input and result receipt | Existing validation and gap |
|---|---|---|---|
| `create-plan` | Map to `.agents/skills/plan/SKILL.md`. Retain grounded planning, original-guide ownership, architect review, and roadmap refresh. Remove command recursion. | Idea or reopened guide, area evidence, current registers → plan path with Why/Do/Done, dependency notes, and architect/roadmap receipts. | Existing plan contract and `architect`/`roadmap` source contracts. No test proves router mapping or once-only execution. |
| `select-plan` | New `.agents/skills/select-plan/SKILL.md`. Move no-argument selection and admission from `implement` lines 19–53 and `implement-all` lines 395–645. Preserve approved subsets, dependency order, repair priority, supersession, capacity, and Backlog exclusion. | User command or live Bug/report, roadmap/register fingerprints, approved scope, dependency heads → durable plan identity, work identity, assigned scope, workspace policy, and waiting/blocked/complete result. | Guard capacity and admitted-scope fixtures exist. Exact no-argument selection once, stable identity, and router receipt tests are missing. |
| `implement-work` | Bind to a narrowed `.agents/skills/implement/SKILL.md`. Keep assigned plan reading, source ownership, focused checks, migration rehearsal, branch evidence, status notes, and `COMMIT PATHS READY`. Remove plan selection, child spawning, staging integration, deployment, smoke release, Completed move, and roadmap retirement. | Durable assigned plan/work identity, exact path set, workspace and authorization → branch SHA, changed paths, branch checks, evidence, unresolved requirements, and deployed-proof recipes where required. | Existing worker brief at `implement-all` lines 646–733 and current `implement` implementation section. No test enforces that an implementation child cannot deploy or retire a plan. |
| `review-work` | New `.agents/skills/review-work/SKILL.md`. Move parent review of branch diff, status notes, and required evidence from `implement-all` lines 943–975. Keep `.agents/skills/review/SKILL.md` separate for Completed-to-Reviewed review. | Child artifact revision, evidence packet, assigned scope, and required checks → revision-bound accept or correction to the same child. | Existing guard self-tests and plan review source exist. No test proves revision binding, same-child corrections, or adversarial evidence review. |
| `finish-work` | New `.agents/skills/finish-work/SKILL.md`. Move worker ending, pushed-branch confirmation, handoff markers, ignored non-cache evidence custody, and safe worktree cleanup from `implement-all` lines 70–75, 448–530, and 690–733. Chordzy finishing pushes and preserves handoff evidence. It does not deploy or release. | Accepted review revision and child handoff → remote branch receipt, custody/hash receipt, resource-closed receipt, and safe cleanup disposition. | Guard self-test covers source, receipt, custody, and cleanup fixtures. Live NAS custody and runtime stop delivery remain pending. |
| `integrate-plan` | New `.agents/skills/integrate-plan/SKILL.md`. Move bounded release-batch selection and release-worker coordination from `implement-all` lines 632–645 and 801–940, plus deployment and closeout from `implement` lines 102–153. It calls existing `deploy` and `smokescreen` contracts. | Accepted child packets, exact ready heads, dependency receipts, current staging identity, and plan Done set → staging/production evidence, every-Done disposition, Completed move or verification-pending state, roadmap/register changes, and integrated disposition receipt. | Deploy and smokescreen contracts are explicit. Existing tests cover helper fixtures, not the complete staging-to-production and final-disposition workflow. |

### Old-to-new responsibility changes

| Current owner | Exact current section | New owner | Required preservation |
|---|---|---|---|
| `implement` | Lines 19–53 | `select-plan` | No-argument selection, dependency order, disjoint path ownership, and admission gates move out of implementation. |
| `implement` | Lines 55–100 | `implement-work` plus `finish-work` | Implementation stays bounded. Branch evidence stays with the child. Handoff and cleanup move to finishing. |
| `implement` | Lines 102–130 | `integrate-plan` | Deploy, rollout, full smoke, and exact candidate identity stay in existing deploy/smokescreen contracts, coordinated by the parent. |
| `implement` | Lines 132–180 | `review-work` and `integrate-plan` | Evidence assessment and status are separated from final plan disposition. `Completed` and roadmap updates remain parent-owned. |
| `implement-all` | Lines 138–228 | Repository profile operations behind router lifecycle | `report` and `Bug: ...` continue during active work. They must produce typed input receipts and route to the original owner. |
| `implement-all` | Lines 268–393 | Router lifecycle plus Chordzy profile | Router owns durable child lifecycle and wake scheduling. Chordzy keeps policy predicates, Datadog receipt semantics, custody, and admission rules. |
| `implement-all` | Lines 395–645 | `select-plan` and `integrate-plan` | Router requests one durable selection result. It must not create a second scheduler or duplicate the current queue. |
| `implement-all` | Lines 646–733 | `implement-work` and `finish-work` | Children never spawn coordinators, push staging or production, deploy, run deployed smoke, retire plans, or edit parent registers. |
| `implement-all` | Lines 801–940 | `integrate-plan` with existing `deploy` and `smokescreen` | Release worker remains the only bounded batch integrator. Failed candidates are attributed, re-formed, and fully rechecked. |
| `implement-all` | Lines 943–1000 | `review-work` then `integrate-plan` | Parent reviews evidence, moves qualifying plans to Completed, refreshes registers, and integrates those docs before terminal completion. |

## Supervisor, guard, and hook ownership changes

The source currently has two related owners. `ImplementAllGuard` owns an ephemeral worker ledger, capacity and lane enforcement, receipt custody, cleanup protection, and the `UserPromptSubmit` activation path. `Supervisor` owns the fenced watcher and adapter wake loop. The migration must leave one recovery owner.

Recommended ownership after parity tests:

1. Router-acp owns durable planner run, plan/work/child identity, attempts, review revision, finish/integration receipts, input acknowledgements, parent resume, and one wake scheduler.
2. Chordzy profile skills own policy. This includes approved scope, dependency and supersession rules, capacity interpretation, exact path ownership, release predicates, Datadog inspection semantics, custody rules, Completed/Reviewed semantics, and roadmap/register rules.
3. Keep `RepoGuard`, the pre-commit guard, the pre-push submodule guard, and the smokescreen helper contracts unchanged. They enforce repository safety and release evidence, not planner scheduling.
4. Keep `ImplementAllGuard` as a Chordzy policy guard during migration. Retain its `audit`, capacity validation, admitted-scope validation, worker ownership, disk-floor, receipt-census, raw-evidence custody, and safe cleanup checks. Expose these as idempotent router lifecycle checks with typed results. Do not let it start a second coordinator.
5. Keep `Supervisor.cs` and `CliAdapter.cs` available for draining existing runs. After the router has a tested wake and recovery path, stop new runs from the `UserPromptSubmit` activation path in `ci/agent-hooks.json`. Route router lifecycle events to the one router wake owner. Do not delete the old supervisor until existing runs have drained and rollback remains possible.
6. During the transition, `SubagentStart`, `SubagentStop`, wait checks, and `Stop` must either be explicitly delegated to the router lifecycle owner or remain guard-owned with no duplicate durable ledger. A hook may enforce a boundary. It must not independently allocate a child, wake a second coordinator, or infer completion.
7. Retain `complete` and `cancel` as guarded transitions. The router must submit the exact run and scope receipt. The guard must reject cancellation or completion with live work, missing custody, dirty source, unpushed source, or unresolved required evidence.
8. Disable old scheduling only after the router passes the existing `ImplementAllOrchestrationTests`, the guard and supervisor self-tests, generated-hook checks, and new end-to-end tests for single activation, same-child correction, restart, and no duplicate coordinator. If those tests fail, keep the legacy activation path and report the exact failed parity row.

## Source-to-test preservation matrix

This matrix records static source and test inventory at the fixed SHA. “Existing” means a committed test or fixture directly names the behavior. “Pending” means the source contract requires a runtime or end-to-end proof that this audit did not execute.

| Requirement | Source owner | Existing test or evidence | Status and missing coverage |
|---|---|---|---|
| Grounded plan and roadmap ownership | `plan/SKILL.md`; `Docs/Plans/README.md`; `roadmap/SKILL.md` | Contract text and plan references exist. | Existing source contract. Missing router `create-plan` mapping and once-only execution test. |
| No-argument selection, approved subsets, dependency order, supersession | `implement/SKILL.md` lines 19–53; `implement-all/SKILL.md` lines 395–645 | `ImplementAllOrchestrationTests` exercises admitted capacity and scope evidence. | Partial. No exact router selection receipt, no-argument once-only assertion, or supersession end-to-end test. |
| Worker model, capacity, and underfilled waits | `implement-all/SKILL.md` lines 268–285 and 403–413; `ImplementAllGuard.cs` capacity commands | `TuneTests/Lib/ImplementAllOrchestrationTests.cs:13–72` tests bound runtime capacity and underfilled status. Guard self-test also checks lane and capacity cases. | Existing fixture coverage. Live runtime census and model selector behavior remain pending. |
| One worker, one worktree, one branch, exact path ownership | `implement-all/SKILL.md` lines 415–446 and 646–733; root `AGENTS.md` worker contract | Guard self-test checks root alignment, receipts, adopted paths, and cleanup. `SeedConcurrencyTests` protects copy/worktree assumptions. | Partial. No router workspace allocation or duplicate child/branch end-to-end test. |
| Focused implementation checks, branch push, status notes, no publication | `implement/SKILL.md` lines 55–100; `implement-all/SKILL.md` lines 646–733 | `GitHookInstallerTests`, `PreCommitPolicyTests`, and CI source contracts cover hook behavior and branch gates. | Existing source and hook tests. Missing enforcement that `implement-work` cannot deploy or retire a plan. |
| Live operator steering and `Bug: ...` routing | `implement-all/SKILL.md` lines 48–49 and 204–228; ticket preservation rows 412–415 | No named Chordzy test in the fixed tree directly drives a live Bug/report through an active run. | Missing and runtime pending. Must test original-owner routing, bounded repair, rescore, and continued unrelated work. |
| On-demand `report` | `implement-all/SKILL.md` lines 138–202 | No named direct report contract test found in the fixed source inventory. | Missing and runtime pending. Must test report while all lanes are occupied and forbid duplicate smoke or premature parent exit. |
| Fenced supervisor activation and wake | `Supervisor.cs`; `CliAdapter.cs`; `references/supervisor.md`; `ImplementAllGuard.cs` lines 49–127 | `ImplementAllOrchestrationTests.cs:137–145` runs supervisor self-test. The guard self-test covers activation and lifecycle fixtures. | Existing fixture coverage. Live adapter wake, process exit, restart, and exact runtime session delivery remain pending. |
| Lifecycle hook manifest and runtime identity | `ci/agent-hooks.json`; generated `.claude/settings.json`, `.codex/hooks.json`, `.grok/hooks/agent-guards.json` | `ImplementAllOrchestrationTests.cs:112–159` checks lifecycle names, 120-second timeouts, committed supervisor tools, and runtime markers. | Existing source-rendered coverage. It does not prove the runtime delivered events. The lifecycle backlog explicitly keeps this pending. |
| Worker stop, handoff, receipt reconciliation, custody, and cleanup | `ImplementAllGuard.cs`; `implement-all/SKILL.md` lines 70–75, 448–530, and 690–733 | Guard self-test assertions cover incomplete handoff, raw custody hash/symlink/cache controls, receipt reconciliation, source-free retirement, and truthful census. `SmokescreenCleanupTests` covers helper cleanup. | Existing fixture coverage. Live NAS availability, real ignored-payload archive, and runtime stop delivery remain pending. |
| Datadog three-stream inspection and unknown handling | `implement-all/SKILL.md` lines 329–376; `smokescreen/ServerLogs.cs` | `TuneTests/Lib/SmokescreenServerLogsTests.cs:27–58` tests unknown coverage and raw hashes plus additional fixture classification. | Existing serializer and fixture coverage. No live provider pagination/coverage or per-release production receipt was executed. |
| Bounded release batch and release worker | `implement-all/SKILL.md` lines 632–645 and 801–940 | Source contract names the release worker and exact-candidate process. | Missing direct orchestration test. Staging Buildkite, release worktree, failure attribution, and fix-forward remain runtime pending. |
| Deploy identity, tags, rollout, DataReady, Lighthouse | `deploy/SKILL.md` Steps 1–10 | `BuildMetaTests` and CI policy tests cover small identity/configuration units. | Runtime pending. No deploy or production action was authorized or executed in this audit. |
| Full web/app smokescreen and cleanup | `smokescreen/SKILL.md` Steps 0–7; `Chrome.cs`, `DevTools.cs`, `App.cs`, `Unity.cs` | `SmokescreenCleanupTests` covers per-run page ownership, failures, signals, discovery, and cleanup. `SmokescreenServerLogsTests` covers log receipts. | Existing helper tests. No exact candidate staging or production full smoke was run. Source cannot establish runtime parity. |
| Parent evidence review and same-child correction | `implement-all/SKILL.md` lines 943–975; `review/SKILL.md` lines 100–169 | No direct test found for revision-bound acceptance or correction delivery to the same child. | Missing. Add review receipt revision tests and process-replacement same-child tests. |
| Completed move, verification pending, roadmap/register refresh | `implement-all/SKILL.md` lines 943–1000; `Docs/Plans/README.md`; `roadmap/SKILL.md`; Completed/Reviewed READMEs | Register contracts and source text exist. | Missing end-to-end test for every-Done assessment, preserved history, link/count reconciliation, and final disposition integration. |
| Production outage priority and failed-candidate repair | `implement-all/SKILL.md` lines 601–640 and 868–884; `deploy/SKILL.md` production interruption rules | No direct workflow test found. | Missing and runtime pending. Add a failure-injection test that drops only the attributed branch and requires a fresh full candidate proof. |
| Terminal completion and cancellation | `implement-all/SKILL.md` lines 1002–1035; guard `complete` and `cancel` commands | Guard self-test covers live-lane and cleanup refusal paths. | Partial. Missing router terminal receipt test, checkpoint-before-cancel, no active worker/release assertion, and integrated disposition proof. |
| Existing `/plan`, `/implement`, and `implement-all` each execute once | `plan/SKILL.md`, `implement/SKILL.md`, `implement-all/SKILL.md`; `ci/agent-hooks.json` | No router consumer test exists in this repository. | Missing. Required by the finalized ticket. Test explicit command, mapped skill, internal invocation marker, attachments, and resume deduplication. |

## Precise migration suggestions

1. Add the six Chordzy role skills named in the finalized ticket. Start with the exact YAML below and keep Chordzy names out of generic router assets:

   ```yaml
   routers:
     planner:
       create-plan:
         skill: plan
       implement-work:
         skill: implement
       # select-plan, review-work, finish-work, and integrate-plan
       # resolve to their exact repository skill names.
   ```

2. Rewrite `.agents/skills/implement/SKILL.md` around one assigned plan/work receipt. Keep its current implementation checks and root safe-change protocol. Remove the first section's roadmap selection and delegation. Remove its deployment section and its Completed/roadmap closeout. State the owning role beside every removed responsibility.

3. Define `select-plan` as the only Chordzy admission owner. It should return a durable plan identity, work identity, exact section and path set, base SHA, dependency receipts, workspace, authorization source, capacity decision, and one of actionable, waiting, blocked, or complete. A missing argument calls it once. An explicit plan still passes through the same admission checks without substitution.

4. Define `review-work` as evidence review of one child revision. It should compare the assigned path set, branch SHA, required checks, status notes, and evidence packet. Corrections should target the same durable child/work identity. It must not run a second independent production review.

5. Define `finish-work` as a child handoff. It should require remote branch identity, clean source, closed resources, custody receipt for ignored non-cache evidence, and a permitted cleanup decision. It must not call `deploy`, run a release smokescreen, move a plan to Completed, or rewrite ROADMAP.

6. Define `integrate-plan` as the only parent integration owner. It should select a finite ready-head batch, spawn one release worker, call the existing deploy and smokescreen skills, collect exact staging and production identities, assess every Done, move only fully proven guides to Completed, preserve verification-pending guides in Plans, refresh roadmap/registers, and land disposition documents through the release worker. It must preserve the production PR rule and the full web/app smoke.

7. Keep `implement-all/SKILL.md` as the public operator contract for live input and reporting. Replace its internal second scheduler with router lifecycle calls. Preserve the `report` and `Bug: ...` behavior as Chordzy profile operations. Router receipts must carry the original plan/work owner, attribution, scope, and acknowledgement.

8. Use a staged hook transition. First add router lifecycle invocation and adapter calls while the legacy guard remains available for existing runs. Then prove single activation, no duplicate coordinator, same-child correction, restart recovery, and terminal fencing. Only then remove new-run supervisor activation from `UserPromptSubmit`. Keep policy audits and cleanup protections until the router owns equivalent durable receipts and the old run population has drained.

9. Preserve release source ownership. `integrate-plan` may coordinate `deploy`, but it should not copy the deploy or smokescreen contracts into a new parallel checklist. The existing `deploy/SKILL.md`, `smokescreen/SKILL.md`, `ServerLogs.cs`, game-tag rules, DataReady, Lighthouse, rollout identity, and cleanup remain the single release evidence path.

10. Add tests before retiring old scheduling ownership. The minimum new tests are: mapped skill resolution and invalid mapping failure, no-argument selection once, explicit command precedence, internal command suppression, implementation child deployment and retirement refusal, same-child correction, durable workspace identity across process replacement, report and Bug input while lanes are busy, full release/disposition flow, failed-candidate reformation, and final disposition integration. Retain every existing `ImplementAllOrchestrationTests`, smoke cleanup, server-log receipt, Git hook, CI policy, and relevant release test.

11. Roll out in this order: land router role resolution and receipts, add Chordzy role skills and compatibility adapters, validate effective YAML, drain or explicitly migrate existing legacy runs, enable one router lifecycle owner, run the full preservation matrix, then remove the old new-run scheduler. Rollback must restore the legacy activation path without allowing both owners to activate. An existing run keeps its recorded profile and ownership until it completes or is explicitly migrated.

12. Record the final migration in the router PR at `docs/migrations/planner-skills.md`. Include the exact source sections above, copyable role YAML, full examples for `/plan`, `/implement <plan>`, `/implement` without arguments, and implement-all with live Bug/report input. Include expected role, workspace, authorization, result receipt, drain, rollback, and runtime-pending statements.

## Audit conclusion

The pinned Chordzy source clearly supports the approved narrowing. It also contains substantial preservation-sensitive behavior outside `implement`, especially in `implement-all`, `ImplementAllGuard`, `Supervisor`, `deploy`, `smokescreen`, and the planning registers. Existing tests cover important guard and helper invariants. They do not establish router consumer parity or live runtime parity. The migration must preserve those pending claims as acceptance work.

Your decision: keep the implementation child bounded and move selection, coordination, release, review, finishing, and integration into the six planner roles with one recovery owner.
