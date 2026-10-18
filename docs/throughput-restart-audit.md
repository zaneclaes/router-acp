# Implementation throughput: durable repair and restart audit

## Current reconciliation — 2026-10-18

The records below are historical checkpoints, not the current restart verdict. The
new Kory run is active. Its monitor now owns infrastructure integration into
[router-acp PR #103](https://github.com/zaneclaes/router-acp/pull/103) and
[goose PR #2](https://github.com/hickoryai/goose/pull/2). Repairs must be reconciled
with the latest PR and base commits, tested and built from that combined source,
pushed to those PRs, and installed atomically with recorded source and binary identity.
A scratch worktree or installed binary alone is not durable delivery.

The canonical router checkout contains the full original repair set and the live
monitor corrections: independent capacity processing beside recovery fences,
bounded worker CI handoffs, and revision-checked external monitor input through the
existing parent queue. Goose's canonical checkout has been brought forward to its
latest PR source and reconciled with its base, preserving the intervening five
commits. Current validation, remote heads and installed hashes belong to the dated
Chordzy monitor receipt; the earlier counts and hashes below do not identify these
new candidates. A running process does not reload a replaced executable.

Chordzy's parent retains its WIP, staging, production and smokescreen obligations.
The monitor skill is being qualified through its own WIP branch. No product release,
plan completion or restart safety claim follows from this infrastructure reconciliation.

## Post-shutdown reconciliation — 2026-10-17 (historical)

Activation complete: the qualified binary is installed, primary Chordzy source and
skills are reconciled, 62 focused Chordzy tests pass, and the activated primary host
self-test and canonical hook installer check pass. Primary staging now equals remote
5899df994; the former five commits and unpublished engine pointer are preserved in
`preserve/throughput-reconcile-20261017`. Strict primary alignment is enforced by
the audit and staging-checkpoint host. Pending working files follow the normal WIP
release, then byte-identical released changes are cleared. Restart is ready; deployed
proof remains owed as recorded in CONTINUE.

This section supersedes the preparation-time status below. Kory has stopped. Its
final smoke source through 9dc9d5c3e, executable-agent classifier, CI deadline
attribution, suite cancellation e50809d9b, lease repair and primary submodule-sync
changes are reconciled in Chordzy's `.scratch/planner-throughput-policy`.
Combined testing found and repaired the missing web Unity prerequisite suppression,
macOS retired-path aliases, and a C# local-function collision in the release guard.
Restore failures now permit explicit `restore --retry` after prerequisites change;
submodule sync refuses failed Git queries and preserves dirty/unpushed source.

All eight rows below now have durable source implementations. Actual product
delivery remains governed by WIP, staging, production and smokescreen; no new
Completed claim is made by this reconciliation. The stale round-m candidate must
be re-formed with the combined repair before release. The Chordzy evidence file
`Docs/Plans/data/implementation/2026-10-17-throughput-reconciliation.md` records
final activation and test results; `Docs/CONTINUE.md` owns the release handoff.

The full router suite now passes **944 tests**, with one existing ignored benchmark,
including 74 planner and 240 protocol tests. Formatting, clippy with warnings denied
and optimized release build pass. The new explicit-session adoption regression
allows a paused run only after its owner stops and every attempt ends; ordinary
loading does not resume it. The release executable SHA-256 is
`aaa043344c3a7c39264492d18b8a8698c1c5fb44188843810da4f7c54368dd74`.

Stopped router and Goose SQLite databases are backed up under
`.scratch/handoff/reconcile-state/`. Adoption on a private copy preserved **140
works, 3 holds, 11 wakes and 4 operator inputs**, with zero conflicts. The killed
run has two unfinished attempts for dead-owner recovery; no live state or receipts
were edited. Actual configuration validation passes and configured capacity is 50.

The primary router source was fast-forwarded to existing 9f4d43e and reconciled with
this candidate, preserving its foreign formatting patch. Chordzy primary commit
1cd6ce6c6 recorded shutdown documents and the existing submodule-sync source during
this work; those changes remain preserved. Its unpublished engine pointer means
the primary HEAD must not be pushed. No new commits, pushes or product deployments
were performed here. No Goose source changes are needed.

## Preparation record (historical)

Operator request: 2026-10-17. Keep the current Kory run alive, give it immediate
steering, implement durable repairs separately, then reconcile all repairs with
Kory's final changes after shutdown before recommending restart. No live executable,
router database or runtime configuration was replaced during preparation.

## Ownership and candidates

- Router candidate: `.scratch/planner-throughput`, branch `wip/planner-throughput`,
  based on `9f4d43e2930a7ca5385b13858cd948b5d856f871`. The original router checkout and
  its foreign edits are preserved. Nothing is committed or installed yet.
- Chordzy candidate: `Chordzy/.scratch/planner-throughput-policy`, branch
  `wip/planner-throughput-policy`, based on
  `5899df994072772f2bef25dc500513f24702c65c`. Own edits stay in that worktree.
- Kory retains its active product, smoke, classifier and lease repairs and release
  train. Its current shared RouterHost changes must be reconciled with this candidate,
  never replaced wholesale. Status uses existing planner status and wake surfaces.
  No Goose source was changed. Its current config handler does not forward the new
  custom capacity option: live increases are tested through direct ACP only; Kory
  gets its configured limit on restart.

## Original fix list

| Repair | Durable implementation | Verification and remaining reconciliation |
|---|---|---|
| 1. Release-tool qualification and bounded smoke failures | Kory's existing SMOKESCREEN-DETERMINISTIC R-4693, RELEASE-LANE-LIVENESS/lease repair and executable `.agents/skills` classifier repair; shared throughput contract preserves prerequisite BLOCKED/overall FAIL and independent probes | After shutdown verify actual source, regression tests, green branch builds, both environment smokes and plan notes. Immediate prompt alone does not close this item. I retain responsibility for missing durable fixes. |
| 2. Stop futile recovery | Router checks workspace/dependencies before attempts, suspends three unchanged failures with one wake, retains accepted handoffs and exact packet idempotence | Missing/restore, unchanged retry, accepted replay, stale attempt and adoption tests. |
| 3. Durable, responsive releases | Parent-owned frozen checkpoints and ordered gates; next staging overlaps production; failed production can abandon the unmerged next candidate before isolated repair. Existing audit batches CI reads, coalesces changes and parks exact fresh pending waits | Release ordering, overlapping failure/recovery, no inferred completion, CI freshness, unknown observer, host acknowledgement and reader matching tests. No replacement scheduler. |
| 4. Bound batches and eliminate gratuitous requalification | Selection cannot acquire late or duplicate heads; selected source dependencies must be carried or in the preceding merged release; RouterHost says bounded selection; select-plan no longer forces every queued head to rebase | Candidate ancestry checked by host; reconcile Kory's lease fixes and any parallel policy edits. |
| 5. Separate worker and deployed proof; avoid receipt-only model turns | Typed integration_requirements stay mandatory at integration. Parent finalize requires independent review, ended attempt, accepted resource closure, stopped clean lease and exact actual remote branch custody | Router refuses old hosts' bare exit 0; host rejects dirty/unpushed/missing-resource finalization. Ordinary finish-work remains available. |
| 6. Get guides actually into Completed | Shared roles prioritize disposition and normal docs release immediately. Disposed requires integrated works, landed staging commit, exact committed disposition table and moved Completed files | Host tests positive landed disposition and rejects unlanded/modified evidence. Actual Kory guide outcomes need post-shutdown reconciliation. |
| 7. Useful parallelism and honest capacity | Literal path ownership; exact reviewed source dependencies before deployment; shared proof reuse only per environment/candidate; actual effective capacity in status | Ownership/dependency tests and direct ACP increase test; Kory uses restart configuration because its current config handler does not forward the custom option. No scope or lease expansion. |
| 8. Useful flow visibility and smaller context | Targeted status, measured ready age, release stage, recovery/proof keys, actual staging Completed inventory and new paths since first audit; wake prioritization/coalescing | Work integration counts explicitly differ from completed guides. Unknown history stays unknown; full conversation history remains. |

An additional restart fix refreshes the policy snapshot on an explicit implement
command after a completed drain with no active attempts. Ordinary loading retains
the original snapshot; old policies and all work/review/release state remain durable.

## Validation record

The full router suite passed 943 tests with one existing ignored benchmark. Tests
run with API-key variables removed from the test process and two test threads;
unbounded parallel execution exposed existing fixture contention, and inherited
API-key variables violate the unsigned-account fixture's assumption. No credential
behavior was changed. Planner tests additionally cover adoption after a partial save,
fresh CI parking/stale recovery, policy refresh and overlapping release failure.

Chordzy `RouterHost.cs self-test` and `PipelineSettings.cs planner-builds-self-test`
pass in isolated fixture repositories with local bare remotes. No product deployment
or production mutation was performed. Both checks are now wired into the existing
`StagingCabinCiTests` suite; the two filtered .NET regression tests passed. These
checks do not replace the Chordzy WIP,
staging, production and smokescreen gates. `dotnet restore Chordzy.sln` succeeded.

`cargo fmt --check`, both repositories' `git diff --check`, and
`cargo clippy --all-targets -j 2 -- -D warnings` passed. The full suite was rerun after
the final source changes. The release binary is built separately and is not installed;
its build result and hash belong in the preserved handoff manifest.

## Last live reconciliation snapshot

Read-only inspection while Kory remained running found classifier source and tests
in its `80a501959` worker commit; R-4693 source in `60b5a1aa3` with subsequent test
and evidence commits through `db006ec56`; and uncommitted lease-host/RepoGuard changes
plus `RouterHostRetiredLeaseTests.cs` in its lease worker. These are ownership pointers,
not a qualification or production verdict. The suite-deadline repair is a separate
active Kory worker. Preserve and re-audit all four tracks after shutdown.

CONTINUE still contained the old instruction to carry every STAGING_IDLE head beside
its newer bounded-batch guidance. The immediate steering asks Kory to reconcile that
contradiction in its own documents. This candidate owns the durable shared role contract.

## Required audit after the operator shuts Kory down

1. Verify actual Kory/router processes and children have stopped; reconcile shutdown
   receipts, dirty source, remote heads, leases and CONTINUE. Preserve all unfinished work.
2. Compare Kory's final changes with every row above. Inspect source/tests, not just
   plan prose. Retain its fixes; implement and test any missing durable item here.
3. Reconcile the RouterHost, skills, CI reader and classifier changes by path. Use the
   repository's commit/release authority and normal pipeline; no hook or smoke bypass.
   Regenerate/check hook manifests with the installer only if their canonical sources changed.
4. Back up stopped session state, inspect effective dispatch paths and reconcile the
   router source against any concurrent changes. Install the qualified executable
   atomically; never truncate a running executable. Record source/binary hashes.
5. Prepare the restart instruction for the same drained session: explicitly invoke
   implement so revised skills load. Qualify its policy-refresh and adoption path offline
   before restart; after the operator starts it, verify policy identity, actual limit,
   preserved accepted work, recovery, host acknowledgement and observation/release state.
6. Recommend restart only when the pre-restart checks have evidence. Any unresolved repair or
   failed check stays explicit. Prompt compliance, tests in an uninstalled worktree,
   occupancy and worker completion are not proof that the full live fix list is active.
