# Chordzy `implement-work` role

Apply this content to `.agents/skills/implement/SKILL.md`.
The YAML mapping is `routers.planner.implement-work.skill: implement`.

This role owns one assigned plan and one bounded work identity.
It never selects another plan.

## Steps

1. Read the assigned plan, exact section, exact path set, base SHA, dependency receipts, authorization receipt, and workspace lease.
2. Preserve foreign edits and disjoint path ownership.
3. Make only the assigned implementation changes.
4. Run focused checks, migration rehearsal, normal commit hooks, branch checks, and every assigned acceptance check.
5. Record branch SHA, changed paths, check results, evidence paths, status notes, and unresolved requirements.
6. Push the WIP branch when the existing worker contract requires it.
7. Report the artifact through `planner_workflow` `artifact` with the current revision.
8. Stop after the artifact report or after a bounded `worker_handoff`.

The child must not select work, spawn a coordinator, merge WIP, push staging or production, deploy, run the release smokescreen, move a guide to Completed, rewrite ROADMAP, or land final disposition documents.

The artifact payload must contain `revision`, `changed_paths`, `checks`, `evidence`, and `unresolved`.
Every assigned `required_check` must have value `pass` in `checks`.
Every evidence entry must be a usable reference.
The router rejects absolute or parent-directory paths and rejects a dirty or stale workspace revision.

The only durable worker mutations are `planner_workflow` actions `artifact` and `finished`.
They require the assigned `work_id`, active `attempt_id`, and an idempotency `key`.

If the parent sends corrections, continue the same `work_id` and use the new attempt context.
Do not create a replacement plan or PR for a correction.

Preserve the implementation checks from `.agents/skills/implement/SKILL.md` lines 55–100 and the worker brief from `.agents/skills/implement-all/SKILL.md` lines 646–733.
