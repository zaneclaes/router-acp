# Chordzy `select-plan` role

Create `.agents/skills/select-plan/SKILL.md` from this template.
The YAML mapping is `routers.planner.select-plan.skill: select-plan`.

This role is the only admission owner.
It returns one durable selection result.

## Steps

1. Read the current-at-invocation versions of `Docs/ROADMAP.md`, `Docs/Plans/README.md`, `Docs/WATCH.md`, `Docs/BLOCKED.md`, and `Docs/CONTINUE.md`.
2. Read the approved scope from the authentic `/implement` or `implement-all` invocation.
3. Apply the existing eligibility rules.
4. Preserve full-roadmap and explicit-subset invocation.
5. Exclude Backlog work and unapproved spare capacity.
6. Prioritize repairs, finishing started plans, dependency heads, and supersession rules in the existing order.
7. Reconcile capacity, worker census, leases, worktrees, branch heads, exact path ownership, and foreign edits.
8. Return `actionable`, `waiting`, `blocked`, or `complete` with a durable `plan_id`, `work_id`, assigned scope, base SHA, dependency receipts, authorization source, capacity decision, and workspace policy.
9. For a missing argument, perform this selection once and persist the result before dispatch.
10. Admit work with `planner_workflow` `admit` using an idempotency key.

Register contents, capacity, queue state, active runs, and watcher state are runtime inputs.
Do not copy them into this role, the planner YAML, or a durable queue policy snapshot.
The active legacy run keeps only its authorized existing watcher until explicit migration or drain authorization and native parity evidence exist.

The result must identify the original guide owner.
The result must not create a second scheduler or coordinator.

For `/implement` with an explicit plan, apply the same admission checks to that plan.
For `/implement` without an argument, perform one selection and persist its result before dispatch.
Return `actionable`, `waiting`, `blocked`, or `complete` as policy output.
The typed admission payload is a `WorkSpec` with `work_id`, `plan_id`, `scope`, `dependencies`, `required_checks`, `required_integration_evidence`, and optional opaque `external_id`.

Preserve the current contracts at `.agents/skills/implement/SKILL.md` lines 19–53 and `.agents/skills/implement-all/SKILL.md` lines 230–266 and 395–645.

Use `.agents/hooks/ImplementAllGuard.cs` for trusted capacity and admitted-scope checks.
Keep `ImplementAllGuard` as a policy guard during migration.
