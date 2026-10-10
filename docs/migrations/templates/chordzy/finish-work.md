# Chordzy `finish-work` role

Create `.agents/skills/finish-work/SKILL.md` from this template.
The YAML mapping is `routers.planner.finish-work.skill: finish-work`.

This role turns an accepted child revision into a durable handoff.
It does not release or retire the plan.

## Steps

1. Confirm parent acceptance for the exact artifact revision.
2. Confirm the remote branch identity and pushed branch SHA.
3. Confirm the source is clean and all required resources and leases are closed.
4. Archive ignored non-cache evidence when policy requires it.
5. Record custody hashes and the archive receipt.
6. Keep the only review, recovery, or integration evidence available.
7. Request cleanup only when no active integration needs the worktree or branch.
8. Report `planner_workflow` `finished` with the accepted revision and durable handoff and custody evidence.

The child must not call `deploy`, run a release smokescreen, move a guide to Completed, rewrite ROADMAP, or integrate the plan.

The typed `finished` action requires the same `work_id` and active `attempt_id` as the accepted artifact.
Its `receipt` contains the accepted `revision` and a nonempty evidence map.
Evidence must cover the remote branch, clean source, closed resources, custody for ignored non-cache evidence, and the permitted cleanup decision.
The child hands back its turn before parent integration.

Preserve cleanup and handoff checks from `.agents/skills/implement-all/SKILL.md` lines 70–75, 448–530, and 690–733.
Keep `ImplementAllGuard` custody, source-free retirement, disk-floor, and receipt-census checks active during migration.
