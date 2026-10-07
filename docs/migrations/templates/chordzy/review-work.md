# Chordzy `review-work` role

Create `.agents/skills/review-work/SKILL.md` from this template.
The YAML mapping is `routers.planner.review-work.skill: review-work`.

This role is the parent-child adversarial review.
It is separate from the later Completed-to-Reviewed review.

## Steps

1. Read the assigned `work_id`, exact path set, child branch, artifact revision, required checks, status notes, and evidence packet.
2. Confirm the workspace HEAD equals the artifact revision.
3. Confirm the diff stays inside the assigned scope.
4. Check every required implementation, migration, hook, CI, and repository evidence item.
5. Reject unknown, pending, or unverified evidence.
6. Return `planner_workflow` `review` with the exact revision, independent evidence, and either `accepted: true` or bounded corrections.
7. Send corrections to the same child and same `work_id`.
8. Require fresh review when the artifact revision changes.

Acceptance does not certify production deployment or independent Completed-to-Reviewed review.
The separate `.agents/skills/review/SKILL.md` contract remains the owner of that later review.

Report the typed `review` action with `work_id`, the exact `revision`, `accepted`, independent `evidence`, optional bounded `corrections`, and an idempotency `key`.
An accepted review must have no unresolved artifact requirements.
Corrections retain the same `work_id` and child identity.

Preserve parent evidence review from `.agents/skills/implement-all/SKILL.md` lines 943–975.
