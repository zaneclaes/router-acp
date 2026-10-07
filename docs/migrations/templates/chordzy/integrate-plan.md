# Chordzy `integrate-plan` role

Create `.agents/skills/integrate-plan/SKILL.md` from this template.
The YAML mapping is `routers.planner.integrate-plan.skill: integrate-plan`.

This role is parent-owned.
It coordinates release workers and final plan disposition.

## Steps

1. Read finished child packets, accepted revisions, dependency receipts, current staging identity, and every active Done item.
2. Select one finite ready-head batch.
3. Spawn one release worker in the existing release worktree contract.
4. Reconcile shared staging changes and preserve foreign dirt.
5. Use the existing `.agents/skills/deploy/SKILL.md` contract for commit, game tags, branch movement, Buildkite, rollout identity, DataReady, Lighthouse, and deployed verification.
6. Run the existing `.agents/skills/smokescreen/SKILL.md` contract for full staging smoke before production and full production smoke after rollout.
7. Preserve web and app smoke, served identity, game tags, client provenance, DataReady, Lighthouse, log, and cleanup evidence.
8. Attribute the complete failure set. Drop only the affected branch, re-form the candidate, and fully recheck it.
9. Hold feature releases for a production outage and release the bounded hotfix alone when the existing policy requires it.
10. Assess every active Done after verified release evidence.
11. Move only fully proven guides to `Docs/Completed` with `git mv`, preserved history, links, counts, and required evidence.
12. Keep missing proof as `VERIFICATION PENDING` in the owning guide outside Completed.
13. Refresh `Docs/ROADMAP.md`, `Docs/WATCH.md`, `Docs/BLOCKED.md`, and registers under `.agents/skills/roadmap/SKILL.md`.
14. Land final planning dispositions through the release worker.
15. Report `planner_workflow` `integrate` with exact staging, production, smoke, disposition, roadmap, and final integration evidence.

Do not copy the deploy or smokescreen contracts into a second checklist.
Call the existing skills and preserve their ownership.

The typed `integrate` action requires the accepted finished `revision`, a nonempty evidence map, and every `required_integration_evidence` key.
The child attempt must have ended before integration.
Use `disposition` only for a non-active `blocked` or `watching` work item with a concrete reason.
Use `complete` only after every admitted work item is integrated and every input and wake is acknowledged.

Preserve release coordination from `.agents/skills/implement-all/SKILL.md` lines 632–645 and 801–940.
Preserve disposition from lines 943–1000 and terminal checks from lines 1002–1035.
