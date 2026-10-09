# Chordzy planner policy profile

Copy this file to `docs/router-planner-policy.md` in Chordzy.
Set `routers.planner.profile` to that repository-relative path.

This profile supplies Chordzy policy to the generic planner roles.
It does not add Chordzy terms to router core or to bundled generic Markdown assets.

## Ownership

`create-plan` uses the existing `.agents/skills/plan/SKILL.md` contract.
`select-plan` owns approved scope, dependency order, supersession, capacity, path ownership, and workspace admission.
`implement-work` owns one admitted bounded assignment.
`review-work` owns revision-bound parent-child evidence review.
`finish-work` owns child handoff, custody, resource closure, and permitted cleanup.
`integrate-plan` owns release-worker coordination, deploy and smokescreen orchestration, disposition, and roadmap/register refresh.

`implement-all` remains the public operator wrapper for live questions, `report`, `Bug: ...`, approved scope, capacity, custody, and terminal checks.
It calls the router lifecycle owner and does not start a second scheduler.

## Preserved repository contracts

Keep these exact owners and paths:

1. `.agents/skills/plan/SKILL.md`
2. `.agents/skills/implement/SKILL.md`
3. `.agents/skills/implement-all/SKILL.md`
4. `.agents/skills/implement-all/Supervisor.cs`
5. `.agents/skills/implement-all/CliAdapter.cs`
6. `.agents/skills/implement-all/references/supervisor.md`
7. `.agents/hooks/ImplementAllGuard.cs`
8. `.agents/hooks/RepoGuard.cs`
9. `.agents/skills/deploy/SKILL.md`
10. `.agents/skills/smokescreen/SKILL.md` and its helper files
11. `.agents/skills/review/SKILL.md`
12. `.agents/skills/roadmap/SKILL.md`
13. `Docs/Plans/README.md`, `Docs/WATCH.md`, `Docs/BLOCKED.md`, `Docs/CONTINUE.md`, `Docs/Completed/README.md`, and `Docs/Reviewed/README.md`

`ImplementAllGuard`, `Supervisor.cs`, and `CliAdapter.cs` remain available for existing runs during drain.
They must not create a second durable ledger, coordinator, or wake owner.
The active legacy run retains only its authorized existing watcher until explicit migration or drain authorization and native parity evidence exist.

## Authorization and evidence

An authentic `/implement` or `implement-all` request supplies the repository's existing implementation authorization.
The profile cannot manufacture deployment, publication, merge, or release permission from prompt text.

Implementation children return `Artifact` evidence.
Parents return revision-bound `Review` evidence.
Children return accepted-revision `Receipt` evidence.
Parents return release, smoke, disposition, roadmap, register, and final integration evidence.

Unknown, pending, or unverified evidence cannot certify completion.
Missing deployed proof stays `VERIFICATION PENDING` in the owning guide outside `Docs/Completed`.
Completed-to-Reviewed remains the separate `.agents/skills/review/SKILL.md` workflow.
