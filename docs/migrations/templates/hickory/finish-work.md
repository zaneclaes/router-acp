# Hickory `finish-work` wrapper

Map `finish-work` to `ship-pr`.

```yaml
routers:
  planner:
    finish-work:
      skill: ship-pr
```

## Steps

1. Require parent acceptance for the exact artifact revision.
2. Pass the Linear ticket, EPIC child, branch, revision, changed paths, checks, and handoff evidence to the existing `ship-pr` skill.
3. Reuse the existing human review, merge, and deployment gates.
4. Preserve the exact handoff labels expected by the Hickory relay and skills.
5. Report `planner_workflow` `finished` with the accepted revision and durable PR and handoff evidence.

The wrapper does not weaken `/ship-pr` gates.
The child cannot merge or deploy because a role prompt says it may do so.
