# Chordzy planner workspace adapter

Implement the command named by `routers.planner.workspace.command` under Chordzy repository policy.
The migration YAML uses `chordzy-planner-workspace` as the command name.
The adapter runs with the Chordzy session repository as its current directory.

The router writes one JSON object to standard input.

```json
{
  "event": "planner_allocate",
  "session_id": "router-session-id",
  "work": {
    "work_id": "work-001",
    "plan_id": "plan-001",
    "scope": "the assigned plan section",
    "dependencies": [],
    "required_checks": ["branch-check"],
    "required_integration_evidence": ["staging-smoke"],
    "external_id": "Docs/Plans/example.md"
  },
  "child_id": "child-opaque-id"
}
```

The adapter returns JSON that deserializes as the router `Workspace` value.

```json
{
  "path": "/chordzy/worktrees/child-opaque-id",
  "lease": "lease-opaque-id",
  "environment": {},
  "mcp_servers": []
}
```

`environment` and `mcp_servers` are optional and default to empty values.
The adapter must allocate a separate Chordzy worktree, branch, and durable lease.
It must reject the parent checkout, a shared checkout, a missing lease, and a lease owned by another live child.
It must preserve the existing WIP worktree, branch, NAS, custody, and cleanup rules.
It must make retries for the same `work_id` and child identity idempotent.

The router independently requires an accessible Git root whose canonical path differs from the parent repository.
The adapter remains responsible for pool ownership, branch naming, Chordzy worktree policy, and lease reconciliation.
