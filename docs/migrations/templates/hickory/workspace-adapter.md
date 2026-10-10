# Hickory planner workspace adapter

The Hickory workspace adapter uses pool checkouts only.
It must not allocate a shared checkout.
It must not return the parent checkout.
It must return a durable lease that the existing workstation pool can reconcile.

The adapter receives the router's `planner_allocate` JSON on standard input.
It returns JSON with `path` and `lease` on standard output.

```json
{
  "event": "planner_allocate",
  "session_id": "router-session-id",
  "work": {
    "work_id": "HAI-1234",
    "plan_id": "epic-opaque-id",
    "scope": "the complete ticket assignment",
    "dependencies": [],
    "required_checks": ["ticket-checks", "frontend-checks"],
    "required_integration_evidence": ["ship-pr-receipt"],
    "external_id": "HAI-1234"
  },
  "child_id": "child-opaque-id"
}
```

```json
{
  "path": "/opt/dev/hickory-ai7",
  "lease": "pool-lease-opaque-id"
}
```

The adapter must reject a missing pool lease, a path equal to the parent, a non-checkout path, and a checkout already owned by another live child.
The adapter must make allocation idempotent for the same durable `work_id` and child identity.
The router still records `Workspace.path` and `Workspace.lease` and fences integration on that receipt.
