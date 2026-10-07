# Chordzy public `implement-all` wrapper and policy

Apply this content to `.agents/skills/implement-all/SKILL.md` after router lifecycle support is available.
This remains the public Chordzy operator contract.

`implement-all` remains the public operator entrypoint.
The router owns durable lifecycle, child identity, parent review, wake delivery, and recovery.
This wrapper owns Chordzy policy and operator input.

## Steps

1. Accept a valid roadmap request, explicit subset, `report`, side question, or `Bug: ...` input.
2. Preserve the original session, plan owner, work owner, attribution, scope, and acknowledgement.
3. Call `create-plan` for a new or reopened guide.
4. Call `select-plan` once for no-argument selection and for every explicit scope.
5. Call `implement-work` only for an admitted bounded assignment.
6. Route artifacts to parent `review-work`.
7. Return corrections to the same child and work identity.
8. Call `finish-work` after revision-bound acceptance.
9. Call `integrate-plan` for release workers, staging, production, smokescreen, disposition, and roadmap refresh.
10. Reconcile every input and wake before terminal completion.

The wrapper does not directly invoke a second scheduler.
It records operator messages as router `InputReceipt` values and uses the one router lifecycle owner.
The router supplies `planner_wake` through the existing lifecycle hook and outbox.

### Live input policy

`Bug: ...` admits the smallest investigation or correction for the actual owning guide.
It keeps the original plan owner and does not compete with the owner's writes.
It accepts human reproduction and screenshots as input evidence.
It rescales Value and Viability and keeps one roadmap row per guide.
An unowned bug may create one focused guide only after normal admission.

`report` reconciles ownership and accepted evidence.
It reports Production, Staging, and WIP with build numbers, deployment times, smoke verdicts, follow-ups, and honest unknowns.
It does not pause active work or start duplicate verification.

Side questions receive a response while work continues.
Busy lanes do not end the run.

### Preserved policy

Keep the exact behavior from `.agents/skills/implement-all/SKILL.md` sections at lines 8–85, 87–127, 138–202, 204–228, 230–266, 268–393, 395–645, 646–799, 801–940, 943–1000, and 1002–1035.
Keep approved queue pinning, dependency order, supersession, capacity, leases, path ownership, shared staging, release workers, outage priority, resource waits, custody, disk-floor, receipt census, and exact session fencing.
Keep three-stream Datadog inspection, cursor overlap and retry, unknown handling, and per-release inspection where the final baseline retains them.
Keep `Docs/CONTINUE.md` checkpoints and checkpoint-before-cancel.
Keep separate Completed-to-Reviewed processing from `.agents/skills/review/SKILL.md`.

### One recovery owner

During drain, `ImplementAllGuard`, `Supervisor.cs`, and `CliAdapter.cs` remain available for existing runs.
They must not start a second coordinator or duplicate the router ledger.

After parity tests pass, the router owns new-run activation and wake scheduling.
The guard remains the policy enforcement boundary.
Disable the old `UserPromptSubmit` scheduler only after single activation, restart recovery, same-child correction, terminal fencing, generated-hook checks, and full preservation tests pass.

## Required receipts

The wrapper must retain the router receipts for `InputReceipt`, `WorkSpec`, `Artifact`, `Review`, `Receipt`, `Wake`, and final `queue_evidence`.
It must also retain Chordzy release, smoke, custody, Completed, roadmap, register, and final disposition evidence.

The public `report` and `Bug: ...` messages remain Chordzy policy operations.
They must identify the original owner and acknowledge the input before completion.
They must not allocate a second child for a correction.

Source and fixture inventory are evidence only.
Runtime parity remains pending until the native Goose bug/report, busy-lane report, crash/restart, full release, failure re-form, and final disposition tests pass.
