# Hickory `integrate-plan` wrapper

Map `integrate-plan` to the existing Hickory parent integration policy.

## Steps

1. Read accepted and finished child receipts.
2. Verify each shipped ticket and dependency outcome.
3. Reconcile the EPIC parent view through the existing `kory-code/relay/sessions.mjs` path.
4. Preserve one PR per ticket and no implementation PR for the EPIC parent.
5. Record `planner_workflow` `integrate` with revision-bound shipped evidence and dependency evidence.
6. Select the next eligible child or return a truthful blocked, waiting, or complete result.

The parent does not create an aggregate implementation PR.
Cross-repository work requires a separately authorized implementation unit.
