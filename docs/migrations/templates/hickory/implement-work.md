# Hickory `implement-work` wrapper

Map `implement-work` to the existing bounded Hickory implementation skill.

## Steps

1. Read the assigned ticket, exact scope, branch, pool checkout, required checks, and dependency receipt.
2. Reuse existing Hickory implementation guidance for the ticket.
3. Keep the child inside its ticket and assigned scope.
4. Return the current artifact revision, changed paths, checks, evidence, and unresolved requirements.
5. Report the artifact through `planner_workflow` `artifact`.

The child must not create another ticket, create another PR, merge, deploy, or integrate the EPIC.
The child must not reinterpret the parent authorization.
