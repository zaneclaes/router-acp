# Hickory `review-work` wrapper

Map `review-work` to the existing parent review policy.

## Steps

1. Check the assigned ticket, child identity, pool checkout, branch SHA, exact changed paths, and required checks.
2. Review the artifact independently.
3. Return `planner_workflow` `review` with the exact revision and evidence.
4. Send corrections to the same ticket, child, and `work_id`.
5. Require fresh review when the artifact revision changes.

This review does not grant merge or production deploy authority.
The existing Hickory independent review remains separate where the repository requires it.
