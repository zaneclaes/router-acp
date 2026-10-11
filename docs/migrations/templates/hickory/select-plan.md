# Hickory `select-plan` wrapper

Map `select-plan` to the existing Hickory selection policy.

## Steps

1. Read the EPIC children and their Linear dependencies.
2. Preserve one ticket per implementation child.
3. Preserve the configured worker capacity and pool checkout availability.
4. Return one stable `plan_id`, `work_id`, ticket identity, bounded scope, dependency receipts, and authorization receipt.
5. Admit work once with `planner_workflow` `admit`.

The parent owns selection.
The parent does not become a child when `/implement` starts execution.
