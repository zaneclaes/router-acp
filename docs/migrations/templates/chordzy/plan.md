# Chordzy `create-plan` role

Apply this content to `.agents/skills/plan/SKILL.md` after the router role is available.
The YAML mapping is `routers.planner.create-plan.skill: plan`.

This skill owns grounded plan creation and refinement.
It does not recursively invoke `/plan`.

## Steps

1. Read the idea or reopened guide.
2. Read the relevant source, data, plan registers, and area contracts.
3. Preserve the original guide owner when repairing an existing guide.
4. Write Why, Do, and Done with exact paths and required evidence.
5. Record dependencies, supersession, Value, Viability, Immediate or Deferred placement, and legitimate WATCH or BLOCKED facts.
6. Run the existing `architect` review from `.agents/skills/architect/SKILL.md`.
7. Run the existing `roadmap` review from `.agents/skills/roadmap/SKILL.md`.
8. Return the plan path, plan identity, requirements, dependency heads, and architect and roadmap receipts to the parent.

The parent calls `planner_workflow` `admit` only after `select-plan` confirms approved scope.
This skill does not select a child, allocate a workspace, delegate implementation, release a build, move a guide to Completed, or rewrite the roadmap after integration.

The role executes once for the authentic `/plan` input.
An internal role marker must not cause `/plan` to be parsed again.

Retain the source contracts at `.agents/skills/plan/SKILL.md` sections `plan`, `Review findings`, `Ground the idea`, `Decide and write`, and `Finish`.
