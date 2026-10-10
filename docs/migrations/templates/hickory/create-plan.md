# Hickory `create-plan` wrapper

Map `create-plan` to the existing Hickory planning skill.
The wrapper keeps Linear ticket and EPIC binding in the existing skill.

## Steps

1. Read the authentic ticket and EPIC context.
2. Preserve the existing ticket identity and parent relationship.
3. Reuse the existing planning skill for requirements, scope, and dependencies.
4. Return the ticket identity, plan identity, requirements, dependency receipts, and authorization source.
5. Mark the prompt as the internal `create-plan` role before the router delivers it.

Do not copy the ticket system contract into router core.
Do not recursively invoke `/plan`.
