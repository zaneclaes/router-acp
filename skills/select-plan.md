# Select authorized work

Resolve the supplied plan or configured roadmap. With no explicit plan, resolve
the repository's eligible queue once. Validate supplied scope through the same
admission checks. Do not substitute another plan silently.

Record a stable plan_id and work_id, assigned scope, dependencies, declared
required checks, and required integration evidence with planner_workflow admit.
Dispatch each assignment with delegate_task work_id. Its workspace must be
isolated. Respect repository allocation and capacity limits. Do not create a
second coordinator inside a worker.

Return actionable work, a concrete waiting reason, a scoped blocker, or a
verified complete result. Missing or ambiguous plans require resolution.
Continue independent authorized scope while dependent work waits.
