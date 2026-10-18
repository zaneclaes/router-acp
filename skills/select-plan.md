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

Declare literal owned_paths. Use ready_dependencies (work ID to accepted SHA) when
reviewed source suffices; dependencies still requires integration. Carry and qualify
those sources in the candidate. Reconcile suspended recovery before retrying; a missing
workspace is not a model failure. Use effective capacity. Do not rebase to occupy a slot.

Before resuming adopted allocated work, use parent `planner_workflow reconcile` with its
complete current WorkSpec, carried `source_revision` and custody evidence. The previous attempt
must have ended. Preserve transferred source; an old conversation brief is not the assignment.
Automatic admissions wait for reconciliation. Release/review work can proceed meanwhile.
