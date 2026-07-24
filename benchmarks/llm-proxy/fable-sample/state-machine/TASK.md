# Session-state repair

Repair the three incomplete state modules without changing their public API.

## Busy state

- `beginLocalTurn` marks only the selected session busy.
- `endLocalTurn` clears only the selected session.
- `reconcileSessions` accepts server snapshots, but an id present in
  `optimisticBusyIds` remains busy until the caller removes that id.
- Inputs must not be mutated.

## Pending first send

- `queuePendingSend` stores text and attachments and immediately appends one
  optimistic user message. Re-queueing the same temporary session replaces the
  queued payload without adding a duplicate optimistic message.
- `promotePendingSession` changes the temporary id to the real id everywhere:
  session, active id, messages, and pending send.
- `drainPendingSend` dispatches only when the real session exists, is not
  pending, and is not busy. It returns the request once and marks it dispatched;
  every later drain returns `request: null`.
- Inputs and attachment objects must not be mutated.

## Partial history

- `mergeHistory` deduplicates by message id.
- A newer version of an id replaces the older version, including partial
  assistant content.
- An older incoming version never regresses newer content.
- New messages are ordered by `(createdAt, id)`.
- Inputs must not be mutated.

Only edit files under `src/`.
