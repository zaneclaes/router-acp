# Busy reconciliation

The relay is eventually consistent. A local send therefore owns a short
optimistic-busy lease which must survive stale session snapshots.
