# Deployment invariants

1. `DEPENDENCY_ORDER`: every manifest dependency must be emitted before its
   dependent in `scripts/schedule.py`.
2. `SHARED_PATH_FANOUT`: a change under `packages/shared/` must select every
   manifest service.
3. `SKIPPED_IS_NOT_SUCCESS`: the summary must preserve `skipped`; it must never
   count skipped deployments as successful.
4. `DATABASE_BEFORE_BACKEND`: the production workflow must deploy `database`
   before `backend`.
5. `UNKNOWN_SERVICE_FAILS_CLOSED`: scheduler input containing an unknown
   service must raise an error instead of silently dropping it.
