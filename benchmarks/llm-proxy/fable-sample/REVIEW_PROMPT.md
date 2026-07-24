Perform the independent read-only review in `REVIEW_TASK.md`.

For trace comparability, inspect every path below in order with a separate
developer tool call. Do not combine reads:

1. `REVIEW_TASK.md`
2. `docs/invariants.md`
3. `deployables.json`
4. `scripts/schedule.py`
5. `scripts/summary.py`
6. `workflows/deploy-production.yml`
7. `workflows/deploy-backend.yml`
8. `workflows/deploy-database.yml`
9. `workflows/deploy-frontend.yml`
10. `workflows/deploy-alerts.yml`
11. `tests/test_schedule.py`
12. `tests/test_summary.py`

Do not modify existing files and do not fix the defects. Write only
`review.json`, validate it with `python -m json.tool review.json`, and report
`FABLE_REVIEW_OK`.
