Read `TASK.md` and repair the session state machine completely.

For trace comparability, inspect each path below in order with one separate
developer tool call per path. Do not combine reads:

1. `TASK.md`
2. `docs/busy.md`
3. `docs/pending.md`
4. `docs/history.md`
5. `package.json`
6. `src/index.mjs`
7. `src/busy.mjs`
8. `src/pending.mjs`
9. `src/history.mjs`
10. `tests/busy.test.mjs`
11. `tests/pending.test.mjs`
12. `tests/history.test.mjs`

Edit only `src/busy.mjs`, `src/pending.mjs`, and `src/history.mjs`. Run each
test file separately, then run `npm test`. Do not finish until the suite passes.
Report `FABLE_STATE_MACHINE_OK`.
