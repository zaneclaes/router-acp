Read `TASK.md` and implement it completely.

For trace comparability, inspect each of these paths in the listed order using
one separate developer tool call per path; do not combine paths into one shell
command:

1. `TASK.md`
2. `src/orderflow/constants.py`
3. `src/orderflow/models.py`
4. `src/orderflow/money.py`
5. `src/orderflow/tax.py`
6. `src/orderflow/normalize.py`
7. `src/orderflow/discounts.py`
8. `src/orderflow/shipping.py`
9. `src/orderflow/validation.py`
10. `src/orderflow/summary.py`
11. `tests/test_normalize.py`
12. `tests/test_discounts.py`
13. `tests/test_shipping.py`
14. `tests/test_validation.py`
15. `tests/test_summary.py`

After implementing, run each test file separately in the same order, then run
the full suite with:

`PYTHONPATH=src python -m unittest discover -s tests -v`

Do not edit tests, `TASK.md`, or files without TODOs. Finish only when the full
suite passes, and report `BENCHMARK_TASK_OK`.
