#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.11"
# dependencies = [
#   "PyYAML>=6.0,<7",
# ]
# ///
"""Update router-acp's model catalog and quality/cost tables.

One command that discovers what each agent currently exposes, proposes anchored
scores for anything unscored, keeps the first-match pattern table correct, and
validates that routing ORDER still matches policy — then shows you the diff.

    scripts/update_models.py                   # dry run: report + diffs, no writes
    scripts/update_models.py --validate-only    # just the invariants (CI-friendly)
    scripts/update_models.py --apply            # write the proposed changes

Runbook: docs/model-updater.md — the implementation lives in
`scripts/update_models/` (this file is only the entry point, because a module
sitting next to a package of the same name can be run but never imported).
"""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from update_models.cli import main  # noqa: E402

if __name__ == "__main__":
    raise SystemExit(main())
