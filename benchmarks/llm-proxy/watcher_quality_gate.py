#!/usr/bin/env python3
"""Exact semantic gate for the CI watcher workload."""

from __future__ import annotations

import json
import sys
from pathlib import Path


def main() -> int:
    repo = Path(sys.argv[1]).resolve()
    snapshots = [
        json.loads(path.read_text())
        for path in sorted((repo / "checks").glob("*.json"))
    ]
    expected = {
        "total": 20,
        "passed": 16,
        "pending": 4,
        "pending_services": ["svc-04", "svc-09", "svc-14", "svc-19"],
        "max_duration_ms": 2300,
    }
    assert len(snapshots) == expected["total"]
    assert sum(item["status"] == "passed" for item in snapshots) == expected["passed"]
    assert sum(item["status"] == "pending" for item in snapshots) == expected["pending"]
    actual = json.loads((repo / "result.json").read_text())
    assert actual == expected, (actual, expected)
    print("WATCHER_QUALITY_GATE_OK: exact 20-snapshot aggregate")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
