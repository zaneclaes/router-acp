#!/usr/bin/env python3
import json
import sys
from pathlib import Path


root = Path(sys.argv[1])
result = json.loads((root / "review.json").read_text())
assert result["verdict"] == "CHANGES_REQUESTED"
findings = result["findings"]
assert [item["code"] for item in findings] == [
    "DATABASE_BEFORE_BACKEND",
    "DEPENDENCY_ORDER",
    "SHARED_PATH_FANOUT",
    "SKIPPED_IS_NOT_SUCCESS",
    "UNKNOWN_SERVICE_FAILS_CLOSED",
]
expected_files = {
    "DATABASE_BEFORE_BACKEND": "workflows/deploy-production.yml",
    "DEPENDENCY_ORDER": "scripts/schedule.py",
    "SHARED_PATH_FANOUT": "scripts/schedule.py",
    "SKIPPED_IS_NOT_SUCCESS": "scripts/summary.py",
    "UNKNOWN_SERVICE_FAILS_CLOSED": "scripts/schedule.py",
}
for finding in findings:
    assert finding["file"] == expected_files[finding["code"]]
    assert isinstance(finding["line"], int) and finding["line"] > 0
    assert len(finding["explanation"].split()) >= 5

print("REVIEW_QUALITY_GATE_OK: exact 5 blocking findings")
