import json
from pathlib import Path

from scripts.schedule import schedule, services_for_paths


MANIFEST = json.loads(
    (Path(__file__).parents[1] / "deployables.json").read_text()
)


def test_simple_backend_selection():
    assert schedule(["database", "backend"], MANIFEST) == ["database", "backend"]


def test_backend_path():
    assert services_for_paths(["backend/api.py"], MANIFEST) == ["backend"]
