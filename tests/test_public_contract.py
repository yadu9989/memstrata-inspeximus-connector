"""Checks for public fixtures, exports and the source distribution boundary."""

import ast
import json
import sys
from pathlib import Path

try:
    import tomllib
except ModuleNotFoundError:  # Python 3.10 development extra.
    import tomli as tomllib

import memstrata_mnemo_connector as connector


ROOT = Path(__file__).resolve().parents[1]


def test_public_exports_are_available():
    assert connector.__version__ == "0.1.1"
    for name in connector.__all__:
        assert callable(getattr(connector, name))


def test_runtime_imports_use_only_the_standard_library():
    tree = ast.parse((ROOT / "src/memstrata_mnemo_connector/producer.py").read_text("utf-8"))
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            names = [alias.name for alias in node.names]
        elif isinstance(node, ast.ImportFrom):
            assert node.level == 0
            names = [node.module]
        else:
            continue
        assert all(name.split(".")[0] in sys.stdlib_module_names for name in names)


def test_package_discovery_is_an_explicit_whitelist():
    config = tomllib.loads((ROOT / "pyproject.toml").read_text("utf-8"))
    assert config["project"]["dependencies"] == []
    assert config["tool"]["setuptools"]["packages"]["find"]["include"] == [
        "memstrata_mnemo_connector"
    ]
    assert sorted(p.name for p in (ROOT / "src/memstrata_mnemo_connector").glob("*.py")) == [
        "__init__.py", "producer.py"
    ]


def test_fixture_pair_is_synthetic_and_can_be_queued(tmp_path):
    before = json.loads((ROOT / "fixtures/before.json").read_text("utf-8"))
    after = json.loads((ROOT / "fixtures/after.json").read_text("utf-8"))
    assert before["fact_record"]["id"].startswith("synthetic-")
    assert after["fact_record"]["id"].startswith("synthetic-")
    assert before["fact_record"]["key"] == after["fact_record"]["key"]
    assert before["fact_record"]["valid_from"] < after["fact_record"]["valid_from"]
    assert before["fact_record"]["text"] != after["fact_record"]["text"]
    box = connector.DurableOutbox(tmp_path / "outbox.db", "https://memory.example.com")
    box.enqueue(before)
    box.enqueue(after)
    assert box.counts() == {"pending": 2}
