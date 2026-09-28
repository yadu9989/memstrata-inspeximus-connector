"""Rastislav's dishonest-adapter counterexample and independent verifier boundaries."""
from pathlib import Path
import pytest
from research import fixture_verifier as v
from research.lifecycle_reference import Coordinator, KINDS, Request, SQLiteTarget, SyntheticWorkspace, Target

MARKER = "SYNTHETIC-INDEPENDENT-VERIFIER-9b9b"
KEY = b"synthetic-only-probe-key-at-least-32-bytes"


class LyingAdapter(SQLiteTarget):
    def apply(self, *args): pass
    def inspect(self, *args): return {"rows": 0, "current": 0}
    def byte_probe(self, values):
        return {"status": "checked", "matched": 0, "files": [], "probe_hits": [False] * len(values)}
    def calibrate(self, *args):
        return {"status": "positive_verified", "before_rows": 1}


def world(ws, cls=SQLiteTarget):
    targets = tuple(Target(k, k) for k in sorted(KINDS))
    stores = {t.name: cls(ws, t) for t in targets}
    c = Coordinator(ws, stores, hmac_key=KEY, authorize=lambda p, r: p == "operator")
    for store in stores.values():
        store.seed(c.scope("tenant", "subject"), c.record("record"), MARKER * 100)
    return c, stores, Request("op", "tenant", "subject", "erased", targets)


def test_lying_inspect_byte_probe_and_calibration_cannot_pass():
    with SyntheticWorkspace() as ws:
        c, stores, req = world(ws, LyingAdapter)
        result = c.execute(req, "operator", probe_values=(MARKER,))
        assert not result["complete"]
        for t in req.targets:
            entry = result["entries"][t.name]
            assert entry["status"] == "incomplete"
            assert entry["byte_probe"]["matched"] > 0
            assert entry["remaining"]["rows"] == 1
            assert not entry["adapter_agrees"]
            assert MARKER.encode() in ws.path("target-" + t.name).read_bytes()


def test_honest_erasure_and_operation_retry():
    with SyntheticWorkspace() as ws:
        c, _, req = world(ws)
        assert c.execute(req, "operator", probe_values=(MARKER,))["complete"]
        assert c.execute(req, "operator", probe_values=(MARKER,))["complete"]
        with pytest.raises(ValueError, match="immutable_operation"):
            c.execute(req, "operator", probe_values=())


def test_adapter_clean_path_does_not_redirect_verifier():
    with SyntheticWorkspace() as ws:
        c, stores, req = world(ws, LyingAdapter)
        clean = SQLiteTarget(ws, Target("unregistered-clean", "current"))
        for store in stores.values(): store.path = clean.path
        result = c.execute(req, "operator", probe_values=(MARKER,))
        assert not result["complete"]
        assert all(e["byte_probe"]["matched"] > 0 for e in result["entries"].values())


def test_unreadable_file_never_completes(monkeypatch):
    with SyntheticWorkspace() as ws:
        c, _, req = world(ws)
        original = Path.read_bytes
        def refuse(path):
            if path.name.startswith("target-"): raise PermissionError("synthetic denied")
            return original(path)
        monkeypatch.setattr(Path, "read_bytes", refuse)
        assert not c.execute(req, "operator", probe_values=(MARKER,))["complete"]


def test_missing_database_never_completes():
    with SyntheticWorkspace() as ws:
        c, stores, req = world(ws)
        def remove_one(name):
            if name == "current": stores[name].path.unlink()  # Owned temporary fixture only.
        result = c.execute(req, "operator", probe_values=(MARKER,), after_target=remove_one)
        assert not result["complete"] and result["entries"]["current"]["status"] == "failed"


@pytest.mark.parametrize("suffix", ["-wal", "-shm", "-journal"])
def test_independent_scan_covers_sidecars(suffix):
    with SyntheticWorkspace() as ws:
        database = ws.path("synthetic")
        database.write_bytes(b"clean")
        Path(str(database) + suffix).write_bytes(MARKER.encode())
        assert v.byte_scan(ws.root, database, (MARKER,))["matched"] == 1
