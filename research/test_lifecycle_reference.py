"""Offline lifecycle invariants using owned temporary SQLite fixtures only."""

import json
import sqlite3
from dataclasses import replace
from pathlib import Path

import pytest

from research.lifecycle_reference import (
    ACTIONS,
    CAPABILITY,
    KINDS,
    LIVE_KINDS,
    Coordinator,
    Request,
    SQLiteTarget,
    SyntheticWorkspace,
    Target,
)

KEY = b"synthetic-key-for-offline-fixtures-only-32bytes"
MARKER = "SYNTHETIC-SENSITIVE-MARKER-4e2ca1"
ABSENT = "NEVER-INSERTED-NEGATIVE-CONTROL-a88fd0"
TARGETS = tuple(Target(kind, kind) for kind in sorted(KINDS))


def auth(principal, request):
    return principal == "fixture-operator"


@pytest.fixture
def world():
    with SyntheticWorkspace() as workspace:
        stores = {t.name: SQLiteTarget(workspace, t) for t in TARGETS}
        coordinator = Coordinator(workspace, stores, hmac_key=KEY, authorize=auth)
        yield workspace, stores, coordinator


def request(action="erased", **changes):
    req = Request(
        "synthetic-operation-1",
        "tenant-a",
        "document-a",
        action,
        TARGETS,
        record_id=None if action == "erased" else "version-a",
    )
    return replace(req, **changes)


def seed(stores, coordinator, *, marker=MARKER):
    for store in stores.values():
        store.seed(
            coordinator.scope("tenant-a", "document-a"),
            coordinator.record("version-a"),
            marker * 120,
        )
        store.seed(
            coordinator.scope("tenant-a", "document-a"),
            coordinator.record("derived-version-b"),
            marker * 120,
            current=False,
        )
        store.seed(
            coordinator.scope("tenant-b", "document-a"),
            coordinator.record("version-a"),
            "OTHER-TENANT-CONTROL",
        )


def erase(coordinator, req=None, **kwargs):
    return coordinator.execute(
        req or request(), "fixture-operator", probe_values=(MARKER,), **kwargs
    )


def test_dry_run_and_claimed_authority_have_no_effect(world):
    _, stores, coordinator = world
    seed(stores, coordinator)
    req = request(authorized_by="admin")
    with pytest.raises(PermissionError):
        coordinator.execute(req, "untrusted", probe_values=(MARKER,))
    receipt = coordinator.execute(req, "fixture-operator", dry_run=True)
    assert receipt["state"] == "dry_run" and receipt["complete"] is False
    assert not coordinator.suppressed("tenant-a", "document-a", "version-a")
    with coordinator.connect() as con:
        assert con.execute("SELECT count(*) FROM operations").fetchone()[0] == 0
    assert all(e["matched_rows"] == 2 for e in receipt["entries"].values())


@pytest.mark.parametrize("action", ["superseded", "retracted"])
def test_version_lifecycle_preserves_audit_and_other_versions(world, action):
    _, stores, coordinator = world
    seed(stores, coordinator)
    receipt = coordinator.execute(request(action), "fixture-operator")
    assert receipt["complete"] and receipt["action"] == action
    assert receipt["global_erasure_proven"] is False
    assert coordinator.suppressed("tenant-a", "document-a", "version-a")
    assert not coordinator.suppressed("tenant-a", "document-a", "derived-version-b")
    for store in stores.values():
        state = store.inspect(
            coordinator.scope("tenant-a", "document-a"), coordinator.record("version-a")
        )
        assert state["rows"] == 1
        assert state["current"] == (0 if store.target.kind in LIVE_KINDS else 1)
        assert store.byte_probe((MARKER,))["matched"] > 0


def test_erasure_all_versions_derivations_and_scoped_byte_controls(world):
    _, stores, coordinator = world
    seed(stores, coordinator)
    for store in stores.values():
        assert store.byte_probe((MARKER,))["matched"] > 0  # Positive control.
        assert store.byte_probe((ABSENT,))["matched"] == 0  # Negative control.
    receipt = erase(coordinator)
    assert receipt["complete"] and not receipt["global_erasure_proven"]
    assert receipt["schema_v0_modified"] is False
    for store in stores.values():
        assert store.inspect(coordinator.scope("tenant-a", "document-a"))["rows"] == 0
        assert store.inspect(coordinator.scope("tenant-b", "document-a"))["rows"] == 1
        assert store.byte_probe((MARKER,))["matched"] == 0
    assert coordinator.suppressed("tenant-a", "document-a", "brand-new-record-id")
    assert not coordinator.suppressed("tenant-b", "document-a", "version-a")
    retained = coordinator.path.read_bytes()
    assert MARKER.encode() not in retained and b"document-a" not in retained
    assert b"synthetic-operation-1" not in retained
    assert "pseudonyms" in receipt["retained_identifiers"]


@pytest.mark.parametrize("change", ["missing_kind", "missing_adapter", "unknown_kind"])
def test_coverage_never_defaults_to_complete(world, change):
    _, stores, coordinator = world
    seed(stores, coordinator)
    req = request()
    if change == "missing_kind":
        req = replace(req, targets=tuple(t for t in TARGETS if t.kind != "backups"))
    elif change == "missing_adapter":
        del coordinator.targets["backups"]
    else:
        req = replace(req, targets=TARGETS + (Target("unknown-copy", "unknown"),))
    receipt = erase(coordinator, req)
    assert receipt["complete"] is False
    assert receipt["state"] == "incomplete"
    assert coordinator.suppressed("tenant-a", "document-a", "version-a")


def test_second_registered_outbox_cannot_be_omitted(world):
    workspace, stores, coordinator = world
    extra = SQLiteTarget(workspace, Target("second-outbox", "outbox"))
    stores["second-outbox"] = extra
    coordinator.targets["second-outbox"] = extra
    seed(stores, coordinator)
    receipt = erase(coordinator)
    assert not receipt["complete"] and receipt["missing_kinds"] == []
    assert receipt["missing_targets"] == ["second-outbox"]
    assert extra.inspect(coordinator.scope("tenant-a", "document-a"))["rows"] == 2


def test_wrong_initial_probe_stops_before_purge(world):
    _, stores, coordinator = world
    seed(stores, coordinator)
    receipt = coordinator.execute(request(), "fixture-operator", probe_values=(ABSENT,))
    assert not receipt["complete"]
    for store in stores.values():
        assert store.inspect(coordinator.scope("tenant-a", "document-a"))["rows"] == 2
        entry = receipt["entries"][store.target.name]
        assert entry["calibration"]["status"] == "positive_control_failed"


def test_retry_cannot_weaken_probe_and_positive_calibration_survives(world):
    workspace, stores, coordinator = world
    for store in stores.values():
        store.secure = False
        store.vacuum = False
    seed(stores, coordinator)
    assert erase(coordinator)["complete"] is False
    reopened = Coordinator(workspace, stores, hmac_key=KEY, authorize=auth)
    with pytest.raises(ValueError, match="immutable_operation_id_conflict"):
        reopened.execute(request(), "fixture-operator", probe_values=(ABSENT,))
    for store in stores.values():
        assert store.byte_probe((MARKER,))["matched"] > 0
        store.secure, store.vacuum = True, True
    receipt = erase(reopened)
    assert receipt["complete"]
    for entry in receipt["entries"].values():
        assert entry["calibration"]["status"] == "positive_verified"
        assert entry["calibration"]["before_rows"] == 2


def test_file_missing_and_adapter_failure_stay_incomplete(world, monkeypatch):
    _, stores, coordinator = world
    seed(stores, coordinator)
    stores["backups"].path.unlink()  # A disposable, exact fixture file only.
    monkeypatch.setattr(
        stores["vectors"],
        "apply",
        lambda *args: (_ for _ in ()).throw(
            RuntimeError("sensitive details must never enter receipt")
        ),
    )
    receipt = erase(coordinator)
    assert not receipt["complete"]
    assert receipt["entries"]["backups"]["status"] == "failed"
    assert receipt["entries"]["vectors"]["status"] == "failed"
    assert "sensitive details" not in json.dumps(receipt)


def test_fence_committed_before_first_purge_and_restart(world):
    workspace, stores, coordinator = world
    seed(stores, coordinator)

    def crash():
        assert coordinator.suppressed("tenant-a", "document-a", "version-a")
        assert coordinator.current_evidence("tenant-a", "document-a", "version-a", "current") == []
        assert stores["outbox"].inspect(coordinator.scope("tenant-a", "document-a"))["rows"] == 2
        raise SystemExit("synthetic crash")

    with pytest.raises(SystemExit):
        erase(coordinator, after_fence=crash)
    reopened = Coordinator(workspace, stores, hmac_key=KEY, authorize=auth)
    assert erase(reopened)["complete"]
    assert erase(reopened)["complete"]  # Immutable operation retries converge.
    with pytest.raises(ValueError, match="hmac_key_mismatch"):
        Coordinator(workspace, stores, hmac_key=b"another-fixture-key" * 3, authorize=auth)


def test_crash_after_target_before_receipt_converges(world):
    workspace, stores, coordinator = world
    seed(stores, coordinator)
    with pytest.raises(SystemExit):
        erase(coordinator, after_target=lambda name: (_ for _ in ()).throw(SystemExit()))
    reopened = Coordinator(workspace, stores, hmac_key=KEY, authorize=auth)
    receipt = erase(reopened)
    assert receipt["complete"]
    assert all(
        e["calibration"]["status"] == "positive_verified" for e in receipt["entries"].values()
    )


@pytest.mark.parametrize(
    "changes",
    [
        {"subject": "other-document"},
        {"action": "retracted", "record_id": "version-a"},
        {"targets": TARGETS[:-1]},
        {"authorized_by": "changed-claim"},
    ],
)
def test_operation_id_bound_to_immutable_request(world, changes):
    _, _, coordinator = world
    erase(coordinator)
    with pytest.raises(ValueError, match="immutable_operation_id_conflict"):
        erase(coordinator, replace(request(), **changes))


def test_pending_delivery_and_late_ack_cannot_resurrect(world):
    _, stores, coordinator = world
    seed(stores, coordinator)
    token = coordinator.claim_delivery("tenant-a", "document-a", "version-a", "outbox", MARKER)
    receipt = erase(coordinator)
    assert receipt["state"] == "awaiting_quiescence" and not receipt["complete"]
    coordinator.settle_delivery(token, "outbox", outcome="unknown")
    assert erase(coordinator)["state"] == "awaiting_quiescence"
    with pytest.raises(ValueError, match="wrong_target"):
        coordinator.settle_delivery(token, "current", outcome="delivered")
    assert (
        coordinator.settle_delivery(token, "outbox", outcome="delivered") == "suppressed_late_ack"
    )
    assert erase(coordinator)["complete"]
    assert coordinator.commit_synthetic_delivery(token, "outbox", MARKER) == "suppressed"
    assert stores["outbox"].inspect(coordinator.scope("tenant-a", "document-a"))["rows"] == 0


def test_final_fence_recheck_and_fresh_outbox_replay(world):
    workspace, stores, coordinator = world
    token = coordinator.claim_delivery("tenant-a", "document-a", "version-a", "outbox", MARKER)
    assert erase(coordinator)["state"] == "awaiting_quiescence"
    assert coordinator.commit_synthetic_delivery(token, "outbox", MARKER) == "suppressed"
    assert erase(coordinator)["complete"]
    new_outbox = SQLiteTarget(workspace, Target("fresh-outbox", "outbox"))
    reopened = Coordinator(
        workspace, {**stores, "fresh-outbox": new_outbox}, hmac_key=KEY, authorize=auth
    )
    with pytest.raises(ValueError, match="replay_rejected"):
        reopened.claim_delivery("tenant-a", "document-a", "new-record-id", "fresh-outbox", MARKER)


def test_delivery_ack_and_commit_retry_idempotency(world):
    _, stores, coordinator = world
    token = coordinator.claim_delivery("tenant-a", "document-a", "version-a", "outbox", MARKER)
    with pytest.raises(ValueError, match="changed_delivery_payload"):
        coordinator.commit_synthetic_delivery(token, "outbox", "changed")
    assert coordinator.commit_synthetic_delivery(token, "outbox", MARKER) == "delivered"
    assert coordinator.commit_synthetic_delivery(token, "outbox", MARKER) == "delivered"
    assert stores["outbox"].inspect(coordinator.scope("tenant-a", "document-a"))["rows"] == 1
    assert coordinator.settle_delivery(token, "outbox", outcome="delivered") == "delivered"
    with pytest.raises(ValueError, match="conflicting_delivery_ack"):
        coordinator.settle_delivery(token, "outbox", outcome="cancelled")


def test_record_payload_identity_survives_restart_and_new_outbox(world):
    workspace, stores, coordinator = world
    token = coordinator.claim_delivery("tenant-a", "document-a", "version-a", "outbox", MARKER)
    assert (
        coordinator.claim_delivery("tenant-a", "document-a", "version-a", "outbox", MARKER) == token
    )
    coordinator.commit_synthetic_delivery(token, "outbox", MARKER)
    extra = SQLiteTarget(workspace, Target("new-outbox", "outbox"))
    reopened = Coordinator(workspace, {**stores, "new-outbox": extra}, hmac_key=KEY, authorize=auth)
    with pytest.raises(ValueError, match="immutable_record_payload_conflict"):
        reopened.claim_delivery("tenant-a", "document-a", "version-a", "new-outbox", "changed")
    assert reopened.claim_delivery("tenant-a", "document-a", "version-a", "outbox", MARKER) == token


@pytest.mark.parametrize("record_id", [12, "   ", "r" * 513])
def test_invalid_version_identity_has_no_effect(world, record_id):
    _, _, coordinator = world
    with pytest.raises(ValueError, match="invalid_version_record_id"):
        coordinator.execute(request("retracted", record_id=record_id), "fixture-operator")
    assert not coordinator.suppressed("tenant-a", "document-a", "version-a")


def test_delivery_target_commit_before_control_crash_replays_once(world, monkeypatch):
    _, stores, coordinator = world
    token = coordinator.claim_delivery("tenant-a", "document-a", "version-a", "outbox", MARKER)
    original = stores["outbox"].seed

    def crash_after_write(*args, **kwargs):
        original(*args, **kwargs)
        raise SystemExit("synthetic commit-window crash")

    monkeypatch.setattr(stores["outbox"], "seed", crash_after_write)
    with pytest.raises(SystemExit):
        coordinator.commit_synthetic_delivery(token, "outbox", MARKER)
    monkeypatch.setattr(stores["outbox"], "seed", original)
    assert coordinator.commit_synthetic_delivery(token, "outbox", MARKER) == "delivered"
    assert stores["outbox"].inspect(coordinator.scope("tenant-a", "document-a"))["rows"] == 1


def test_authentication_context_and_inventory_bound_on_restart(world):
    workspace, stores, coordinator = world
    erase(coordinator)
    reopened = Coordinator(workspace, stores, hmac_key=KEY, authorize=lambda *args: True)
    with pytest.raises(ValueError, match="immutable_operation_id_conflict"):
        reopened.execute(request(), "different-authenticated-principal", probe_values=(MARKER,))
    del reopened.targets["backups"]
    assert erase(reopened)["complete"] is False


def test_erasure_fence_survives_later_retraction(world):
    _, stores, coordinator = world
    seed(stores, coordinator)
    assert erase(coordinator)["complete"]
    later = request("retracted", operation_id="later-distinct-act")
    assert coordinator.execute(later, "fixture-operator")["complete"]
    assert coordinator.current_evidence("tenant-a", "document-a", "version-a", "current") == []
    with pytest.raises(ValueError, match="replay_rejected"):
        coordinator.claim_delivery("tenant-a", "document-a", "new-version", "outbox", MARKER)


@pytest.mark.parametrize("wal", [False, True])
def test_delete_count_alone_does_not_prove_file_bytes_erased(wal):
    with SyntheticWorkspace() as workspace:
        t = SQLiteTarget(workspace, Target("outbox", "outbox"), wal=wal, secure=False, vacuum=False)
        t.seed("scope", "record", MARKER * 150)
        assert t.byte_probe((MARKER,))["matched"] > 0
        assert t.byte_probe((ABSENT,))["matched"] == 0
        t.apply("erased", "scope")
        assert t.inspect("scope")["rows"] == 0
        assert t.byte_probe((MARKER,))["matched"] > 0
        t.vacuum = True
        t.apply("erased", "scope")
        assert t.byte_probe((MARKER,))["matched"] == 0


@pytest.mark.parametrize("wal", [False, True])
def test_secure_delete_positive_control_with_normal_connection_lifetime(wal):
    with SyntheticWorkspace() as workspace:
        t = SQLiteTarget(workspace, Target("outbox", "outbox"), wal=wal, secure=True, vacuum=False)
        t.seed("scope", "record", MARKER * 150)
        assert t.byte_probe((MARKER,))["matched"] > 0
        t.apply("erased", "scope")
        assert t.byte_probe((MARKER,))["matched"] == 0


def test_open_wal_reader_blocks_completion_until_quiescent(world):
    workspace, stores, coordinator = world
    stores["extra-wal"] = SQLiteTarget(workspace, Target("extra-wal", "outbox"), wal=True)
    coordinator.targets["extra-wal"] = stores["extra-wal"]
    req = request(targets=TARGETS + (Target("extra-wal", "outbox"),))
    seed(stores, coordinator)
    target = stores["extra-wal"]
    reader = sqlite3.connect(target.path)
    try:
        reader.execute("BEGIN")
        assert reader.execute("SELECT count(*) FROM records").fetchone()[0] == 3
        receipt = erase(coordinator, req)
        assert not receipt["complete"]
        assert reader.execute("SELECT count(*) FROM records").fetchone()[0] == 3
        assert target.byte_probe((MARKER,))["matched"] > 0
    finally:
        reader.close()
    assert erase(coordinator, req)["complete"]


def test_no_byte_probe_is_not_a_complete_erasure(world):
    _, _, coordinator = world
    result = coordinator.execute(request(), "fixture-operator")
    assert not result["complete"]
    assert all(e["byte_probe"]["status"] == "not_checked" for e in result["entries"].values())


def test_versioned_capability_path_guard_and_hmac_domain_separation(world):
    workspace, _, coordinator = world
    assert coordinator.scope("a", "bc") != coordinator.scope("ab", "c")
    assert coordinator.key("subject", "a") != coordinator.key("record", "a")
    with pytest.raises(ValueError, match="unsupported_lifecycle"):
        coordinator.execute(request(capability="schema_v0"), "fixture-operator")
    with pytest.raises(TypeError, match="owned_synthetic"):
        SQLiteTarget(Path("I:/not-a-fixture.sqlite"), TARGETS[0])
    with pytest.raises(ValueError, match="filename"):
        workspace.path("../escaped")
    assert ACTIONS == {"superseded", "retracted", "erased"}
    assert CAPABILITY != "schema_v0"
