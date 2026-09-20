"""Offline lifecycle reference, deliberately disconnected from the pilot and schema_v0.

All SQLite files are newly created inside an owned TemporaryDirectory. The adapter
models ordinary tables, not the private engine, FTS, real vectors or real backups.
"""

from __future__ import annotations

import hashlib
import hmac
import json
import re
import sqlite3
import tempfile
import uuid
from contextlib import contextmanager
from dataclasses import asdict, dataclass
from pathlib import Path

CAPABILITY = "mnemo-lifecycle-experimental-v1"
VERIFIER = "scoped-logical-and-exact-utf8-fixture-files-v1"
KINDS = frozenset({"current", "history", "outbox", "index", "vectors", "backups"})
LIVE_KINDS = frozenset({"current", "outbox", "index", "vectors"})
ACTIONS = frozenset({"superseded", "retracted", "erased"})


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


@dataclass(frozen=True)
class Target:
    name: str
    kind: str


@dataclass(frozen=True)
class Request:
    operation_id: str
    tenant: str
    subject: str
    action: str
    targets: tuple[Target, ...]
    record_id: str | None = None
    capability: str = CAPABILITY
    authorized_by: str = ""  # An untrusted claim. It never grants authority.

    def validate(self):
        if self.capability != CAPABILITY or self.action not in ACTIONS:
            raise ValueError("unsupported_lifecycle_capability_or_action")
        if any(
            not isinstance(x, str) or not x or len(x) > 512
            for x in (self.operation_id, self.tenant, self.subject)
        ):
            raise ValueError("invalid_lifecycle_identity")
        if self.action != "erased" and not self.record_id:
            raise ValueError("version_record_id_required")
        if self.record_id is not None and (
            not isinstance(self.record_id, str)
            or not self.record_id.strip()
            or len(self.record_id) > 512
        ):
            raise ValueError("invalid_version_record_id")
        if self.action == "erased" and self.record_id is not None:
            raise ValueError("erasure_is_subject_wide_in_this_reference")
        if not self.targets or len({t.name for t in self.targets}) != len(self.targets):
            raise ValueError("empty_or_duplicate_target_inventory")
        for t in self.targets:
            if not re.fullmatch(r"[a-z][a-z0-9_-]{0,63}", t.name):
                raise ValueError("invalid_target_label")


class SyntheticWorkspace:
    """The only constructor for experimental storage; accepts no caller path."""

    def __init__(self):
        self._temp = tempfile.TemporaryDirectory(prefix="mnemo-lifecycle-experimental-")
        self.root = Path(self._temp.name).resolve()

    def path(self, name):
        if not re.fullmatch(r"[a-z][a-z0-9_-]{0,63}", name):
            raise ValueError("invalid_fixture_filename")
        path = (self.root / (name + ".sqlite")).resolve()
        if self.root != Path(self._temp.name).resolve() or path.parent != self.root:
            raise ValueError("fixture_path_escape")
        return path

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self._temp.cleanup()


class _Database:
    def __init__(self, workspace, name, schema, *, wal=False, secure=True):
        if not isinstance(workspace, SyntheticWorkspace):
            raise TypeError("owned_synthetic_workspace_required")
        self.workspace, self.name = workspace, name
        self.path = workspace.path(name)
        self.secure = secure
        new = not self.path.exists()
        if new:
            # Exclusive creation prevents adopting an unrelated existing database.
            with self.path.open("xb"):
                pass
        con = sqlite3.connect(self.path)
        try:
            if new:
                con.execute("CREATE TABLE fixture_marker(value TEXT NOT NULL)")
                con.execute("INSERT INTO fixture_marker VALUES (?)", (CAPABILITY,))
            else:
                try:
                    marker = con.execute("SELECT value FROM fixture_marker").fetchall()
                except sqlite3.Error as exc:
                    raise ValueError("not_a_lifecycle_fixture") from exc
                if marker != [(CAPABILITY,)]:
                    raise ValueError("not_a_lifecycle_fixture")
            con.executescript(schema)
            con.commit()
        finally:
            con.close()
        if new:
            with self.connect() as con:
                mode = con.execute(
                    "PRAGMA journal_mode=" + ("WAL" if wal else "DELETE")
                ).fetchone()[0]
                if mode.lower() != ("wal" if wal else "delete"):
                    raise RuntimeError("fixture_journal_mode_unavailable")

    @contextmanager
    def connect(self):
        if self.workspace.path(self.name) != self.path or not self.path.is_file():
            raise RuntimeError("fixture_target_missing_or_moved")
        for suffix in ("-wal", "-shm", "-journal"):
            sidecar = Path(str(self.path) + suffix)
            if sidecar.exists() and sidecar.resolve().parent != self.workspace.root:
                raise RuntimeError("fixture_sidecar_escape")
        con = sqlite3.connect(self.path, isolation_level=None, timeout=0.2)
        try:
            marker = con.execute("SELECT value FROM fixture_marker").fetchall()
            if marker != [(CAPABILITY,)]:
                raise ValueError("not_a_lifecycle_fixture")
            con.row_factory = sqlite3.Row
            con.execute("PRAGMA secure_delete=" + ("ON" if self.secure else "OFF"))
            con.execute("PRAGMA synchronous=FULL")
            yield con
            if con.in_transaction:
                con.commit()
        except BaseException:
            if con.in_transaction:
                con.rollback()
            raise
        finally:
            con.close()


class SQLiteTarget(_Database):
    """Synthetic rows stand in for one named store, including one backup target."""

    def __init__(self, workspace, target: Target, *, wal=False, secure=True, vacuum=True):
        if target.kind not in KINDS:
            raise ValueError("unknown_synthetic_target_kind")
        self.target, self.vacuum = target, vacuum
        super().__init__(
            workspace,
            "target-" + target.name,
            """
            CREATE TABLE IF NOT EXISTS records(
                scope_key TEXT, record_key TEXT, payload TEXT, current INTEGER NOT NULL,
                delivery_key TEXT UNIQUE);
            CREATE INDEX IF NOT EXISTS lookup ON records(scope_key,record_key);
        """,
            wal=wal,
            secure=secure,
        )

    def seed(self, scope_key, record_key, payload, *, current=True, delivery_key=None):
        """Fixture setup only. This deliberately bypasses the delivery fence."""
        with self.connect() as con:
            con.execute(
                "INSERT OR IGNORE INTO records VALUES (?,?,?,?,?)",
                (scope_key, record_key, payload, int(current), delivery_key or uuid.uuid4().hex),
            )

    def inspect(self, scope_key, record_key=None):
        where, args = "scope_key=?", [scope_key]
        if record_key is not None:
            where += " AND record_key=?"
            args.append(record_key)
        with self.connect() as con:
            row = con.execute(
                "SELECT count(*),coalesce(sum(current),0) FROM records WHERE " + where, args
            ).fetchone()
            return {"rows": row[0], "current": row[1]}

    def apply(self, action, scope_key, record_key=None):
        where, args = "scope_key=?", [scope_key]
        if record_key is not None:
            where += " AND record_key=?"
            args.append(record_key)
        with self.connect() as con:
            con.execute("BEGIN IMMEDIATE")
            if action == "erased":
                con.execute("DELETE FROM records WHERE " + where, args)
            elif self.target.kind in LIVE_KINDS:
                con.execute("UPDATE records SET current=0 WHERE " + where, args)
            con.commit()
            if action == "erased":
                if self.vacuum:
                    con.execute("VACUUM")
                checkpoint = con.execute("PRAGMA wal_checkpoint(TRUNCATE)").fetchone()
                if checkpoint and checkpoint[0] == 1:
                    raise RuntimeError("wal_checkpoint_busy")

    def byte_probe(self, values):
        """Exact UTF-8 marker search in these fixture files only. No global claim."""
        if not values:
            return {"status": "not_checked", "matched": None, "files": []}
        self.workspace.path(self.name)  # Resolve again before reading any file.
        if not self.path.exists():
            return {"status": "unknown", "matched": None, "files": []}
        matched, files = 0, []
        hits = [False] * len(values)
        for suffix in ("", "-wal", "-shm", "-journal"):
            path = Path(str(self.path) + suffix)
            if path.exists():
                if path.resolve().parent != self.workspace.root:
                    raise RuntimeError("fixture_sidecar_escape")
                raw = path.read_bytes()
                present = [value.encode("utf-8") in raw for value in values]
                matched += sum(present)
                hits = [a or b for a, b in zip(hits, present)]
                files.append({"name": path.name, "bytes": len(raw)})
        return {"status": "checked", "matched": matched, "files": files, "probe_hits": hits}

    def calibrate(self, scope_key, record_key, values):
        where, args = "scope_key=?", [scope_key]
        if record_key is not None:
            where += " AND record_key=?"
            args.append(record_key)
        with self.connect() as con:
            payloads = [
                r[0] for r in con.execute("SELECT payload FROM records WHERE " + where, args)
            ]
        logical = bool(values) and all(any(v in p for p in payloads) for v in values)
        physical = self.byte_probe(values)
        passed = logical and physical["status"] == "checked" and all(physical["probe_hits"])
        return {
            "status": "positive_verified" if passed else "positive_control_failed",
            "before_rows": len(payloads),
            "logical_values_present": logical,
            "file_values_present": physical.get("probe_hits", []),
        }


class Coordinator(_Database):
    """Durable reference fences and operation state; auth remains caller-owned."""

    def __init__(self, workspace, targets, *, hmac_key: bytes, authorize):
        if not isinstance(hmac_key, bytes) or len(hmac_key) < 32:
            raise ValueError("persistent_hmac_key_required")
        self._key, self.authorize, self.targets = hmac_key, authorize, dict(targets)
        super().__init__(
            workspace,
            "coordinator",
            """
            CREATE TABLE IF NOT EXISTS key_check(value TEXT PRIMARY KEY);
            CREATE TABLE IF NOT EXISTS operations(
                op_key TEXT PRIMARY KEY, request_key TEXT NOT NULL, state TEXT NOT NULL,
                entries TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS fences(
                scope_key TEXT, record_key TEXT, PRIMARY KEY(scope_key,record_key));
            CREATE TABLE IF NOT EXISTS deliveries(
                token TEXT PRIMARY KEY, scope_key TEXT, record_key TEXT,
                target_name TEXT, payload_key TEXT, state TEXT NOT NULL);
        """,
        )
        check = self.key("key-check", CAPABILITY)
        with self.connect() as con:
            con.execute("BEGIN IMMEDIATE")
            rows = con.execute("SELECT value FROM key_check").fetchall()
            if rows and rows[0][0] != check:
                raise ValueError("hmac_key_mismatch_on_restart")
            con.execute("INSERT OR IGNORE INTO key_check VALUES (?)", (check,))

    def key(self, domain, *parts):
        return hmac.new(self._key, canonical([domain, *parts]).encode(), hashlib.sha256).hexdigest()

    def scope(self, tenant, subject):
        return self.key("subject", tenant, subject)

    def record(self, record_id):
        return self.key("record", record_id)

    @staticmethod
    def _blocked(con, scope_key, record_key):
        return (
            con.execute(
                "SELECT 1 FROM fences WHERE scope_key=? AND record_key IN (?, '*')",
                (scope_key, record_key),
            ).fetchone()
            is not None
        )

    def suppressed(self, tenant, subject, record_id):
        with self.connect() as con:
            return self._blocked(con, self.scope(tenant, subject), self.record(record_id))

    def current_evidence(self, tenant, subject, record_id, target_name):
        """Reference read boundary; callers must not bypass it via fixture internals."""
        target = self.targets[target_name]
        if target.target.kind not in LIVE_KINDS:
            raise ValueError("audit_target_is_not_current_evidence")
        scope_key, record_key = self.scope(tenant, subject), self.record(record_id)
        with self.connect() as con:
            con.execute("BEGIN IMMEDIATE")
            if self._blocked(con, scope_key, record_key):
                return []
            with target.connect() as read:
                return [
                    r[0]
                    for r in read.execute(
                        "SELECT payload FROM records WHERE scope_key=? "
                        "AND record_key=? AND current=1",
                        (scope_key, record_key),
                    )
                ]

    def claim_delivery(self, tenant, subject, record_id, target_name, payload):
        """Persist before dispatch. Unknown delivery outcomes must remain in flight."""
        if target_name not in self.targets:
            raise ValueError("unknown_delivery_target")
        scope_key, record_key = self.scope(tenant, subject), self.record(record_id)
        with self.connect() as con:
            con.execute("BEGIN IMMEDIATE")
            if self._blocked(con, scope_key, record_key):
                raise ValueError("lifecycle_replay_rejected")
            payload_key = self.key("payload", payload)
            prior = con.execute(
                "SELECT * FROM deliveries WHERE scope_key=? AND record_key=?",
                (scope_key, record_key),
            ).fetchall()
            if any(row["payload_key"] != payload_key for row in prior):
                raise ValueError("immutable_record_payload_conflict")
            for row in prior:
                if row["target_name"] == target_name and row["state"] in {"inflight", "delivered"}:
                    return row["token"]
            token = uuid.uuid4().hex
            con.execute(
                "INSERT INTO deliveries VALUES (?,?,?,?,?,?)",
                (
                    token,
                    scope_key,
                    record_key,
                    target_name,
                    payload_key,
                    "inflight",
                ),
            )
        return token

    def settle_delivery(self, token, target_name, *, outcome):
        """Exact known ACK/cancel only; 'unknown' is deliberately not an ACK."""
        if outcome not in {"delivered", "cancelled", "unknown"}:
            raise ValueError("invalid_delivery_outcome")
        with self.connect() as con:
            con.execute("BEGIN IMMEDIATE")
            row = con.execute("SELECT * FROM deliveries WHERE token=?", (token,)).fetchone()
            if row is None or row["target_name"] != target_name:
                raise ValueError("unknown_or_wrong_target_ack")
            if outcome == "unknown":
                return row["state"]
            if row["state"] != "inflight" and row["state"] != outcome:
                raise ValueError("conflicting_delivery_ack")
            con.execute("UPDATE deliveries SET state=? WHERE token=?", (outcome, token))
            return (
                "suppressed_late_ack"
                if self._blocked(con, row["scope_key"], row["record_key"])
                else outcome
            )

    def commit_synthetic_delivery(self, token, target_name, payload):
        """Single-process fixture final fence recheck; not a remote transaction."""
        with self.connect() as con:
            con.execute("BEGIN IMMEDIATE")
            row = con.execute("SELECT * FROM deliveries WHERE token=?", (token,)).fetchone()
            if row is None or row["target_name"] != target_name:
                raise ValueError("unknown_or_wrong_target_delivery")
            if self.key("payload", payload) != row["payload_key"]:
                raise ValueError("changed_delivery_payload")
            if self._blocked(con, row["scope_key"], row["record_key"]):
                con.execute(
                    "UPDATE deliveries SET state='cancelled' WHERE token=? AND state='inflight'",
                    (token,),
                )
                return "suppressed"
            if row["state"] != "inflight":
                return row["state"]
            self.targets[target_name].seed(
                row["scope_key"], row["record_key"], payload, delivery_key=token
            )
            con.execute("UPDATE deliveries SET state='delivered' WHERE token=?", (token,))
            return "delivered"

    def execute(
        self,
        request: Request,
        principal,
        *,
        dry_run=False,
        probe_values=(),
        after_fence=None,
        after_target=None,
    ):
        request.validate()
        if not isinstance(probe_values, (tuple, list)) or any(
            not isinstance(v, str) or not v for v in probe_values
        ):
            raise ValueError("invalid_verification_probes")
        if self.authorize(principal, request) is not True:
            raise PermissionError("lifecycle_authorization_denied")
        op_key = self.key("operation", request.operation_id)
        request_key = self.key("request", asdict(request), principal, VERIFIER, probe_values)
        scope_key = self.scope(request.tenant, request.subject)
        record_key = self.record(request.record_id) if request.record_id else None
        missing = sorted(KINDS - {t.kind for t in request.targets})
        missing_targets = sorted(set(self.targets) - {t.name for t in request.targets})
        entries = {}
        if not dry_run:
            with self.connect() as con:
                con.execute("BEGIN IMMEDIATE")
                row = con.execute("SELECT * FROM operations WHERE op_key=?", (op_key,)).fetchone()
                if row and row["request_key"] != request_key:
                    raise ValueError("immutable_operation_id_conflict")
                if row:
                    entries = json.loads(row["entries"])
                con.execute(
                    "INSERT OR IGNORE INTO operations VALUES (?,?,?,?)",
                    (op_key, request_key, "fenced", "{}"),
                )
                con.execute(
                    "INSERT OR IGNORE INTO fences VALUES (?,?)", (scope_key, record_key or "*")
                )
            if after_fence:
                after_fence()
            with self.connect() as con:
                sql = "SELECT count(*) FROM deliveries WHERE scope_key=? AND state='inflight'"
                args = [scope_key]
                if record_key:
                    sql += " AND record_key=?"
                    args.append(record_key)
                pending = con.execute(sql, args).fetchone()[0]
            if pending:
                return self._receipt(
                    request, op_key, entries, missing, False, "awaiting_quiescence", missing_targets
                )
        for target in request.targets:
            adapter = self.targets.get(target.name)
            calibration = entries.get(target.name, {}).get("calibration")
            if target.kind not in KINDS or adapter is None or adapter.target != target:
                entries[target.name] = {"kind": target.kind, "status": "unknown_target"}
                continue
            try:
                before = adapter.inspect(scope_key, record_key)
                if dry_run:
                    entries[target.name] = {
                        "kind": target.kind,
                        "status": "would_apply",
                        "matched_rows": before["rows"],
                    }
                    continue
                if request.action == "erased":
                    if before["rows"]:
                        calibration = adapter.calibrate(scope_key, record_key, probe_values)
                    elif calibration is None:
                        calibration = {"status": "empty_at_observation", "before_rows": 0}
                    entries[target.name] = {
                        "kind": target.kind,
                        "status": "calibrating",
                        "calibration": calibration,
                    }
                    # Persist verifier calibration before deleting the first payload byte.
                    with self.connect() as con:
                        con.execute(
                            "UPDATE operations SET entries=?,state='calibrated' WHERE op_key=?",
                            (canonical(entries), op_key),
                        )
                    if calibration["status"] == "positive_control_failed":
                        raise RuntimeError("positive_control_failed_before_purge")
                adapter.apply(request.action, scope_key, record_key)
                if after_target:
                    after_target(target.name)
                remaining = adapter.inspect(scope_key, record_key)
                byte_check = (
                    adapter.byte_probe(probe_values) if request.action == "erased" else None
                )
                logical_ok = (
                    remaining["rows"] == 0
                    if request.action == "erased"
                    else remaining["current"] == 0
                    if target.kind in LIVE_KINDS
                    else True
                )
                verified = logical_ok and (
                    request.action != "erased"
                    or (byte_check["status"] == "checked" and byte_check["matched"] == 0)
                )
                entries[target.name] = {
                    "kind": target.kind,
                    "status": "verified" if verified else "incomplete",
                    "remaining": remaining,
                    "logical_verified": logical_ok,
                    "byte_probe": byte_check,
                    "audit_content_retained": request.action != "erased",
                    "calibration": calibration,
                }
            except Exception as exc:
                entries[target.name] = {
                    "kind": target.kind,
                    "status": "failed",
                    "error_class": type(exc).__name__,
                    "calibration": calibration,
                }
            if not dry_run:
                with self.connect() as con:
                    con.execute(
                        "UPDATE operations SET entries=?,state='applying' WHERE op_key=?",
                        (canonical(entries), op_key),
                    )
        complete = (
            not dry_run
            and not missing
            and not missing_targets
            and all(x["status"] == "verified" for x in entries.values())
        )
        state = "dry_run" if dry_run else "complete" if complete else "incomplete"
        if not dry_run:
            with self.connect() as con:
                con.execute(
                    "UPDATE operations SET entries=?,state=? WHERE op_key=?",
                    (canonical(entries), state, op_key),
                )
        return self._receipt(request, op_key, entries, missing, complete, state, missing_targets)

    @staticmethod
    def _receipt(request, op_key, entries, missing, complete, state, missing_targets):
        return {
            "capability": CAPABILITY,
            "schema_v0_modified": False,
            "operation_key": op_key,
            "action": request.action,
            "state": state,
            "complete": complete,
            "scope": "explicit synthetic target inventory only",
            "missing_kinds": missing,
            "missing_targets": missing_targets,
            "verifier": VERIFIER,
            "entries": entries,
            "global_erasure_proven": False,
            "retained_identifiers": "keyed HMAC pseudonyms; not anonymous",
            "excluded": [
                "unregistered copies",
                "process memory",
                "filesystem snapshots",
                "SSD remnants",
                "FTS shadow tables",
                "embedding inversion",
                "real backup retention",
                "legal compliance",
            ],
        }
