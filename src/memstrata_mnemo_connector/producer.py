# Copyright (c) 2026 Neeraj Yadav
# SPDX-License-Identifier: MIT
# Inspeximus schema_v0 mapping adapted from the Agora reference emitter.
# Upstream copyright and license notices are preserved in NOTICE.

"""Durable sender for the explicitly scoped Inspeximus schema_v0 pilot.

An outbox is bound to one destination. Source ids are immutable: later revisions,
retractions and deletion replication are NOT supported by this pilot contract.
Only byte-identical queued payloads are retried, including the time-dependent
effective_value. No bearer or Cloudflare Access credential is stored here.

remember_and_enqueue is deliberately NOT a transaction spanning Inspeximus and
this database. A crash after source flush and before enqueue needs reconciliation.
Snapshot reconciliation cannot recover a record already erased upstream.
"""

from __future__ import annotations

import hashlib
import http.client
import ipaddress
import json
import math
import sqlite3
import ssl
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid
from collections.abc import Callable
from contextlib import contextmanager
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

MAX_BODY_BYTES = 64 * 1024
MAX_ACK_BYTES = 16 * 1024


class ProducerError(ValueError):
    """A safe error without request bodies or credentials."""


class SourceConflict(ProducerError):
    """A source id was reused with different content."""


@dataclass(frozen=True)
class DeliveryCredentials:
    bearer_token: str = field(repr=False)
    access_client_id: str | None = field(default=None, repr=False)
    access_client_secret: str | None = field(default=None, repr=False)

    def headers(self) -> dict[str, str]:
        values = [self.bearer_token, self.access_client_id, self.access_client_secret]
        if not isinstance(self.bearer_token, str) or not self.bearer_token:
            raise ProducerError("A nonempty application credential is required.")
        if bool(self.access_client_id) != bool(self.access_client_secret):
            raise ProducerError("Both Cloudflare Access credentials must be supplied together.")
        for value in values:
            if value is not None and (
                not isinstance(value, str)
                or not value
                or any(ord(c) < 33 or ord(c) > 126 for c in value)
            ):
                raise ProducerError("Credentials must contain only visible ASCII without spaces.")
        result = {"Authorization": "Bearer " + self.bearer_token}
        if self.access_client_id:
            result["CF-Access-Client-Id"] = self.access_client_id
            result["CF-Access-Client-Secret"] = self.access_client_secret  # type: ignore[assignment]
        return result


@dataclass(frozen=True)
class DeliveryResult:
    source_id: str
    state: str
    attempts: int
    reason: str | None = None
    http_status: int | None = None


def canonical_bytes(payload: dict[str, Any]) -> bytes:
    try:
        return json.dumps(
            payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False
        ).encode("utf-8")
    except (TypeError, ValueError, UnicodeError, RecursionError):
        raise ProducerError("Payload must be finite, UTF-8 encodable JSON.") from None


def _number(value: Any) -> bool:
    try:
        return (
            isinstance(value, (int, float)) and not isinstance(value, bool) and math.isfinite(value)
        )
    except OverflowError:
        return False


def _source_id(payload: dict[str, Any]) -> str:
    if not isinstance(payload, dict) or payload.get("version") != "schema_v0":
        raise ProducerError("Expected a schema_v0 fact envelope.")
    fact = payload.get("fact_record")
    if not isinstance(fact, dict):
        raise ProducerError("Expected one fact_record object.")
    source_id = fact.get("id")
    if not isinstance(source_id, str) or not source_id.strip() or len(source_id) > 512:
        raise ProducerError("A nonempty source id of at most 512 characters is required.")
    if not isinstance(fact.get("text"), str) or not fact["text"].strip():
        raise ProducerError("Fact text must be nonempty.")
    if not _number(fact.get("valid_from")) or not _number(fact.get("recorded_at")):
        raise ProducerError("Fact timestamps must be finite numeric epoch seconds.")
    if fact.get("status") not in ("active", "superseded", "hub"):
        raise ProducerError("Unsupported upstream status for schema_v0.")
    if not isinstance(fact.get("sources"), list):
        raise ProducerError("Fact sources must be an array.")
    for source in fact["sources"]:
        if not isinstance(source, dict) or not all(
            isinstance(source.get(k), str) for k in ("channel", "principal")
        ):
            raise ProducerError("Every source needs string channel and principal fields.")
    count = fact.get("corroboration_count")
    if not isinstance(count, int) or isinstance(count, bool) or count < 0:
        raise ProducerError("Corroboration count must be a nonnegative integer.")
    return source_id


def _endpoint(url: str, allow_loopback_http: bool) -> str:
    try:
        parsed = urllib.parse.urlsplit(url)
        port = parsed.port
        host = parsed.hostname
    except (ValueError, TypeError):
        raise ProducerError("Invalid bridge endpoint.") from None
    if (
        not host
        or parsed.username is not None
        or parsed.password is not None
        or parsed.query
        or parsed.fragment
    ):
        raise ProducerError(
            "Endpoint must have a host and no embedded credentials, query or fragment."
        )
    if parsed.path not in ("", "/", "/mnemo/v0"):
        raise ProducerError("The only supported endpoint path is /mnemo/v0.")
    if any(ord(c) < 33 or ord(c) > 126 for c in url) or "\\" in url:
        raise ProducerError("Endpoint must be a plain ASCII URL.")
    if parsed.scheme != "https":
        try:
            loopback = ipaddress.ip_address(host).is_loopback
        except ValueError:
            loopback = False
        if parsed.scheme != "http" or not allow_loopback_http or not loopback:
            raise ProducerError(
                "HTTPS is required except explicitly enabled literal loopback HTTP."
            )
    if port is not None and not 1 <= port <= 65535:
        raise ProducerError("Invalid endpoint port.")
    return urllib.parse.urlunsplit((parsed.scheme, parsed.netloc, "/mnemo/v0", "", ""))


class _NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


def _read_ack(raw: bytes) -> dict[str, Any]:
    def pairs(items):
        result = {}
        for key, value in items:
            if key in result:
                raise ValueError("duplicate key")
            result[key] = value
        return result

    def invalid_constant(_):
        raise ValueError("nonfinite number")

    if len(raw) > MAX_ACK_BYTES:
        raise ProducerError("Oversized acknowledgement.")
    try:
        result = json.loads(
            raw.decode("utf-8"), object_pairs_hook=pairs, parse_constant=invalid_constant
        )
        if not isinstance(result, dict):
            raise ValueError("not an object")
        canonical_bytes(result)
        return result
    except (ValueError, TypeError, UnicodeError, RecursionError):
        raise ProducerError("Malformed acknowledgement.") from None


def _ack_matches(ack: dict[str, Any], source_id: str, digest: str) -> bool:
    return (
        ack.get("source_id") == source_id
        and ack.get("payload_sha256") == digest
        and ack.get("status") == "committed"
        and all(
            isinstance(ack.get(k), str) and bool(ack[k].strip())
            for k in ("ledger_record_id", "transaction_id")
        )
        and _number(ack.get("committed_at"))
    )


class DurableOutbox:
    """One destination, immutable source ids, leased sends and bounded retries.

    Construction and enqueue do not access the network. Call deliver_next from
    a service loop; it never sleeps and returns None when nothing is due. A
    crashed sender's lease expires automatically. Source text stays in SQLite,
    not logs. The caller owns encryption at rest, file permissions and retention.
    """

    def __init__(
        self,
        db_path: str | Path,
        endpoint_url: str,
        *,
        allow_loopback_http: bool = False,
        max_attempts: int = 8,
        timeout: float = 20.0,
        base_backoff: float = 2.0,
        max_backoff: float = 300.0,
        max_pending: int = 10000,
        max_pending_bytes: int = 256 * 1024 * 1024,
        clock: Callable[[], float] = time.time,
    ):
        self.endpoint = _endpoint(endpoint_url, allow_loopback_http)
        if (
            not isinstance(max_attempts, int)
            or isinstance(max_attempts, bool)
            or not 1 <= max_attempts <= 100
            or not _number(timeout)
            or not 0 < timeout <= 60
            or not _number(base_backoff)
            or not _number(max_backoff)
            or not 0 < base_backoff <= max_backoff <= 3600
            or not isinstance(max_pending, int)
            or max_pending < 1
            or not isinstance(max_pending_bytes, int)
            or max_pending_bytes < MAX_BODY_BYTES
        ):
            raise ProducerError("Invalid bounded sender configuration.")
        if str(db_path) == ":memory:":
            raise ProducerError("The outbox requires a persistent database file.")
        self.path = Path(db_path).resolve()
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.max_attempts, self.timeout = max_attempts, timeout
        self.base_backoff, self.max_backoff = base_backoff, max_backoff
        self.max_pending, self.max_pending_bytes = max_pending, max_pending_bytes
        self.clock = clock
        self._opener = urllib.request.build_opener(urllib.request.ProxyHandler({}), _NoRedirect())
        with self._connect() as conn:
            conn.execute("PRAGMA journal_mode=WAL")
            conn.executescript("""
                CREATE TABLE IF NOT EXISTS producer_metadata (
                    key TEXT PRIMARY KEY, value TEXT NOT NULL);
                CREATE TABLE IF NOT EXISTS producer_outbox (
                    source_id TEXT PRIMARY KEY, payload BLOB NOT NULL, payload_sha256 TEXT NOT NULL,
                    state TEXT NOT NULL, attempts INTEGER NOT NULL DEFAULT 0,
                    created_at REAL NOT NULL, due_at REAL NOT NULL,
                    lease_token TEXT, lease_until REAL, reason TEXT, http_status INTEGER,
                    ack_json TEXT, delivered_at REAL);
                CREATE INDEX IF NOT EXISTS producer_due ON producer_outbox(state,due_at);
            """)
            conn.execute("BEGIN IMMEDIATE")
            prior = conn.execute(
                "SELECT value FROM producer_metadata WHERE key='endpoint'"
            ).fetchone()
            if prior is not None and prior[0] != self.endpoint:
                raise ProducerError("This outbox is bound to a different endpoint.")
            conn.execute(
                "INSERT OR IGNORE INTO producer_metadata VALUES ('endpoint',?)", (self.endpoint,)
            )
            conn.commit()

    @contextmanager
    def _connect(self):
        conn = sqlite3.connect(str(self.path), timeout=5.0)
        try:
            conn.execute("PRAGMA synchronous=FULL")
            conn.execute("PRAGMA busy_timeout=5000")
            conn.row_factory = sqlite3.Row
            yield conn
        except BaseException:
            conn.rollback()
            raise
        finally:
            conn.close()

    def enqueue(self, payload: dict[str, Any]) -> str:
        source_id = _source_id(payload)
        raw = canonical_bytes(payload)
        if len(raw) > MAX_BODY_BYTES:
            raise ProducerError("Fact envelope exceeds the 64 KiB request limit.")
        digest = hashlib.sha256(raw).hexdigest()
        now = self.clock()
        with self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            prior = conn.execute(
                "SELECT payload_sha256,payload FROM producer_outbox WHERE source_id=?", (source_id,)
            ).fetchone()
            if prior is not None:
                if prior[0] != digest or bytes(prior[1]) != raw:
                    raise SourceConflict(
                        "Source id already queued with different content; "
                        "revisions are not supported."
                    )
                return source_id
            count, size = conn.execute(
                "SELECT COUNT(*),COALESCE(SUM(length(payload)),0) "
                "FROM producer_outbox WHERE state!='delivered'"
            ).fetchone()
            if count >= self.max_pending or size + len(raw) > self.max_pending_bytes:
                raise ProducerError(
                    "Outbox pending capacity reached; preserve the source and reconcile later."
                )
            conn.execute(
                "INSERT INTO producer_outbox"
                "(source_id,payload,payload_sha256,state,created_at,due_at) "
                "VALUES (?,?,?,'pending',?,?)",
                (source_id, raw, digest, now, now),
            )
            conn.commit()
        return source_id

    def status(self, source_id: str) -> dict[str, Any] | None:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT source_id,payload_sha256,state,attempts,due_at,reason,"
                "http_status,delivered_at "
                "FROM producer_outbox WHERE source_id=?",
                (source_id,),
            ).fetchone()
            return dict(row) if row is not None else None

    def counts(self) -> dict[str, int]:
        with self._connect() as conn:
            return {
                row[0]: row[1]
                for row in conn.execute("SELECT state,COUNT(*) FROM producer_outbox GROUP BY state")
            }

    def retry_failed(self, source_id: str) -> bool:
        """Explicit operator action after repair, never silently unstick auth/conflicts."""
        with self._connect() as conn:
            changed = conn.execute(
                "UPDATE producer_outbox SET state='pending',attempts=0,due_at=?,reason=NULL,"
                "http_status=NULL,lease_token=NULL,lease_until=NULL "
                "WHERE source_id=? AND state IN ('rejected','exhausted')",
                (self.clock(), source_id),
            )
            conn.commit()
            return changed.rowcount == 1

    def _claim(self):
        now = self.clock()
        with self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            conn.execute(
                "UPDATE producer_outbox SET state='pending',lease_token=NULL,lease_until=NULL "
                "WHERE state='sending' AND lease_until<=?",
                (now,),
            )
            conn.execute(
                "UPDATE producer_outbox SET state='exhausted',reason='retry_limit' "
                "WHERE state='pending' AND attempts>=?",
                (self.max_attempts,),
            )
            row = conn.execute(
                "SELECT * FROM producer_outbox WHERE state='pending' AND due_at<=? "
                "ORDER BY created_at,source_id LIMIT 1",
                (now,),
            ).fetchone()
            if row is None:
                conn.commit()
                return None
            claim = dict(row)
            claim["lease_token"] = uuid.uuid4().hex
            claim["attempts"] += 1
            conn.execute(
                "UPDATE producer_outbox SET state='sending',attempts=?,lease_token=?,lease_until=? "
                "WHERE source_id=?",
                (
                    claim["attempts"],
                    claim["lease_token"],
                    now + self.timeout + 30,
                    claim["source_id"],
                ),
            )
            conn.commit()
            return claim

    def _finish(self, claim, state, reason=None, http_status=None, ack=None, retry_after=None):
        now = self.clock()
        if state == "pending" and claim["attempts"] >= self.max_attempts:
            state = "exhausted"
        delay = min(self.max_backoff, self.base_backoff * 2 ** min(claim["attempts"] - 1, 30))
        if retry_after is not None:
            delay = max(delay, min(self.max_backoff, retry_after))
        with self._connect() as conn:
            changed = conn.execute(
                "UPDATE producer_outbox SET state=?,reason=?,http_status=?,ack_json=?,"
                "delivered_at=?,"
                "due_at=?,lease_token=NULL,lease_until=NULL "
                "WHERE source_id=? AND state='sending' AND lease_token=?",
                (
                    state,
                    reason,
                    http_status,
                    canonical_bytes(ack).decode("utf-8") if ack is not None else None,
                    now if state == "delivered" else None,
                    now + delay,
                    claim["source_id"],
                    claim["lease_token"],
                ),
            )
            conn.commit()
            if changed.rowcount != 1:
                raise ProducerError("Send lease changed; do not mark this response delivered.")
        return DeliveryResult(claim["source_id"], state, claim["attempts"], reason, http_status)

    def deliver_next(self, *, credentials: DeliveryCredentials) -> DeliveryResult | None:
        headers = credentials.headers()  # Validate before consuming an attempt.
        claim = self._claim()
        if claim is None:
            return None
        headers.update(
            {"Content-Type": "application/json; charset=utf-8", "Accept": "application/json"}
        )
        request = urllib.request.Request(
            self.endpoint, data=bytes(claim["payload"]), headers=headers, method="POST"
        )
        status, raw, retry_after = None, b"", None
        try:
            with self._opener.open(request, timeout=self.timeout) as response:
                status = response.status
                raw = response.read(MAX_ACK_BYTES + 1)
        except urllib.error.HTTPError as exc:
            status = exc.code
            if status == 429:
                try:
                    retry_after = float(exc.headers.get("Retry-After", ""))
                    if not math.isfinite(retry_after) or retry_after < 0:
                        retry_after = None
                except (ValueError, TypeError):
                    retry_after = None
            exc.close()
        except urllib.error.URLError as exc:
            if isinstance(exc.reason, ssl.SSLError):
                return self._finish(claim, "rejected", "tls_error")
            return self._finish(claim, "pending", "transport_error")
        except ssl.SSLError:
            return self._finish(claim, "rejected", "tls_error")
        except (TimeoutError, OSError, http.client.HTTPException):
            return self._finish(claim, "pending", "transport_error")
        # No exception body, destination URL, response text or credential is logged.
        if status in (200, 201):
            try:
                ack = _read_ack(raw)
            except ProducerError:
                return self._finish(claim, "pending", "invalid_ack", status)
            if _ack_matches(ack, claim["source_id"], claim["payload_sha256"]):
                safe_ack = {
                    k: ack[k]
                    for k in (
                        "source_id",
                        "payload_sha256",
                        "status",
                        "ledger_record_id",
                        "transaction_id",
                        "committed_at",
                    )
                }
                return self._finish(claim, "delivered", http_status=status, ack=safe_ack)
            # A committed HTTP code is insufficient without exact content identity.
            return self._finish(claim, "pending", "unbound_ack", status)
        if status in (408, 425, 429) or (status is not None and 500 <= status < 600):
            return self._finish(claim, "pending", "transient_http", status, retry_after=retry_after)
        # Redirects, auth errors, conflicts and validation errors need operator repair.
        return self._finish(claim, "rejected", "permanent_http", status)


def _source_identity_kind(source: Any) -> str:
    """Describe the legacy identity choice, without claiming verified identity."""
    if not source:
        return "absent"
    if isinstance(source, dict):
        for field, kind in (("principal", "principal"), ("doc", "document"), ("id", "id")):
            if source.get(field):
                return kind
    return "opaque"


def _normal_source(source: Any) -> dict[str, str] | None:
    if not source:
        return None
    if isinstance(source, dict):
        principal = source.get("principal") or source.get("doc") or source.get("id")
        if not principal:
            try:
                principal = json.dumps(source, sort_keys=True)
            except (TypeError, ValueError, UnicodeError, RecursionError):
                raise ProducerError("Source annotations must be valid JSON.") from None
        # A document handle may accompany a principal. It is not the kind of
        # that principal, so infer "doc" only when the document is the identity.
        channel = source.get("channel") or (
            "doc" if _source_identity_kind(source) == "document" else "unknown"
        )
    else:
        principal, channel = str(source), "unknown"
    return {"channel": str(channel), "principal": str(principal)}


def freeze_record(store: Any, record: dict[str, Any]) -> dict[str, Any]:
    """Create a schema_v0 snapshot, preserving current object and attribution.

    Uses the caller's scoped handle only. The old emitter's private scoring API
    remains optional metadata. The receiver must not treat source labels as proof
    of source independence. Call once per source id, then enqueue and retry bytes.
    """
    by_id = {r["id"]: r for r in store.items}
    if record.get("id") not in by_id:
        raise ProducerError("The selected record is not visible through this scoped writer handle.")
    # Read the scoped actual record, not arbitrary substituted content under its id.
    record = by_id[record["id"]]
    key = record.get("key")
    if key is not None and not isinstance(key, str):
        raise ProducerError("The source key must be a string or null.")
    subject, relation = (key.split("::", 1) + [None])[:2] if key else (None, None)
    sources, source_indexes, associations = [], {}, []
    scoped_records = [("primary", record)]
    seen_records = {record["id"]}
    for linked_id in record.get("links", []):
        if linked_id in by_id and linked_id not in seen_records:
            seen_records.add(linked_id)
            scoped_records.append(("linked", by_id[linked_id]))
    for role, scoped_record in scoped_records:
        raw_source = scoped_record.get("source")
        source = _normal_source(raw_source)
        source_index = None
        if source:
            principal = source["principal"]
            if principal not in source_indexes:
                source_indexes[principal] = len(sources)
                sources.append(source)
            source_index = source_indexes[principal]
        if "source" in scoped_record:
            # Keep each association, even if several documents share a
            # principal. This is provenance, not additional corroboration.
            associations.append({
                "role": role,
                "writer_record_id": scoped_record["id"],
                "identity_kind": _source_identity_kind(raw_source),
                "source_index": source_index,
                "source": raw_source,
            })
    fact = {
        "id": record["id"],
        "valid_from": record.get("valid_from", record["ts"]),
        "recorded_at": record["ts"],
        "key": key,
        "subject": subject,
        "relation": relation,
        "object": record.get("object"),
        "text": record["text"],
        "sources": sources,
        "corroboration_count": len(sources),
        "status": record["status"],
    }
    if record.get("mtype") is not None:
        fact["mtype"] = record["mtype"]
    if callable(getattr(store, "_effective_value", None)):
        fact["effective_value"] = round(store._effective_value(record, time.time()), 4)
    # No vectors or code. Preserve attribution, provenance and scope as declared
    # metadata; credentials on the receiver own authorization, not these labels.
    metadata_keys = (
        "source",
        "meta",
        "derived_from",
        "derived_from_unresolved",
        "taint",
        "user_id",
        "agent_id",
        "session_id",
        "project",
        "tenant",
        "owner_agent",
        "attestation",
        "identity_confidence",
        "pii",
        "provisional",
    )
    fact["writer_metadata"] = {k: record[k] for k in metadata_keys if k in record}
    fact["writer_metadata"]["source_provenance"] = {
        "version": "source_provenance_v1",
        "identity_rule": "principal_then_doc_then_id_else_opaque",
        "independence": "unverified",
        "associations": associations,
    }
    payload = {"version": "schema_v0", "fact_record": fact}
    _source_id(payload)
    # Detach mutable tracked dictionaries from the source handle.
    raw = canonical_bytes(payload)
    if len(raw) > MAX_BODY_BYTES:
        raise ProducerError("Fact envelope exceeds the 64 KiB request limit.")
    return json.loads(raw.decode("utf-8"))


def remember_and_enqueue(store: Any, outbox: DurableOutbox, text: str, **kwargs) -> str:
    """Explicit source write -> flush -> enqueue; NOT an atomic cross-store write.

    If enqueue fails the source may already be durable. Do not blindly repeat
    remember: reconcile that source id into this outbox after repairing capacity.
    This wrapper does not intercept other MCP tools or existing agent writes.
    """
    source_id = store.remember(text, **kwargs)
    store.flush()
    record = next((r for r in store.items if r.get("id") == source_id), None)
    if record is None:
        raise ProducerError(
            "The source write is not visible after flush; reconcile the writer before retrying."
        )
    outbox.enqueue(freeze_record(store, record))
    return source_id
