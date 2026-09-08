"""Offline and loopback-only tests for the durable partner sender."""

import hashlib
import http.client
import io
import json
import ssl
import threading
import urllib.error
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pytest

from memstrata_mnemo_connector.producer import (
    MAX_ACK_BYTES,
    MAX_BODY_BYTES,
    DeliveryCredentials,
    DurableOutbox,
    ProducerError,
    SourceConflict,
    canonical_bytes,
    freeze_record,
    remember_and_enqueue,
)


@pytest.fixture
def payload():
    return {
        "version": "schema_v0",
        "fact_record": {
            "id": "writer-1",
            "valid_from": 1782700000.0,
            "recorded_at": 1782700000.4,
            "key": "billing::auth",
            "subject": "billing",
            "relation": "auth",
            "object": None,
            "text": "Billing uses the café account.",
            "sources": [{"channel": "doc", "principal": "runbook#auth"}],
            "corroboration_count": 1,
            "status": "active",
            "mtype": "semantic",
            "effective_value": 3.1,
        },
    }


class Clock:
    def __init__(self):
        self.now = 1000.0

    def __call__(self):
        return self.now


@pytest.fixture
def clock():
    return Clock()


@pytest.fixture
def outbox(tmp_path, clock):
    return DurableOutbox(
        tmp_path / "queue.sqlite3", "https://bridge.example.org", clock=clock, max_attempts=3
    )


@pytest.fixture
def credentials():
    return DeliveryCredentials(
        "test-only-bearer-5217", "test-only-access-id-8321", "test-only-access-secret-9812"
    )


def ack_for(raw):
    return {
        "source_id": json.loads(raw)["fact_record"]["id"],
        "payload_sha256": hashlib.sha256(raw).hexdigest(),
        "ledger_record_id": "ledger-12",
        "transaction_id": "txn-28",
        "status": "committed",
        "committed_at": 1001.0,
    }


class Response(io.BytesIO):
    status = 200


class FakeOpener:
    def __init__(self, callback):
        self.callback = callback
        self.requests = []

    def open(self, request, timeout):
        self.requests.append(request)
        return self.callback(request)


def use_ack(outbox, alter=lambda ack: ack):
    outbox._opener = FakeOpener(lambda r: Response(json.dumps(alter(ack_for(r.data))).encode()))
    return outbox._opener


def raise_http(status):
    def fail(request):
        raise urllib.error.HTTPError(
            request.full_url, status, "private server body", {}, io.BytesIO(b"not stored")
        )

    return fail


def test_canonical_matches_contract_and_is_utf8(payload):
    raw = canonical_bytes(payload)
    assert b"caf\xc3\xa9" in raw
    assert (
        raw
        == json.dumps(
            payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False
        ).encode()
    )


def test_queue_is_durable_and_input_is_detached(outbox, payload, clock):
    expected = canonical_bytes(payload)
    outbox.enqueue(payload)
    payload["fact_record"]["text"] = "changed caller value"
    reopened = DurableOutbox(outbox.path, outbox.endpoint, clock=clock)
    with reopened._connect() as conn:
        row = conn.execute("SELECT payload FROM producer_outbox").fetchone()
        assert bytes(row[0]) == expected
        assert conn.execute("PRAGMA synchronous").fetchone()[0] == 2
        assert conn.execute("PRAGMA journal_mode").fetchone()[0] == "wal"


def test_exact_reenqueue_is_idempotent_but_changed_same_id_conflicts(outbox, payload):
    assert outbox.enqueue(payload) == "writer-1"
    assert outbox.enqueue(dict(reversed(list(payload.items())))) == "writer-1"
    assert outbox.counts() == {"pending": 1}
    payload["fact_record"]["effective_value"] = 3.0
    with pytest.raises(SourceConflict):
        outbox.enqueue(payload)


@pytest.mark.parametrize("value", [float("nan"), float("inf"), float("-inf"), True, 10**1000])
def test_invalid_timestamps_rejected(outbox, payload, value):
    payload["fact_record"]["recorded_at"] = value
    with pytest.raises(ProducerError):
        outbox.enqueue(payload)
    assert not outbox.counts()


@pytest.mark.parametrize(
    "field,value",
    [
        ("id", ""),
        ("text", ""),
        ("sources", {}),
        ("sources", [{"channel": 3, "principal": "a"}]),
        ("corroboration_count", True),
        ("corroboration_count", -1),
        ("status", "deleted"),
    ],
)
def test_malformed_fact_rejected(outbox, payload, field, value):
    payload["fact_record"][field] = value
    with pytest.raises(ProducerError):
        outbox.enqueue(payload)


def test_invalid_unicode_and_body_size_rejected(outbox, payload):
    payload["fact_record"]["text"] = "\ud800"
    with pytest.raises(ProducerError):
        outbox.enqueue(payload)
    payload["fact_record"]["text"] = "x" * MAX_BODY_BYTES
    with pytest.raises(ProducerError):
        outbox.enqueue(payload)


def test_queue_capacity_fails_without_changing_prior(tmp_path, payload):
    box = DurableOutbox(tmp_path / "queue.db", "https://example.org", max_pending=1)
    box.enqueue(payload)
    payload["fact_record"]["id"] = "second"
    with pytest.raises(ProducerError):
        box.enqueue(payload)
    assert box.counts() == {"pending": 1}


@pytest.mark.parametrize(
    "url",
    [
        "http://example.org",
        "http://localhost",
        "ftp://example.org",
        "https://user:password@example.org",
        "https://example.org?token=x",
        "https://example.org#x",
        "https://example.org/other",
        "https://example.org:0",
        "https://example.org:65536",
        "https://example.org/\n",
        "https://example.org\\@elsewhere.org",
    ],
)
def test_unsafe_endpoints_rejected(tmp_path, url):
    with pytest.raises(ProducerError):
        DurableOutbox(tmp_path / "queue.db", url, allow_loopback_http=True)


def test_literal_loopback_requires_explicit_flag(tmp_path):
    with pytest.raises(ProducerError):
        DurableOutbox(tmp_path / "queue.db", "http://127.0.0.1:9000")
    box = DurableOutbox(tmp_path / "queue.db", "http://127.0.0.1:9000", allow_loopback_http=True)
    assert box.endpoint == "http://127.0.0.1:9000/mnemo/v0"


def test_destination_binding_prevents_cross_endpoint_reuse(outbox):
    with pytest.raises(ProducerError):
        DurableOutbox(outbox.path, "https://different.example.org")


def test_success_requires_bound_ack_and_sends_credentials(outbox, payload, credentials):
    outbox.enqueue(payload)
    opener = use_ack(outbox)
    result = outbox.deliver_next(credentials=credentials)
    assert result.state == "delivered"
    headers = {k.lower(): v for k, v in opener.requests[0].header_items()}
    assert headers["authorization"] == "Bearer " + credentials.bearer_token
    assert headers["cf-access-client-id"] == credentials.access_client_id
    assert headers["cf-access-client-secret"] == credentials.access_client_secret
    assert outbox.deliver_next(credentials=credentials) is None
    assert outbox.counts() == {"delivered": 1}


@pytest.mark.parametrize(
    "field,value",
    [
        ("source_id", "wrong"),
        ("payload_sha256", "wrong"),
        ("status", "queued"),
        ("status", "rejected"),
        ("ledger_record_id", ""),
        ("transaction_id", ""),
        ("transaction_id", 17),
        ("committed_at", True),
        ("committed_at", None),
    ],
)
def test_http_200_is_not_enough(outbox, payload, credentials, field, value):
    outbox.enqueue(payload)
    use_ack(outbox, lambda ack: dict(ack, **{field: value}))
    result = outbox.deliver_next(credentials=credentials)
    assert result.state == "pending"
    assert result.reason == "unbound_ack"


@pytest.mark.parametrize(
    "raw",
    [
        b"not json",
        b"[]",
        b'{"status":"committed","status":"rejected"}',
        b'{"bad":NaN}',
        b"x" * (MAX_ACK_BYTES + 1),
    ],
)
def test_invalid_ack_is_never_delivered(outbox, payload, credentials, raw):
    outbox.enqueue(payload)
    outbox._opener = FakeOpener(lambda request: Response(raw))
    result = outbox.deliver_next(credentials=credentials)
    assert result.state == "pending" and result.reason == "invalid_ack"


@pytest.mark.parametrize("code", [400, 401, 403, 404, 409, 413, 422, 301, 302, 303, 307, 308])
def test_permanent_errors_stop_automatic_retries(outbox, payload, credentials, code):
    outbox.enqueue(payload)
    outbox._opener = FakeOpener(raise_http(code))
    result = outbox.deliver_next(credentials=credentials)
    assert result.state == "rejected" and result.http_status == code
    assert outbox.deliver_next(credentials=credentials) is None
    assert outbox.retry_failed("writer-1") is True
    assert outbox.status("writer-1")["state"] == "pending"


@pytest.mark.parametrize("code", [408, 425, 429, 500, 502, 503, 504])
def test_transient_statuses_back_off(outbox, payload, credentials, clock, code):
    outbox.enqueue(payload)
    outbox._opener = FakeOpener(raise_http(code))
    result = outbox.deliver_next(credentials=credentials)
    assert result.state == "pending"
    assert outbox.status("writer-1")["due_at"] == clock.now + 2
    assert outbox.deliver_next(credentials=credentials) is None


def test_retry_limit_and_explicit_repair(outbox, payload, credentials, clock):
    outbox.enqueue(payload)
    outbox._opener = FakeOpener(raise_http(503))
    for attempt in range(3):
        clock.now += 1000
        result = outbox.deliver_next(credentials=credentials)
        assert result.attempts == attempt + 1
    assert result.state == "exhausted"
    assert outbox.deliver_next(credentials=credentials) is None
    assert outbox.retry_failed("writer-1")
    use_ack(outbox)
    assert outbox.deliver_next(credentials=credentials).state == "delivered"


def test_retry_after_is_bounded(outbox, payload, credentials, clock):
    outbox.enqueue(payload)

    def limited(request):
        raise urllib.error.HTTPError(request.full_url, 429, "rate", {"Retry-After": "999999"}, None)

    outbox._opener = FakeOpener(limited)
    assert outbox.deliver_next(credentials=credentials).state == "pending"
    assert outbox.status("writer-1")["due_at"] == clock.now + 300


@pytest.mark.parametrize(
    "exc", [urllib.error.URLError("offline"), TimeoutError(), http.client.RemoteDisconnected()]
)
def test_transport_errors_preserve_payload(outbox, payload, credentials, exc):
    outbox.enqueue(payload)

    def failed(request):
        raise exc

    outbox._opener = FakeOpener(failed)
    assert outbox.deliver_next(credentials=credentials).state == "pending"
    with outbox._connect() as conn:
        assert bytes(
            conn.execute("SELECT payload FROM producer_outbox").fetchone()[0]
        ) == canonical_bytes(payload)


def test_tls_failure_does_not_downgrade_or_retry(outbox, payload, credentials):
    outbox.enqueue(payload)

    def failed(request):
        raise urllib.error.URLError(ssl.SSLCertVerificationError("invalid certificate"))

    outbox._opener = FakeOpener(failed)
    result = outbox.deliver_next(credentials=credentials)
    assert result.state == "rejected" and result.reason == "tls_error"


@pytest.mark.parametrize(
    "creds",
    [
        DeliveryCredentials(""),
        DeliveryCredentials("test\r\nInjected: yes"),
        DeliveryCredentials("valid", "only-id"),
        DeliveryCredentials("valid", None, "only-secret"),
    ],
)
def test_bad_credentials_do_not_consume_attempt(outbox, payload, creds):
    outbox.enqueue(payload)
    with pytest.raises(ProducerError):
        outbox.deliver_next(credentials=creds)
    assert outbox.status("writer-1")["attempts"] == 0


def test_credentials_are_not_persisted_or_represented(outbox, payload, credentials, capsys):
    outbox.enqueue(payload)
    use_ack(outbox, lambda ack: dict(ack, debug_token=credentials.bearer_token))
    outbox.deliver_next(credentials=credentials)
    assert capsys.readouterr() == ("", "")
    with outbox._connect() as conn:
        dump = "\n".join(conn.iterdump())
    for secret in (
        credentials.bearer_token,
        credentials.access_client_id,
        credentials.access_client_secret,
    ):
        assert secret not in dump
        assert secret not in repr(credentials)


def test_crashed_claim_recovered_after_lease_expiry(outbox, payload, clock, credentials):
    outbox.enqueue(payload)
    assert outbox._claim()["attempts"] == 1
    reopened = DurableOutbox(outbox.path, outbox.endpoint, clock=clock)
    use_ack(reopened)
    assert reopened.deliver_next(credentials=credentials) is None
    clock.now += 51
    result = reopened.deliver_next(credentials=credentials)
    assert result.state == "delivered" and result.attempts == 2


def test_only_one_concurrent_claim(outbox, payload):
    outbox.enqueue(payload)
    with ThreadPoolExecutor(max_workers=4) as pool:
        results = list(pool.map(lambda _: outbox._claim(), range(4)))
    assert sum(result is not None for result in results) == 1


@contextmanager
def loopback_server(callback):
    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *args):
            pass

        def do_POST(self):
            body = self.rfile.read(int(self.headers.get("Content-Length", "0")))
            callback(self, body)

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{server.server_port}"
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=2)


def respond(handler, status, body=b"", **headers):
    handler.send_response(status)
    handler.send_header("Content-Length", str(len(body)))
    for key, value in headers.items():
        handler.send_header(key, value)
    handler.end_headers()
    handler.wfile.write(body)


def test_live_loopback_retry_sends_exact_bytes_after_lost_ack(
    tmp_path, payload, credentials, clock
):
    requests = []

    def callback(handler, raw):
        assert handler.path == "/mnemo/v0"
        requests.append(raw)
        if len(requests) == 1:
            handler.close_connection = True  # Simulate commit then lost acknowledgement.
            return
        respond(handler, 200, json.dumps(ack_for(raw)).encode())

    with loopback_server(callback) as endpoint:
        box = DurableOutbox(tmp_path / "queue.db", endpoint, allow_loopback_http=True, clock=clock)
        box.enqueue(payload)
        assert box.deliver_next(credentials=credentials).state == "pending"
        clock.now += 3
        reopened = DurableOutbox(box.path, endpoint, allow_loopback_http=True, clock=clock)
        assert reopened.deliver_next(credentials=credentials).state == "delivered"
    assert len(requests) == 2 and requests[0] == requests[1] == canonical_bytes(payload)


@pytest.mark.parametrize("code", [301, 302, 303, 307, 308])
def test_live_redirect_never_forwards_credentials(tmp_path, payload, credentials, code):
    leaked = []

    def target(handler, raw):
        leaked.append(dict(handler.headers))
        respond(handler, 200, b"{}")

    with loopback_server(target) as target_url:
        with loopback_server(
            lambda h, b: respond(h, code, Location=target_url + "/mnemo/v0")
        ) as endpoint:
            box = DurableOutbox(tmp_path / "queue.db", endpoint, allow_loopback_http=True)
            box.enqueue(payload)
            result = box.deliver_next(credentials=credentials)
            assert result.state == "rejected" and result.http_status == code
    assert leaked == []


class FakeWriter:
    def __init__(self):
        self.items = []
        self.flushed = False

    def remember(self, text, **kwargs):
        record = {
            "id": "new-record",
            "text": text,
            "ts": 1782700001.0,
            "status": "active",
            **kwargs,
        }
        self.items.append(record)
        return record["id"]

    def flush(self):
        self.flushed = True

    def _effective_value(self, record, now):
        return 1.25


def test_current_writer_metadata_and_object_survive(outbox):
    writer = FakeWriter()
    rid = remember_and_enqueue(
        writer,
        outbox,
        "Billing uses the new value.",
        key="billing::auth",
        object="new",
        source={"doc": "source-a"},
        user_id="user-1",
        agent_id="agent-1",
        session_id="session-1",
        project="pilot",
        tenant="scoped-tenant",
        derived_from=["source-parent"],
        meta={"purpose": "synthetic test"},
    )
    assert writer.flushed and rid == "new-record"
    with outbox._connect() as conn:
        fact = json.loads(conn.execute("SELECT payload FROM producer_outbox").fetchone()[0])[
            "fact_record"
        ]
    assert fact["object"] == "new" and fact["subject"] == "billing" and fact["relation"] == "auth"
    assert fact["writer_metadata"]["session_id"] == "session-1"
    assert fact["writer_metadata"]["derived_from"] == ["source-parent"]
    assert fact["sources"] == [{"channel": "doc", "principal": "source-a"}]


def test_freeze_is_detached_and_uses_actual_scoped_record():
    writer = FakeWriter()
    writer.remember("Actual text", meta={"nested": ["original"]})
    supplied = {"id": "new-record", "text": "forged substitute"}
    frozen = freeze_record(writer, supplied)
    writer.items[0]["meta"]["nested"].append("later edit")
    assert frozen["fact_record"]["text"] == "Actual text"
    assert frozen["fact_record"]["writer_metadata"]["meta"]["nested"] == ["original"]
    with pytest.raises(ProducerError):
        freeze_record(writer, {"id": "other-tenant-record"})


def test_sources_deduped_only_from_visible_links():
    writer = FakeWriter()
    writer.items = [
        {
            "id": "a",
            "text": "fact",
            "ts": 3,
            "status": "active",
            "source": {"principal": "same"},
            "links": ["b", "c", "missing"],
        },
        {"id": "b", "source": {"principal": "same"}},
        {"id": "c", "source": {"principal": "another", "channel": "tool"}},
    ]
    fact = freeze_record(writer, writer.items[0])["fact_record"]
    assert fact["corroboration_count"] == 2
    assert fact["sources"] == [
        {"channel": "unknown", "principal": "same"},
        {"channel": "tool", "principal": "another"},
    ]


def test_source_flush_failure_never_queues(outbox):
    writer = FakeWriter()

    def failed():
        raise OSError("synthetic persist failure")

    writer.flush = failed
    with pytest.raises(OSError):
        remember_and_enqueue(writer, outbox, "fact")
    assert len(writer.items) == 1  # Explicit source/outbox atomicity limitation.
    assert not outbox.counts()


def test_source_success_outbox_failure_is_not_reported_atomic(tmp_path):
    box = DurableOutbox(tmp_path / "queue.db", "https://example.org", max_pending=1)
    first = FakeWriter()
    remember_and_enqueue(first, box, "first")
    second = FakeWriter()
    with pytest.raises(SourceConflict):
        remember_and_enqueue(second, box, "conflicting same source id")
    assert second.flushed and len(second.items) == 1
