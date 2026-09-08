# schema_v0 delivery contract

This document describes the connector's contract with the MemStrata pilot receiver. The receiver is a separate service and is not included in this repository.

## Request

Send one UTF-8 JSON object with `POST /mnemo/v0` over HTTPS. The body limit is 65,536 bytes. Use `Content-Type: application/json` and `Authorization: Bearer <application token>`. Deployments protected by Cloudflare Access also require the privately supplied `CF-Access-Client-Id` and `CF-Access-Client-Secret` headers. Authorization determines the receiving scope; a field in the payload cannot choose another tenant, disk path or project.

The envelope has two required fields:

```json
{
  "version": "schema_v0",
  "fact_record": {
    "id": "synthetic-before-001",
    "valid_from": 1782700000.0,
    "recorded_at": 1782700000.4,
    "key": "example-billing::auth-method",
    "subject": "example-billing",
    "relation": "auth-method",
    "object": null,
    "text": "The example billing service authenticates with API keys.",
    "sources": [{"channel": "doc", "principal": "synthetic-runbook"}],
    "corroboration_count": 1,
    "mtype": "semantic",
    "status": "active"
  }
}
```

Within `fact_record`, `id`, `valid_from`, `recorded_at`, `text`, `sources`, `corroboration_count` and `status` are required. Timestamps are finite Unix epoch seconds, not ISO date strings. Boolean values are not timestamps. IDs are nonempty strings of at most 512 characters. The receiver accepts text up to 32,768 characters and at most 256 source entries.

When present, `key` has one `::` separator and two nonempty components. `subject` and `relation` must match those components exactly. Without a key, subject and relation must be absent or null. Such a record has its own source identity and cannot supersede a separately identified record by subject and relation.

An explicit `object` is a JSON scalar: string, finite number or boolean. When `object` is missing or null, the full, exact `text` becomes the value. No model extracts a shorter object. Unknown annotations are preserved by the receiving service but do not become authorization or executable instructions.

Each source entry has string `channel` and `principal` fields. Source labels, `corroboration_count` and `effective_value` describe what the producer reports. They do not establish independently verified source identity or confidence. The receiver owns its actual transaction timestamp and preserves the producer's `recorded_at` separately.

The wire schema permits `active`, `superseded` and `hub` statuses. This pilot accepts only `active` for a new ledger write. Other statuses are rejected pending an agreed lifecycle contract. Future valid times, older valid times behind an existing current key, and conflicting values at the same valid time are rejected. Do not interpret a producer's local status or a missing snapshot record as a deletion command.

## Content identity

The sender serializes the envelope once with Python `json.dumps` using `sort_keys=True`, `separators=(",", ":")`, `ensure_ascii=False` and `allow_nan=False`, then encodes it as UTF-8. It retains those exact bytes for retries. The content digest is lowercase SHA-256 of those bytes.

This serialization is a shared Python format, not RFC 8785 JSON Canonicalization Scheme. A client in another language must reproduce its bytes, including number representation, or agree a different versioned scheme first. A schema_v0 source ID is immutable within its receiving scope. Recomputing a time-dependent `effective_value` before a retry changes the payload and causes a conflict.

The receiver computes identity after parsing and serializing with the same rules. It rejects duplicate JSON object keys, nonfinite numbers, invalid UTF-8 and excessive nesting. The sender's preflight checks cover basic shape; the receiver performs additional validation.

## Acknowledgement

A successful response is HTTP 200 with a bare JSON object. There is no enclosing `ack` field:

```json
{
  "source_id": "synthetic-before-001",
  "ledger_record_id": "example-ledger-id",
  "transaction_id": "example-transaction-id",
  "status": "committed",
  "committed_at": 1782700002.0,
  "payload_sha256": "<64 lowercase hexadecimal characters>"
}
```

`payload_sha256` is the bridge's content-binding addition to the original acknowledgement shape. The client requires the matching source ID and digest, `status="committed"`, nonempty ledger and transaction IDs, and a finite numeric `committed_at`. HTTP 201 is also accepted for a future compatible receiver. HTTP 200 alone does not complete delivery. The acknowledgement size limit is 16,384 bytes.

The receiving service commits the original payload, ledger decision, acknowledgement and indexing job in one transaction. The committed receipt confirms that transaction. It does not confirm completed embedding, searchable evidence, a correct model answer or the writer's independent source claims. Indexing readiness is reported separately by the receiver operator.

Replaying an identical record returns its original receipt. A replay of an older, already accepted source ID must not make its evidence current again. Changed content under the same ID returns HTTP 409. A newly generated source ID is required for a genuinely new write; it is not a workaround for validation failure.

Rejected responses use `status="rejected"` and a machine-readable `reason`. An admission failure can have empty ledger and transaction IDs and is not a durable receipt. A recorded business-rule rejection can have stable IDs and a commit timestamp, but still must never be treated as an accepted fact. The client stops automatic retries on HTTP 409 and other permanent client errors.

## Receiver limitations and first test

Use a fresh, agreed test namespace. Submit the synthetic before fixture, then after fixture, then retry the original before bytes. Confirm both receipts, check the receiver's indexing status, and query the current value only once indexing is ready. Restart the client and repeat the retry check. Never publish credentials with the results.

Source revisions, retractions, erasure, deletion propagation, arbitrary historical interval insertion and reconciliation after upstream data loss are outside schema_v0 pilot support. The optional Python wrapper is not an MCP hook or a subscription to all agent writes.

Upstream contract: [Agora schema_v0](https://github.com/DanceNitra/agora/blob/d7431a5319a84be8acb79db2b6305ae1035ca3d9/research/schema_v0.json).
Reference emitter: [schema_v0_emit.py](https://github.com/DanceNitra/agora/blob/d7431a5319a84be8acb79db2b6305ae1035ca3d9/research/probes/schema_v0_emit.py).
