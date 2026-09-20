# MemStrata Inspeximus connector

The [offline lifecycle reference](research/README.md) and
[joint technical pilot checklist](docs/JOINT_PILOT.md) are available for peer testing.
They do not change connector 0.1.1, enable a deletion endpoint or modify schema_v0.

A small Python client that sends Inspeximus fact records to a separately operated MemStrata receiver. It saves each outgoing record in a local SQLite outbox before delivery, retries the same payload after temporary failures, and checks the acknowledgement against the source ID and payload hash.

Inspeximus, formerly mnemo, remains usable on its own. This connector is optional. It contains no MemStrata engine, receiver, retrieval code, model weights or service credentials. Access to a compatible receiver is arranged separately.

The client uses the Python standard library. Inspeximus is an optional dependency for the convenience wrapper. Python 3.10 or newer is required.

## Install

Install the small Python wheel from the `v0.1.1` GitHub release. This does not require Git:

```console
python -m pip install "https://github.com/yadu9989/memstrata-inspeximus-connector/releases/download/v0.1.1/memstrata_inspeximus_connector-0.1.1-py3-none-any.whl"
```

Or install from a reviewed checkout:

```console
python -m pip install .
```

To use the optional Inspeximus wrapper:

```console
python -m pip install ".[inspeximus]"
```

On Windows, `py -3` can replace `python` in these commands. No virtual environment activation is required if you call that environment's Python executable directly.

To install the tagged source with Git:

```console
python -m pip install "git+https://github.com/yadu9989/memstrata-inspeximus-connector.git@v0.1.1"
```

The GitHub command requires Git. A PyPI release is not implied by this repository.

Version 0.1.1 preserves document references from linked sources through the optional [source provenance annotation](docs/SOURCE_PROVENANCE.md). Existing outbox records keep their exact saved bytes. Do not regenerate an existing source ID to add the new annotation. See the [release notes](docs/RELEASE_NOTES_0.1.1.md).

## Send a record

Start with synthetic records or data you have permission to transfer. Obtain the endpoint and credentials privately from the receiver operator. Do not paste credentials into source files, issues or shared terminal output.

```python
import json
import os
from pathlib import Path

from memstrata_mnemo_connector import DeliveryCredentials, DurableOutbox

outbox = DurableOutbox(
    Path("outbox.sqlite3"),
    os.environ["MEMSTRATA_BRIDGE_URL"],
)
payload = json.loads(Path("fixtures/before.json").read_text(encoding="utf-8"))
source_id = outbox.enqueue(payload)

credentials = DeliveryCredentials(
    bearer_token=os.environ["MEMSTRATA_BRIDGE_TOKEN"],
    access_client_id=os.environ.get("CF_ACCESS_CLIENT_ID"),
    access_client_secret=os.environ.get("CF_ACCESS_CLIENT_SECRET"),
)
result = outbox.deliver_next(credentials=credentials)
print(result)
print(outbox.status(source_id))
```

`enqueue` does not contact the receiver. `deliver_next` attempts one due record and returns immediately when no record is due. It can block for the configured network timeout, which defaults to 20 seconds. Call it from your own worker or service loop, outside a latency-sensitive agent turn.

The example in `examples/send_payload.py` runs a bounded delivery loop. The two fixture files describe a fictional service changing its authentication method. They are test data, not product configuration advice. A receiver may reject an older fixture if that source ID or key has already been used. Arrange a fresh test namespace with the operator; never reset a real history to make a test pass.

## Use an Inspeximus writer

```python
from inspeximus import Inspeximus
from memstrata_mnemo_connector import DurableOutbox, remember_and_enqueue

writer = Inspeximus(path="inspeximus.json")
outbox = DurableOutbox("outbox.sqlite3", "https://memory.example.com")

source_id = remember_and_enqueue(
    writer,
    outbox,
    "The example billing service authenticates with signed requests.",
    key="example-billing::auth-method",
    object="signed requests",
    source={"doc": "synthetic-runbook"},
    mtype="semantic",
)
```

This wrapper calls `remember`, then `flush`, then snapshots the visible record into the outbox. It does not intercept MCP calls, attach to other running agents or subscribe to every Inspeximus write. The wrapper was developed against Inspeximus 2.27.0. Its optional `effective_value` annotation uses the writer's scoring method when present; this private upstream method may change.

Use a writer handle scoped to the records you intend to share. `freeze_record` only looks at records visible through that handle. It includes selected provenance and scope metadata, which can itself be sensitive. Review what you export.

## What a receipt means

The receiver returns a committed acknowledgement after its receiving ledger transaction is durable. Embedding and retrieval readiness can happen later. A `delivered` outbox state therefore does not mean the record is already searchable or that an answer has been verified.

An identical retry returns the original receipt. Changed content under the same source ID is a conflict. A new value needs a new source ID and a later valid time. The pilot receiver rejects conflicting late writes, future valid times, and unagreed lifecycle operations. Retractions and deletion propagation need a separate contract.

See [the API contract](docs/API_CONTRACT.md) for field mapping and acknowledgement checks.

## Failure handling

Temporary network failures, HTTP 408, 425, 429 and server errors are retried with bounded exponential backoff. Defaults are eight attempts, a two-second initial delay and a five-minute maximum delay. A numeric `Retry-After` on HTTP 429 is honored within that bound. HTTP-date retry headers are not parsed. Redirects are refused so credentials are not forwarded to another destination. Certificate verification remains enabled.

Authentication errors, validation failures and conflicts stop automatic retries. The result has `state="rejected"`. An exhausted retry budget has `state="exhausted"`. Repair the cause, inspect the record's status, and explicitly call `outbox.retry_failed(source_id)` when retrying the same payload is appropriate. That method cannot make a conflicting payload valid.

The receiver operator must configure its HTTPS gateway for authenticated API clients. An interactive browser challenge or a browser-only protection rule can reject a legitimate Python request before it reaches the receiver. Repair the narrowly scoped API policy with the operator; do not impersonate a browser or disable certificate checks.

The source store and outbox are separate databases. A crash after `writer.flush()` but before `enqueue()` can leave a durable source record that was never queued. Preserve the source ID and reconcile visible records after recovery. Do not blindly repeat `remember`, which may create another write. This package does not run automatic reconciliation and cannot recover an upstream record already erased before export.

The outbox stores source text and successful receipt fields in SQLite. It does not store credentials, response bodies or model vectors. Protect the database with suitable file permissions, encryption and retention rules. Pending capacity is bounded, but delivered records are retained, so disk use can grow over time. There is no automatic deletion policy.

## Test and build

```console
python -m pip install ".[dev]"
python -m pytest
python -m build
```

Tests use synthetic data and local HTTP servers. They do not require Inspeximus, a MemStrata installation, a model, an account or public network access. Passing them validates client behavior; it does not establish that a remote deployment is available.

## License and upstream work

This connector is MIT licensed. The mapping follows Agora's published `schema_v0` contract and reference emitter. [NOTICE](NOTICE) preserves the upstream attribution and license text. The MemStrata engine is distributed separately under its own terms. This repository grants no rights to that engine or to anyone's source data.
