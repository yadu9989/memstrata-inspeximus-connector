# Inspeximus / MemStrata synthetic test packet

Prepared for Rastislav Drahos (DanceNitra), September 28, 2026. Synthetic data only.
The MIT connector runtime remains 0.1.1; no engine, model or credentials are bundled.

## Frozen data and schema

- [before.json](../fixtures/joint-rastislav-20260928/before.json)
- [after.json](../fixtures/joint-rastislav-20260928/after.json)
- [manifest](../fixtures/joint-rastislav-20260928/fixture.json)
- [machine-readable fixture profile](../fixtures/joint-pair-profile.schema.json)
- [HTTP and acknowledgement contract](API_CONTRACT.md)
- [unchanged upstream schema_v0, pinned](https://github.com/DanceNitra/agora/blob/d7431a5319a84be8acb79db2b6305ae1035ca3d9/research/schema_v0.json)

The profile is a narrower test schema, not a replacement of upstream schema_v0.
Reserved namespace: `synthetic-joint-7ac34ce17bc1489aabc2805df3a95ae2`.
Our local live smoke test must use different IDs; this pair is reserved for you.

| File | Bytes | SHA-256 |
| --- | ---: | --- |
| before.json | 1354 | ade1e66ffffa30ff871eb36e4db91adc7444dfe8b233b621ecba34b71c0b3521 |
| after.json | 1365 | 090259b20a5613020da156ab2177301f793789aeb6478d76fc1d0e6a17f30d07 |

The fictional service changes from "The fictional billing service uses API keys."
to "The fictional billing service uses signed short-lived tokens." Both use
`object: null`: the full exact text is the value. The after valid time is later;
both times are in the past.

Each payload has a primary document and linked runbook with distinct document
handles, sharing principal `synthetic-billing-team`. Expect two preserved
associations, both source_index 0, one compact source with channel `unknown`,
and corroboration_count 1. These are declarations, not independently verified sources.

## Run the pair

Create a virtual environment; install the local client with `python -m pip install .`.
No private engine, model or Inspeximus installation is required for the frozen pair.
Use privately supplied pilot credentials in your process environment, never logs
or command-line arguments:

- MEMSTRATA_BRIDGE_URL = https://mnemo-ingest.memstrata.dev
- MEMSTRATA_BRIDGE_TOKEN = private pilot application bearer
- CF_ACCESS_CLIENT_ID = private pilot Access client ID
- CF_ACCESS_CLIENT_SECRET = private pilot Access client secret

The URL alone grants no access. Never use a tunnel token or management credential.
Credentials are NOT in this packet.

```console
python examples/run_joint_pair.py fixtures/joint-rastislav-20260928 --out joint-pair-result
```

The five requests are before, after, exact old replay, another old replay from a
separate client instance, and changed content under the before ID. The last must
return HTTP 409. The two writes must have distinct transaction IDs. Both old replays
must return the original receipt. A delivered outbox must not resend when reopened.

The runner checks hashes before dispatch, preserves SQLite outboxes and sanitized
receipts, and resumes completed steps without re-sending. Unknown/unexpected outcomes
stop. Inspect before rerunning the SAME command. Do not delete the run directory,
regenerate IDs or modify saved bytes to make a failure pass. Changed endpoint or
fixture bindings under an existing run directory are refused.

Return `joint-pair-result/result.json` privately with your Python/OS versions.
Do not send outboxes, credentials, headers, raw response bodies or shell history.
Neeraj then compares stored associations, confirms indexing readiness, and checks
that current evidence remains the after value after the old replay. Until that
operator check, the runner deliberately records `joint_test_complete=false`.
Durable commit is not indexing readiness or answer accuracy.

Use the query "What authentication method does the fictional billing service use?"
The opaque record ID is metadata, not source text. A query using that ID did not
retrieve the source in the operator check. Compare returned source IDs with the
manifest; matching text from another fixture does not prove this pair was retrieved.

## Lifecycle regression, no credentials needed

```console
python research/run_lifecycle_probes.py
python -m pip install ".[dev]"
python -m pytest -q tests research
```

The new capability is `inspeximus-lifecycle-experimental-v2`. Expected result:
`lying_byte_probe.complete=false`, `independent_markers_present=true`, and
`adapter_mismatch_detected=true`. Honest deletion must still complete. Non-deletion,
unreadable/missing files, absent marker calibration and unresolved WAL readers cannot.

Your existing upstream probe can target this checkout too. Record the actual
new tag/commit beside its output because its hard-coded label still says v0.1.
Its `a_lying_byte_probe_PASSES_while_the_bytes_remain` diagnostic should be false.

The coordinator derives registered paths from the owned workspace and target labels,
independently reads rows/files and calibrates markers, then compares adapter claims.
This single-process fixture is not a sandbox for hostile Python, hardware erasure,
or proof of coverage of unregistered backups.

## Compatibility and remaining scope

Inspeximus names apply to new capabilities/paths. Existing
`memstrata_mnemo_connector`, `POST /mnemo/v0` and frozen schema_v0 bytes are preserved.
The old reference tag remains available; new verifier semantics require fresh
synthetic workspaces rather than reinterpretation of old operation stores.

The September 21 agreement establishes design boundaries: a 30-day maximum from
suppression, no extension by retries, earlier scope expiry, scope/credential
retirement and in-flight accounting before destroying keys/identifiers, and no
cross-scope replay promise. Production retention/revocation and live erasure are
not implemented or enabled by this update.

Pilot expiry remains October 8, 2026, 02:21:52 UTC. No access extension is granted.
The Governance recording follows separately and has not been recorded yet.
Joint completion requires your actual run and receipt comparison; no endorsement
or commercial agreement is implied.
