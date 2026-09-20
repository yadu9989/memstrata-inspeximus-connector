# Offline lifecycle reference

This is the standalone reference requested by Rastislav (DanceNitra) in
[Agora discussion 2](https://github.com/DanceNitra/agora/discussions/2#discussioncomment-18366258).
It is MIT-licensed research code, separate from the connector wheel, receiving
service and private MemStrata engine. It accepts no caller database path. Storage
is created inside owned temporary directories and contains synthetic fixtures only.

Run the standalone checks with Python 3.10 or newer:

```sh
python research/run_lifecycle_probes.py
```

The script writes `research/run_lifecycle_probes.result.json` beside itself and
prints the same receipt, including source hashes and Python/SQLite versions.
It demonstrates a receiver reporting two deletions while deleting nothing (must
remain incomplete), a receiver reporting zero while really deleting the covered
fixture (can complete), rejection of a weaker verification marker on retry, and
the provenance corruption caused by source-array compaction versus stable slots.

For the full suite, install the existing development extras and run:

```sh
python -m pip install ".[dev]"
python -m pytest -q tests research
```

The lifecycle tests also cover held-open WAL readers, ordinary connection closure,
restart, delivery races, false authorization claims, missing backup/outbox targets,
tenant isolation, changed operation IDs and failed physical marker checks.
The SQLite checks are exact synthetic UTF-8 marker searches in named database and
sidecar files. They do not prove SSD, snapshot, memory, FTS or real backup erasure.
Index/vector/backup adapters here are ordinary tables, not real product adapters.

`lifecycle_reference.py` contains `Coordinator`, `SQLiteTarget`, `SyntheticWorkspace`,
`Request`, and `Target`. `Coordinator.execute` binds the operation to its request,
principal, verifier and probe values. It commits a suppression fence first, waits
for known delivery outcomes, applies each named adapter and verifies state rather
than trusting a count. Its authorization callback is supplied by the test harness;
it is not an authentication service.

`provenance_projection.py` creates a separate source-slot projection. It does not
mutate queued `schema_v0` envelopes or claim to purge every source copy. An erased
slot carries no source text, principal or writer record ID. Its array position is
reserved for that projection's lifetime. Unmapped (`source_index: null`) references
remain explicit. A production eraser still needs complete lineage and store coverage.

Key retention and credential retirement are deliberately **not implemented** in
the coordinator. It currently refuses a changed key on reopen. The proposed bounded
policy is in [the lifecycle contract](../docs/LIFECYCLE_COMPANION_DRAFT.md); do not
invent a fresh key to bypass a persisted fence. These experiments do not enable
deletion or retraction on `POST /mnemo/v0`.

Credit: Rastislav's source-position and remaining-provenance observations, and his
count-versus-evidence and WAL probes, informed these checks. His `erasure_v0` remains
an upstream draft. This repository does not claim joint approval of a new protocol.
