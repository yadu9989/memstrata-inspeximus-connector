# Version 0.1.1

This patch preserves source details that were lost when a record cited another visible record. Each primary and linked source now has an optional association in `writer_metadata.source_provenance`, including its original document handle and originating record ID.

An explicit principal no longer inherits an inferred document channel merely because a separate document handle is present. Explicit writer-supplied channel labels remain unchanged. Multiple documents attributed to the same principal retain their individual references without increasing the reported corroboration count.

The annotation uses `source_provenance_v1`; the write envelope remains `schema_v0`. The connector reads only records visible through the caller's scoped handle. It does not fetch source URLs or export linked record text. Oversized annotations are rejected within the existing 64 KiB request limit.

## Existing records

Existing outbox rows and their hashes are not migrated. Retry their saved bytes. Freezing a new, enriched envelope under an already queued source ID changes the payload and is correctly rejected as a conflict. Use fresh synthetic IDs to test the new annotation. An agreed correction protocol is required for an already accepted record.

## Verification and limits

The public connector suite passed 111 tests, including 19 source-provenance regressions. An isolated wheel installation passed import, source-preservation and immutable-ID checks without MemStrata or Inspeximus installed. Separate private integration tests exercised the sender, API and ledger through an in-process transport, including a lost acknowledgement and reopening both stores.

This patch does not add a withdrawal or erasure endpoint. The lifecycle reference remains an offline proposal. The package contains no MemStrata engine, receiver, retrieval code, model, database or service credential. No native Linux or macOS execution claim is made by the pure-Python wheel tag.
