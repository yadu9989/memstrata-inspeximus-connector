# Source provenance contract

The connector preserves each declared source reference in `fact_record.writer_metadata.source_provenance`. This optional annotation uses `source_provenance_v1`; the envelope remains `schema_v0`.

A document reference and a principal describe different things. The principal may name the person, team or tool behind a record. The document identifies the material they cited. Keeping both lets a receiving application show the actual document without treating every document as a separate independent witness.

## Stored associations

The annotation contains an `associations` array in primary record order, followed by the record's distinct visible links in their original order. Each entry has:

| Field | Meaning |
| --- | --- |
| `role` | `primary` for the selected writer record, or `linked` for a visible supporting record. |
| `writer_record_id` | The ID of the record that supplied this source. |
| `identity_kind` | The field selected for the compact source label: `principal`, `document`, `id`, `opaque`, or `absent`. |
| `source_index` | A zero based index into `fact_record.sources`, or null when no source label exists. |
| `source` | The original JSON source value, including its document handle and any custom annotations. |

The annotation also declares `identity_rule="principal_then_doc_then_id_else_opaque"` and `independence="unverified"`. The field selection follows the first nonempty value in that rule. For an object with no such field, the compact label uses its JSON representation. A scalar source becomes an opaque string label. A missing source produces no association. An explicitly empty or null source is retained with `identity_kind="absent"`.

Example with two documents from the same team:

```json
{
  "sources": [
    {"channel": "unknown", "principal": "billing-team"}
  ],
  "corroboration_count": 1,
  "writer_metadata": {
    "source": {
      "principal": "billing-team",
      "doc": "synthetic-architecture#authentication"
    },
    "source_provenance": {
      "version": "source_provenance_v1",
      "identity_rule": "principal_then_doc_then_id_else_opaque",
      "independence": "unverified",
      "associations": [
        {
          "role": "primary",
          "writer_record_id": "synthetic-primary",
          "identity_kind": "principal",
          "source_index": 0,
          "source": {
            "principal": "billing-team",
            "doc": "synthetic-architecture#authentication"
          }
        },
        {
          "role": "linked",
          "writer_record_id": "synthetic-support",
          "identity_kind": "principal",
          "source_index": 0,
          "source": {
            "principal": "billing-team",
            "doc": "synthetic-runbook#tokens"
          }
        }
      ]
    }
  }
}
```

This example is a fragment of a fact record, not a complete request.

## Lifecycle interpretation of positions

Positions refer to the immutable source array in that frozen payload. A consumer
must not compact the array after partial erasure while preserving old association
indices. The proposed lifecycle companion uses stable tombstoned positions in a
separate projection. It does not rewrite queued schema_v0 envelopes or add an
unnegotiated tombstone type to their wire format. See
[the lifecycle proposal](LIFECYCLE_COMPANION_DRAFT.md) for source-copy coverage and
the bounded replay-refusal proposal. Current connector 0.1.1 does not implement erasure.

## Compact source labels and counts

`sources[].principal` keeps the existing selection order: explicit principal, document, ID, then an opaque representation. An explicit `channel` is preserved as the writer supplied it. Without an explicit channel, the connector sets `channel="doc"` only when the document itself supplied the compact identity. If an explicit principal supplied that identity, an accompanying document does not make it a document identity; the channel is `unknown`.

Applications should use `identity_kind` and the preserved raw `source` to present source details. Even an explicit channel is a writer declaration, not a verified classification.

Compact labels are deduplicated by their selected principal string. Two visible records from the same principal keep both document associations but count once in `corroboration_count`. Repeating a link or linking a record to itself does not add another association. The connector does not resolve aliases across different strings or prove that different principals are independent. A consumer must not turn this count into verified confidence.

## Scope and limits

The connector reads only the caller's `store.items` snapshot. It resolves one level of visible links, does not access an underlying unscoped store, and does not export unresolved link IDs through the provenance annotation. Linked record text and unrelated linked metadata are not exported. The primary record's existing metadata allowlist remains unchanged.

A source URL or file handle is stored as data. The connector does not fetch the document, open a path or execute an instruction from a source annotation. Those annotations cannot select the receiver's tenant or change the fact's status.

All source annotations count toward the existing 65,536 byte request limit. Both freezing and enqueueing reject an oversized envelope without truncating its provenance. Invalid JSON values, nonfinite numbers and invalid Unicode are rejected. Applications must not treat a rejected snapshot as a successful enqueue. Source and outbox writes remain separate transactions.

## Existing outboxes and adoption

This annotation and the corrected inferred channel change the bytes of a newly frozen payload. An existing source ID must keep the bytes already saved in its outbox. Installing the updated connector does not rewrite queued records, delivered records or their digests.

Retry saved bytes for existing IDs. Do not regenerate them to add provenance, remove their queue records, or create a new ID solely to avoid a conflict. Agree a separate correction protocol if an already accepted record needs more provenance. For the next collaboration test, use a fresh synthetic record ID and compare the full document associations in the receiver's saved payload.

The receiving service can preserve the optional annotation under its existing unknown annotation policy. Durable receipt acceptance alone does not establish that a UI, reader or downstream export displays the new fields. Verify that presentation separately.

