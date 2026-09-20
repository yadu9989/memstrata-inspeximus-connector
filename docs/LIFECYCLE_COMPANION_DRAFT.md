# Lifecycle companion proposal for the Inspeximus pilot

MemStrata-side proposal, September 20, 2026. Awaiting Rastislav's agreement.
This is a technical research plan, not a service contract, legal retention policy
or production erasure API. `schema_v0` and connector 0.1.1 are unchanged.

## Stable source slots

Treat each source position as immutable within a frozen payload. Do not compact
`sources[]` after partial erasure or silently renumber an association. A negotiated
lifecycle projection retains the position as a tombstone; an erased slot must not
retain its principal, document handle or raw source object. Its associated writer
record identifiers are covered content too.

The tombstone belongs to the **companion projection**, not to a rewritten queued
`schema_v0` request. Sending a tombstone where schema_v0 requires a source object
would change the existing contract. Current queues continue retrying their saved
bytes; any real lifecycle adapter must fence sending before it purges those bytes.
If a whole projection/scope is retired, remove the covered projection rather than
reusing its old positions for a different set of sources.

## Name every retained source copy

At minimum, a provenance-aware erasure inventory must cover:

- `fact_record.sources[*]`;
- `fact_record.writer_metadata.source`;
- `fact_record.writer_metadata.source_provenance.associations[*].source`;
- affected `writer_record_id` and other subject-linked identifiers;
- any copy in original source storage, sender outboxes (including delivered rows),
  receiver payload/history, evidence projections, jobs, caches, logs, indexes,
  vectors and explicitly named backups.

This list is a lower bound. Unknown metadata can contain another copy, and text can
itself identify the source. A hard-coded list of these three JSON paths is not a
universal eraser. Require explicit lineage/coverage and independent verification;
missing adapters, unknown in-flight outcomes or unverified copies remain incomplete.
The offline marker helper scans keys and values in the entire supplied fixture so
an extra metadata echo is a failing control, not silently ignored.

## Bound the replay-refusal promise

Propose a **30-day maximum** subject-linked HMAC retention window for the synthetic
lifecycle pilot, starting when its suppression operation is committed. Access and
retries do not extend it. This window is a proposal to agree and implement, not an
already-running production policy or a claim about legal erasure deadlines.
An earlier delivery-scope expiry wins; this proposal does not extend the current
pilot's October 8 authorization window. An unmet prerequisite is reported as an
incomplete operation, never as successful erasure simply because time has elapsed.

Before destroying that scope's key and subject-linked refusal identifiers, retire
the delivery scope, revoke its credentials and settle in-flight deliveries. If
identifier erasure is required sooner, retire the scope sooner. Do not keep an
expired subject identifier merely to preserve an indefinite replay guarantee.
Backups and restoration must respect scope retirement; stale credentials must not
be reactivated by restoring an old outbox or receiver backup.

There are two different outcomes after retirement:

1. Requests using the old scope/credentials are rejected by authorization. This
   needs no retained subject-linked content fingerprint.
2. Under a separately authorized **new** scope, the old keyed identifier is gone.
   We cannot recognize an equivalent record as a replay of erased content. Normal
   admission rules apply; a cross-scope or beyond-window refusal is not promised.

Any retained scope revocation marker should be an independently generated scope
identifier, with no remaining subject mapping. Do not call metadata anonymous
without examining logs, receipts and other joinable copies. Actual key destruction,
credential revocation and retention enforcement require real adapters and evidence;
the offline reference does not yet implement them.

## Lifecycle meanings and verification

Supersession retires an old value from current use while retaining history.
Retraction withdraws a record from current evidence and affected derivations;
it does not silently revive an earlier value. Erasure removes covered content from
history too. Neither a tombstone nor a reported deletion count proves erasure.

The separate lifecycle capability must bind authorization, operation ID, complete
target inventory, verifier version and verification inputs. A retry cannot weaken
those inputs to make a failing operation pass. Real adapters need to expose evidence
for both the successful and deliberately failing cases, including WAL and in-flight
delivery behavior. The offline reference uses owned SQLite fixtures, not distributed
transactions, customer databases, real embeddings or real backup systems.

## Compatibility note

Connector 0.1.1's 65,536-byte limit already applies during `freeze_record` and during
enqueue. Link count and annotation size affect whether freezing succeeds. A snapshot
that once froze and failed only at enqueue can now fail at freeze. No truncation,
implicit enqueue success or mutation of old outbox bytes is permitted.
