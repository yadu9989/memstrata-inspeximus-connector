# Joint technical pilot checklist

Scope: an optional Inspeximus 2.27.0 / connector 0.1.1 write-to-receipt integration
and a Governance walkthrough using synthetic data. The discussion records agreement
to test the provenance annotation. Completion still requires the counterpart's
run and receipt comparison. Do not describe a commercial partnership as finalized
or announce joint endorsement from this checklist.

## Paired write

Agree one fresh synthetic namespace and fresh immutable IDs before sending.
Include a primary document and linked runbook, plus two documents sharing a principal
so preserved document references do not inflate `corroboration_count`.

From a checkout with connector 0.1.1 installed, create an unused local folder:

```sh
python examples/prepare_joint_fixture.py joint-fixture
```

This makes no network call and needs no credentials. It writes new synthetic IDs,
past valid times, two preserved document handles sharing one principal, and exact
payload hashes. It refuses to overwrite an existing fixture folder. Share the
synthetic manifest before delivery so the operator can recognize this test scope.

Freeze the original and later replacement once, retaining their exact bytes and
hashes. Send the original, then the replacement, then the exact original again.
Reopen the client outbox and repeat the old delivery. An old retry must return its
original receipt and must not make the old value current. Changed bytes under an
existing ID must be rejected. Keep valid times in the past, in the agreed order.

Compare both parties' sanitized receipts: source ID, payload digest, transaction
ID, ledger record ID and commit time. Compare the complete stored source associations,
then separately verify indexing readiness and the current evidence. Durable receipt
acceptance is not a search-readiness or answer-accuracy claim.

## Lifecycle review

Rastislav runs his erasure probes against `research/lifecycle_reference.py` and
compares positive and negative controls with the published local receipt. Settle
the stable-slot projection and proposed 30-day synthetic refusal window before
freezing a companion capability. Live lifecycle operations remain disabled until
both sides agree and real adapters are tested.

## Governance walkthrough

Use an isolated demo configuration, synthetic tools and synthetic arguments.
Demonstrate an allowed call, a refused call and the corresponding audit records.
Then test the configured service-unavailable behavior on the disposable demo
instance. Do not stop a live Governance service or alter a real policy for a demo.
No calendar slot is booked by this document; exchange a time zone and two suitable
times privately before confirming one.

## Private handoff

Confirm the private recipient through the existing DanceNitra discussion identity.
Coordinate at `support@memstrata.dev`. Share only the scoped pilot connection details
through an agreed protected channel. Never post credentials, cloud management
tokens, raw authentication headers or private engine code here.

The local pilot has an existing hard expiry of October 8, 2026, 02:21:52 UTC. Recheck
that expiry and authenticated health before handoff. Automatic process recovery
does not extend access. The longer-lived Cloudflare service credential alone must
not outlive the receiver's enforced authorization window.

## Exit criteria

Both parties retain successful paired-write receipts and source-association results;
old replay cannot restore old current evidence; agreed failures are rejected;
indexing/current evidence is checked separately; and the Governance demonstration
matches its documented policy. Publish only mutually reviewed observations. Source
licenses remain as published; no exclusivity, fees, equity or new IP rights are
created by this pilot plan.
