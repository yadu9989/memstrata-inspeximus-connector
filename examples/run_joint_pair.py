"""Run the approved SYNTHETIC pair, exact replays and same-ID conflict.

No credentials in arguments or output. Uses the released outbox's transport,
TLS, no-redirect policy, canonical serialization and receipt validation.
Stored result is not proof of indexing/current retrieval: operator verification
is a separate exit gate. On unknown/transient outcomes, stop; inspect and resume
the same run directory, never regenerate or delete its outboxes.
"""
import argparse
import copy
import hashlib
import json
import os
import sqlite3
from pathlib import Path

from memstrata_mnemo_connector import DeliveryCredentials, DurableOutbox, canonical_bytes


def verify_bundle(bundle):
    manifest = json.loads((bundle / "fixture.json").read_bytes())
    if manifest.get("synthetic_only") is not True:
        raise ValueError("synthetic_fixture_required")
    payloads = {}
    if {x["file"] for x in manifest["files"]} != {"before.json", "after.json"}:
        raise ValueError("wrong_fixture_inventory")
    for entry in manifest["files"]:
        name = entry["file"]; raw = (bundle / name).read_bytes()
        if hashlib.sha256(raw).hexdigest() != entry["sha256"]:
            raise ValueError("fixture_hash_mismatch")
        payload = json.loads(raw)
        if canonical_bytes(payload) != raw or payload["fact_record"]["id"] != entry["source_id"]:
            raise ValueError("fixture_identity_mismatch")
        fact = payload["fact_record"]
        if payload["version"] != "schema_v0" or not fact["id"].startswith(manifest["namespace"]):
            raise ValueError("fixture_scope_mismatch")
        associations = fact["writer_metadata"]["source_provenance"]["associations"]
        if len(fact["sources"]) != 1 or fact["corroboration_count"] != 1 or len(associations) != 2:
            raise ValueError("fixture_provenance_mismatch")
        if [x["role"] for x in associations] != ["primary", "linked"]:
            raise ValueError("fixture_roles_mismatch")
        if any(x["source_index"] != 0 for x in associations):
            raise ValueError("fixture_source_positions_mismatch")
        payloads[name[:-5]] = payload
    a, b = (payloads[n]["fact_record"] for n in ("before", "after"))
    if a["id"] == b["id"] or a["key"] != b["key"] or a["valid_from"] >= b["valid_from"]:
        raise ValueError("invalid_pair_order")
    return manifest, payloads


def saved_ack(database, source_id):
    # Read only the documented sanitized receipt retained by the public client.
    with sqlite3.connect(database.resolve().as_uri() + "?mode=ro", uri=True) as con:
        value = con.execute("SELECT ack_json FROM producer_outbox WHERE source_id=?", (source_id,)).fetchone()
    return json.loads(value[0]) if value and value[0] else None


def run(bundle, destination, endpoint, credentials, *, loopback=False):
    manifest, payloads = verify_bundle(bundle)
    destination.mkdir(parents=True, exist_ok=True)
    binding = {"fixture_sha256": hashlib.sha256((bundle/"fixture.json").read_bytes()).hexdigest(),
               "endpoint_sha256": hashlib.sha256(endpoint.encode()).hexdigest()}
    binding_file = destination/"binding.json"
    if binding_file.exists():
        if json.loads(binding_file.read_bytes()) != binding: raise ValueError("run_binding_mismatch")
    else:
        with binding_file.open("x", encoding="utf-8") as f: json.dump(binding, f)
    changed = copy.deepcopy(payloads["before"])
    changed["fact_record"]["text"] += " SYNTHETIC-CONFLICT-DO-NOT-ACCEPT"
    steps = [("before", payloads["before"]), ("after", payloads["after"]),
             ("old_retry", payloads["before"]), ("old_retry_reopened", payloads["before"]),
             ("changed_same_id", changed)]
    evidence = {}
    for name, payload in steps:
        database = destination/(name+".sqlite3")
        queue = DurableOutbox(database, endpoint, allow_loopback_http=loopback)
        source_id = queue.enqueue(payload)
        state = queue.status(source_id)
        if state["state"] not in ("delivered", "rejected", "exhausted"):
            queue.deliver_next(credentials=credentials)
        state = queue.status(source_id)
        evidence[name] = {"source_id": source_id, "state": state["state"],
                         "payload_sha256": state["payload_sha256"], "http_status": state["http_status"],
                         "reason": state["reason"], "ack": saved_ack(database, source_id)}
        if name == "changed_same_id":
            if state["state"] != "rejected" or state["http_status"] != 409:
                raise RuntimeError("expected_receiver_conflict_not_observed")
        elif state["state"] != "delivered":
            raise RuntimeError("delivery_not_complete_resume_same_run_after_inspection")
    original = evidence["before"]["ack"]
    if any(evidence[name]["ack"] != original for name in ("old_retry", "old_retry_reopened")):
        raise RuntimeError("replay_receipt_changed")
    if original["transaction_id"] == evidence["after"]["ack"]["transaction_id"]:
        raise RuntimeError("pair_did_not_create_distinct_transactions")
    # Outbox already-delivered reopen must not send again.
    reopened = DurableOutbox(destination/"before.sqlite3", endpoint, allow_loopback_http=loopback)
    if reopened.deliver_next(credentials=credentials) is not None:
        raise RuntimeError("delivered_outbox_resent")
    result = {"synthetic_only": True, "namespace": manifest["namespace"], "steps": evidence,
              "delivery_checks_passed": True, "indexing_and_current_evidence": "PENDING_OPERATOR_CHECK",
              "joint_test_complete": False}
    result_file = destination/"result.json"
    raw = json.dumps(result, sort_keys=True, indent=2)+"\n"
    if result_file.exists() and result_file.read_text(encoding="utf-8") != raw:
        raise RuntimeError("saved_result_changed")
    if not result_file.exists():
        with result_file.open("x", encoding="utf-8") as f: f.write(raw)
    return result


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("bundle", type=Path)
    ap.add_argument("--out", type=Path, required=True)
    args = ap.parse_args()
    try:
        result = run(args.bundle, args.out, os.environ["MEMSTRATA_BRIDGE_URL"],
                     DeliveryCredentials(os.environ["MEMSTRATA_BRIDGE_TOKEN"],
                         os.environ.get("CF_ACCESS_CLIENT_ID"), os.environ.get("CF_ACCESS_CLIENT_SECRET")))
        print(json.dumps(result, indent=2))
    except Exception as exc:
        # Do not print URLs, credentials, bodies or arbitrary exception messages.
        print(json.dumps({"complete": False, "error_class": type(exc).__name__,
                          "next": "Inspect local run; preserve files and credentials. No result implies no completion."}))
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
