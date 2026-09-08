"""Send an approved schema_v0 fixture with a bounded worker loop."""

import argparse
import json
import os
import time
from pathlib import Path

from memstrata_mnemo_connector import DeliveryCredentials, DurableOutbox


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("payload", type=Path)
    parser.add_argument("--outbox", type=Path, default=Path("outbox.sqlite3"))
    parser.add_argument("--seconds", type=float, default=60)
    args = parser.parse_args()
    if not 1 <= args.seconds <= 3600:
        parser.error("--seconds must be between 1 and 3600")
    endpoint = os.environ["MEMSTRATA_BRIDGE_URL"]
    credentials = DeliveryCredentials(
        bearer_token=os.environ["MEMSTRATA_BRIDGE_TOKEN"],
        access_client_id=os.environ.get("CF_ACCESS_CLIENT_ID"),
        access_client_secret=os.environ.get("CF_ACCESS_CLIENT_SECRET"),
    )
    payload = json.loads(args.payload.read_text(encoding="utf-8"))
    outbox = DurableOutbox(args.outbox, endpoint)
    source_id = outbox.enqueue(payload)
    deadline = time.monotonic() + args.seconds
    while time.monotonic() < deadline:
        result = outbox.deliver_next(credentials=credentials)
        if result is not None:
            print(result)
        status = outbox.status(source_id)
        if status["state"] == "delivered":
            return 0
        if status["state"] in ("rejected", "exhausted"):
            return 2
        time.sleep(min(1, max(0, deadline - time.monotonic())))
    print("Delivery is still pending in the persistent outbox.")
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
