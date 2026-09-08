"""Queue one synthetic Inspeximus write. This example makes no delivery request."""

import os

from inspeximus import Inspeximus

from memstrata_mnemo_connector import DurableOutbox, remember_and_enqueue


def main() -> None:
    writer = Inspeximus(path="inspeximus.json")
    outbox = DurableOutbox("outbox.sqlite3", os.environ["MEMSTRATA_BRIDGE_URL"])
    source_id = remember_and_enqueue(
        writer,
        outbox,
        "The example billing service authenticates with signed requests.",
        key="example-billing::auth-method",
        object="signed requests",
        source={"doc": "synthetic-runbook"},
        mtype="semantic",
    )
    print(outbox.status(source_id))


if __name__ == "__main__":
    main()
