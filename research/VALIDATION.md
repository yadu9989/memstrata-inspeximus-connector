# Reference validation, September 20, 2026

Local command: `python -m pytest -q tests research`.
Result: **154 passed**. The existing 111 connector tests still pass unchanged.
Runtime: Python 3.13.14, SQLite 3.50.4 on Windows.

The standalone probe receipt includes the code hashes and these controls:

| Case | Observation |
| --- | --- |
| Adapter reports two erased, deletes nothing | Incomplete |
| Adapter reports zero erased, deletes the covered fixture | Complete for the named synthetic inventory |
| Retry replaces the verification marker | Rejected as changed operation |
| Remove first source by compacting | Surviving B association incorrectly resolves to C |
| Reserve first source position as tombstone | Surviving B association still resolves to B |

Other tests cover a linked runbook identity surviving a legacy-only cleanup,
unknown metadata copies, shared principals, unchanged frozen input bytes, denied
authorization, omitted stores, recovery between purges, in-flight delivery fences,
and ordinary versus held-open WAL connections. The joint fixture generator also
refuses overwriting a fixture directory or changing an existing queued source ID.

These are controlled tests of a reference implementation. They do not establish
production erasure, distributed cancellation, actual vector/backup integration,
cryptographic key destruction, legal compliance, partner-side execution or the
behavior of every SQLite version. In particular, a successful fixture run does not
qualify this SQLite build for production concurrent-WAL use; review current SQLite
release advisories for the deployment being evaluated.

The shipped connector runtime and version remain 0.1.1. Research files are outside
its explicit wheel package whitelist. No private receiver/engine, credential,
customer database, model or live deletion endpoint is included in this reference.
