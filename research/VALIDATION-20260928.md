# Independent verifier correction and paired-write kit

Windows, Python 3.13.14, SQLite 3.50.4, September 28, 2026.

Local full suite: `python -m pytest -q tests research`: **165 passed**.
Both published fixed payloads also validate against the supplied JSON Schema
using jsonschema in the operator's validation environment (not a runtime dependency).

The new tests include the adapter that lies about inspect, byte_probe and
calibration simultaneously; honest erase/retry controls; redirecting adapter.path
to a clean file; missing/unreadable files; independent WAL/SHM/journal coverage;
real loopback HTTP delivery/replay/conflict; restart without resending accepted
steps; tampered-fixture refusal; and fixture-directory overwrite refusal.
The existing held-open WAL, marker-binding and provenance tests continue to pass.

The standalone receipt at run_lifecycle_probes.result.json includes local code
hashes normalized from CRLF to LF so the same published source compares across
Windows/Linux/macOS checkouts. The dishonest byte probe now produces incomplete, with markers found by
the coordinator and adapter disagreement reported. Counts alone do not decide
the verdict. Honest deletion still completes for the registered synthetic files.

The source client modules and released version 0.1.1 are unchanged. Research
files are excluded from the runtime wheel package whitelist. This update does not
modify the private receiver, live database, credentials, scheduled task or tunnel,
enable live erasure, retire a real key, or extend the pilot's expiry.

The partner namespace was generated once and not sent to the live receiver by
these tests. Counterpart execution and the operator's post-indexing check remain
required. No commercial endorsement or joint completion is claimed.
