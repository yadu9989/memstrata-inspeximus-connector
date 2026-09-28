"""Independent verifier for owned synthetic fixture files; never an adapter callback.

The caller derives the registered filename from the target label and owned
workspace. This is a single-process research verifier, not an OS security boundary.
"""
import sqlite3
from pathlib import Path

SUFFIXES = ("", "-wal", "-shm", "-journal")


def checked_path(root, path):
    root, path = Path(root).resolve(), Path(path)
    if path.parent != root or path.is_symlink() or path.resolve() != path:
        raise RuntimeError("fixture_verifier_path_escape")
    if path.exists() and (not path.is_file() or path.stat().st_nlink != 1):
        raise RuntimeError("fixture_verifier_not_a_private_file")
    return path


def byte_scan(root, database, values):
    database = checked_path(root, database)
    if not database.is_file():
        raise RuntimeError("fixture_verifier_database_missing")
    if not values:
        return {"status": "not_checked", "matched": None, "files": []}
    files, hits, matched = [], [False] * len(values), 0
    for suffix in SUFFIXES:
        path = checked_path(root, Path(str(database) + suffix))
        if path.exists():
            raw = path.read_bytes()  # Exceptions fail closed at the coordinator.
            present = [v.encode("utf-8") in raw for v in values]
            matched += sum(present)
            hits = [a or b for a, b in zip(hits, present)]
            files.append({"name": path.name, "bytes": len(raw)})
    return {"status": "checked", "matched": matched, "files": files, "probe_hits": hits}


def inspect_rows(root, database, capability, scope_key, record_key=None):
    database = checked_path(root, database)
    for suffix in SUFFIXES[1:]:
        checked_path(root, Path(str(database) + suffix))
    con = sqlite3.connect(database.as_uri() + "?mode=ro", uri=True, timeout=0.2)
    try:
        if con.execute("SELECT value FROM fixture_marker").fetchall() != [(capability,)]:
            raise RuntimeError("fixture_verifier_marker_mismatch")
        where, args = "scope_key=?", [scope_key]
        if record_key is not None:
            where += " AND record_key=?"
            args.append(record_key)
        rows = con.execute("SELECT payload,current FROM records WHERE " + where, args).fetchall()
        return {"rows": len(rows), "current": sum(r[1] for r in rows)}, [r[0] for r in rows]
    finally:
        con.close()
