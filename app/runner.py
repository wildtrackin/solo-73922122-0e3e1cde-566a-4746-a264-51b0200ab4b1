"""Isolated trial runner.

Executed in a *fresh* Python subprocess (``python -I``) for every single
trial so that:

* each trial gets its own in-memory DuckDB database (no state leakage);
* the 2 second wall-clock limit can be enforced by the parent killing the
  whole process group (DuckDB's native code cannot be interrupted safely
  from another thread);
* a crash inside DuckDB only takes the trial down, never the web server.

This script deliberately imports nothing from the application package: under
``python -I`` the project directory is not on ``sys.path``.  Only the standard
library and the installed ``duckdb`` package are required.

Protocol
--------
stdin  : one JSON message describing the trial (rows + deterministic SELECT).
stdout : exactly one JSON line, one of
         {"status": "success", "elapsed_ms": ...}
         {"status": "error", "error_type": "...", "first_line": "...",
          "elapsed_ms": ...}
stderr : diagnostics only (never parsed by the parent).
"""
from __future__ import annotations

import json
import sys
import time
import traceback


def main() -> int:
    payload = json.load(sys.stdin)
    rows = payload["rows"]
    sql = payload["sql"]

    # Imported inside main so an import failure still produces a JSON line.
    import duckdb

    started = time.monotonic()
    con = duckdb.connect(":memory:")
    try:
        con.execute(
            "CREATE TABLE readings ("
            "sample_id VARCHAR, item VARCHAR, value VARCHAR)"
        )
        if rows:
            con.executemany(
                "INSERT INTO readings VALUES (?, ?, ?)",
                rows,
            )
        con.execute(sql).fetchall()
    except Exception as exc:  # noqa: BLE001 - every DuckDB error is a trial result
        result = {
            "status": "error",
            "error_type": type(exc).__name__,
            "first_line": str(exc).splitlines()[0] if str(exc) else "",
            "elapsed_ms": round((time.monotonic() - started) * 1000, 1),
        }
    else:
        result = {
            "status": "success",
            "elapsed_ms": round((time.monotonic() - started) * 1000, 1),
        }
    finally:
        con.close()

    sys.stdout.write(json.dumps(result, ensure_ascii=False) + "\n")
    sys.stdout.flush()
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except SystemExit:
        # Normal interpreter exit (including exit code 0 from main()).
        raise
    except BaseException:  # pragma: no cover - defensive last resort
        traceback.print_exc()
        sys.stdout.write(
            json.dumps(
                {
                    "status": "runner_crash",
                    "error_type": "RunnerCrash",
                    "first_line": "trial runner crashed before producing a result",
                    "elapsed_ms": 0.0,
                }
            )
            + "\n"
        )
        sys.stdout.flush()
        raise SystemExit(2)
