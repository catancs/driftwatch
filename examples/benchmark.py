#!/usr/bin/env python3
"""Measure driftwatch at scale against a real Postgres and a real DuckDB warehouse.

For each table size it reports:
  - matching: how long to verify a fully in-sync table, and how many rows were read
  - sparse drift: how long to find a handful of bad rows, rows read, and whether the
    exact injected rows were found (correctness at scale)
  - naive baseline: time and rows to pull every row hash from both sides (the
    "transfer everything" approach driftwatch avoids), for sizes up to 1M

Run it with examples/benchmark.sh (boots the Postgres container, sets PG_DSN).
Results are written to examples/benchmark-results.json.
"""

import json
import os
import subprocess
import sys
import tempfile
import time

import duckdb
import psycopg

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SRC = os.path.join(REPO, "src")
sys.path.insert(0, SRC)

from driftwatch.connectors.duckdb import DuckDBConnector  # noqa: E402
from driftwatch.connectors.postgres import PostgresConnector  # noqa: E402
from driftwatch.models import KeyRange  # noqa: E402

DSN = os.environ.get("PG_DSN")
SIZES = [int(x) for x in os.environ.get("SIZES", "100000,1000000,10000000").split(",")]
COMPARE = ["amount", "customer", "status", "updated_at"]  # sorted, as the engine resolves "*"
NAIVE_MAX = 1_000_000  # do not pull every row into memory above this


def gen_and_load(n, warehouse, csv_path):
    """Generate n rows in DuckDB, export to CSV, stream the same CSV into Postgres."""
    con = duckdb.connect(warehouse)
    con.execute("DROP TABLE IF EXISTS orders")
    con.execute(
        "CREATE TABLE orders AS SELECT "
        "  i AS id, "
        "  'customer-' || (i %% 97) AS customer, "
        "  CAST(((i * 7) %% 100000) / 100.0 AS DECIMAL(10,2)) AS amount, "
        "  (['new','paid','shipped'])[(i %% 3) + 1] AS status, "
        "  TIMESTAMP '2026-06-01 00:00:00' + (i * INTERVAL 1 MINUTE) AS updated_at "
        "FROM range(1, %d + 1) t(i)" % n
    )
    con.execute("COPY orders TO '%s' (FORMAT CSV, HEADER FALSE)" % csv_path)
    con.close()

    with psycopg.connect(DSN, autocommit=True) as conn:
        conn.execute("DROP TABLE IF EXISTS orders")
        conn.execute(
            "CREATE TABLE orders (id int, customer text, "
            "amount numeric(10,2), status text, updated_at timestamp)"
        )
        with conn.cursor() as cur:
            with cur.copy(
                "COPY orders (id,customer,amount,status,updated_at) FROM STDIN (FORMAT CSV)"
            ) as cp, open(csv_path, "rb") as f:
                for chunk in iter(lambda: f.read(1 << 20), b""):
                    cp.write(chunk)
        # add the key after the bulk load, which is much faster than per-row index upkeep
        conn.execute("ALTER TABLE orders ADD PRIMARY KEY (id)")


def write_config(path, warehouse):
    with open(path, "w", encoding="utf-8") as f:
        f.write(
            "connections:\n"
            "  source: { driver: postgres, dsn: \"${PG_DSN}\" }\n"
            "  target: { driver: duckdb, path: \"%s\" }\n"
            "comparisons:\n"
            "  - name: orders\n"
            "    source_table: public.orders\n"
            "    target_table: orders\n"
            "    primary_key: [id]\n"
            "    compare_columns: \"*\"\n"
            "    recheck: { delay: 0s, rounds: 0 }\n" % warehouse
        )


def run(cfg):
    env = dict(os.environ, PYTHONPATH=SRC)
    args = [sys.executable, "-m", "driftwatch", "run", "-c", cfg, "--format", "json"]
    t = time.time()
    p = subprocess.run(args, env=env, capture_output=True, text=True)
    wall = time.time() - t
    metrics = json.loads(p.stdout)
    return p.returncode, metrics, wall


def naive_baseline(warehouse):
    pg = PostgresConnector(dsn=DSN)
    dk = DuckDBConnector(path=warehouse, read_only=True)
    full = KeyRange()
    t = time.time()
    a = pg.fetch_row_hashes("public.orders", ["id"], COMPARE, full, None, None, 12)
    b = dk.fetch_row_hashes("orders", ["id"], COMPARE, full, None, None, 12)
    elapsed = time.time() - t
    pg.close()
    dk.close()
    return elapsed, len(a) + len(b)


def inject_drift(warehouse, n):
    changed = [n // 10, n // 2, 9 * n // 10]
    missing = [n // 4, 3 * n // 4]
    extra = [n + 1, n + 2]
    con = duckdb.connect(warehouse)
    for i in changed:
        con.execute("UPDATE orders SET amount = -1.00 WHERE id = %d" % i)
    con.execute("DELETE FROM orders WHERE id IN (%s)" % ",".join(map(str, missing)))
    for i in extra:
        con.execute(
            "INSERT INTO orders VALUES (%d, 'ghost', 0.00, 'paid', "
            "TIMESTAMP '2026-06-01 00:00:00')" % i
        )
    con.close()
    expected = {(i,) for i in changed + missing + extra}
    return expected


def main():
    if not DSN:
        sys.exit("PG_DSN is not set. Run this through examples/benchmark.sh.")
    work = tempfile.mkdtemp(prefix="driftwatch-bench-")
    results = []

    for n in SIZES:
        warehouse = os.path.join(work, "wh-%d.duckdb" % n)
        csv_path = os.path.join(work, "data-%d.csv" % n)
        cfg = os.path.join(work, "cfg-%d.yaml" % n)
        print("\n### %s rows ###" % "{:,}".format(n), flush=True)

        t = time.time()
        gen_and_load(n, warehouse, csv_path)
        print("  loaded both sides in %.1fs" % (time.time() - t), flush=True)
        write_config(cfg, warehouse)

        rc, m, wall = run(cfg)
        match = {"engine_s": m["duration_seconds"], "wall_s": round(wall, 2),
                 "rows_read": m["rows_compared"], "exit": rc}
        print("  match:  %.3fs engine  (%d rows read, exit %d)"
              % (match["engine_s"], match["rows_read"], rc), flush=True)

        expected = inject_drift(warehouse, n)
        rc, m, wall = run(cfg)
        found = {tuple(k["key"]) for k in m["drift_keys"]}
        drift = {"engine_s": m["duration_seconds"], "wall_s": round(wall, 2),
                 "rows_read": m["rows_compared"], "segments": m["segments_scanned"],
                 "found": len(found), "correct": found == expected, "exit": rc}
        print("  drift:  %.3fs engine  (%d rows read, %d segments, found %d/%d, correct=%s)"
              % (drift["engine_s"], drift["rows_read"], drift["segments"],
                 drift["found"], len(expected), drift["correct"]), flush=True)

        naive = None
        if n <= NAIVE_MAX:
            elapsed, rows = naive_baseline(warehouse)
            naive = {"s": round(elapsed, 2), "rows": rows}
            speedup = round(elapsed / max(match["engine_s"], 1e-6), 1)
            print("  naive:  %.2fs to pull all %d row hashes  (driftwatch match is %sx faster)"
                  % (elapsed, rows, speedup), flush=True)
        else:
            print("  naive:  skipped (would pull all %s rows into memory)" % "{:,}".format(n),
                  flush=True)

        results.append({"rows": n, "match": match, "drift": drift, "naive": naive})
        with open(os.path.join(REPO, "examples", "benchmark-results.json"), "w") as f:
            json.dump(results, f, indent=2)

    print("\nwrote examples/benchmark-results.json", flush=True)


if __name__ == "__main__":
    main()
