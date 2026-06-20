#!/usr/bin/env python3
"""A real, end-to-end demo: Postgres (source) vs a DuckDB warehouse copy.

It loads 1,000 orders into Postgres and into a DuckDB file, then runs driftwatch
across the two different engines through three scenarios:

  1. the copies match            -> driftwatch exits 0
  2. the warehouse drifts        -> driftwatch exits 1 and names the rows
  3. a fresh row is still syncing -> driftwatch ignores it (lag is not drift)

Run it with examples/demo.sh, which boots the Postgres container and sets PG_DSN.
"""

import datetime
import os
import subprocess
import sys
import tempfile
from decimal import Decimal

import duckdb
import psycopg

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SRC = os.path.join(REPO, "src")
DSN = os.environ.get("PG_DSN")
N = 1000


def banner(text):
    print("\n" + "=" * 68)
    print(" " + text)
    print("=" * 68)


def make_rows():
    base = datetime.datetime(2026, 6, 1, 0, 0, 0)
    rows = []
    for i in range(1, N + 1):
        rows.append((
            i,
            "customer-%d" % (i % 97),
            Decimal("%d.%02d" % ((i * 7) % 1000, i % 100)),
            ["new", "paid", "shipped"][i % 3],
            base + datetime.timedelta(minutes=i),
        ))
    return rows


def seed_postgres(rows):
    with psycopg.connect(DSN, autocommit=True) as conn:
        conn.execute("DROP TABLE IF EXISTS orders")
        conn.execute(
            "CREATE TABLE orders (id int PRIMARY KEY, customer text, "
            "amount numeric(10,2), status text, updated_at timestamp)"
        )
        with conn.cursor() as cur:
            cur.executemany(
                "INSERT INTO orders VALUES (%s, %s, %s, %s, %s)", rows
            )


def seed_duckdb(path, rows):
    con = duckdb.connect(path)
    con.execute("DROP TABLE IF EXISTS orders")
    con.execute(
        "CREATE TABLE orders (id INTEGER, customer VARCHAR, "
        "amount DECIMAL(10,2), status VARCHAR, updated_at TIMESTAMP)"
    )
    con.executemany("INSERT INTO orders VALUES (?, ?, ?, ?, ?)", rows)
    con.close()


def write_config(path, duckdb_path, watermark=None, grace="0s"):
    wm = ""
    if watermark:
        wm = "    watermark_column: %s\n    grace: %s\n" % (watermark, grace)
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
            "%s"
            "    recheck: { delay: 0s, rounds: 1 }\n" % (duckdb_path, wm)
        )


def driftwatch(cfg, fmt="text"):
    """Run the real CLI as a subprocess and return (exit_code, stdout)."""
    env = dict(os.environ, PYTHONPATH=SRC)
    args = [sys.executable, "-m", "driftwatch", "run", "-c", cfg, "--format", fmt]
    print("$ driftwatch run -c demo.yaml" + ("" if fmt == "text" else " --format json"))
    p = subprocess.run(args, env=env, capture_output=True, text=True)
    sys.stdout.write(p.stdout)
    if p.stderr.strip():
        sys.stdout.write(p.stderr)
    print("exit code:", p.returncode)
    return p.returncode, p.stdout


def main():
    if not DSN:
        sys.exit("PG_DSN is not set. Run this through examples/demo.sh.")

    rows = make_rows()
    work = tempfile.mkdtemp(prefix="driftwatch-demo-")
    warehouse = os.path.join(work, "warehouse.duckdb")
    cfg = os.path.join(work, "demo.yaml")
    cfg_lag = os.path.join(work, "demo-lag.yaml")
    cfg_nolag = os.path.join(work, "demo-nolag.yaml")

    banner("driftwatch demo: Postgres (source of truth) vs DuckDB (warehouse copy)")
    print("Loading %d orders into Postgres, then mirroring them into DuckDB." % N)
    seed_postgres(rows)
    seed_duckdb(warehouse, rows)
    write_config(cfg, warehouse)
    write_config(cfg_lag, warehouse, watermark="updated_at", grace="15m")
    write_config(cfg_nolag, warehouse, watermark="updated_at", grace="0s")

    banner("Scenario 1: the copies match")
    driftwatch(cfg)

    banner("Scenario 2: the warehouse drifts (a real pipeline bug)")
    print("Simulating: one row dropped, one value wrong, one phantom row left behind.")
    con = duckdb.connect(warehouse)
    con.execute("DELETE FROM orders WHERE id = 500")                      # dropped event
    con.execute("UPDATE orders SET amount = 0.00 WHERE id = 250")         # bad transform
    con.execute("INSERT INTO orders VALUES "
                "(99999, 'ghost', 1.23, 'paid', TIMESTAMP '2026-06-01 00:30:00')")  # phantom
    con.close()
    driftwatch(cfg)
    print("\nSame run, machine-readable (for alerting and metrics):")
    driftwatch(cfg, fmt="json")

    banner("Scenario 3: a fresh row is still syncing (lag is not drift)")
    print("Reset the warehouse to match, then insert a brand-new order into Postgres only.")
    seed_duckdb(warehouse, rows)
    with psycopg.connect(DSN, autocommit=True) as conn:
        conn.execute(
            "INSERT INTO orders VALUES (1001, 'customer-1', 7.01, 'new', %s)",
            (datetime.datetime.utcnow(),),
        )
    print("\nWith a 15-minute grace window, the not-yet-synced row is ignored:")
    rc_lag, _ = driftwatch(cfg_lag)
    print("\nWithout the grace window, the same row is reported as missing:")
    rc_nolag, _ = driftwatch(cfg_nolag)

    banner("Result")
    print("Scenario 3 proves the difference from a plain diff:")
    print("  with grace window  -> exit %d (lag ignored, no false alarm)" % rc_lag)
    print("  without grace      -> exit %d (the fresh row shows as missing)" % rc_nolag)


if __name__ == "__main__":
    main()
