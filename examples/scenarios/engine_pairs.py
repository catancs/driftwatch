#!/usr/bin/env python3
"""ENGINE-PAIR coverage scenario: does driftwatch work for every engine COMBINATION,
not just the one headline path (Postgres -> DuckDB) the demo shows?

The engine (:func:`driftwatch.engine.compare`) is deliberately symmetric: it only ever
calls the dialect-free :class:`Connector` surface on a ``source`` and a ``target``. Two
connectors agree iff they both reproduce the :mod:`driftwatch.hashing` contract in SQL.
This scenario exercises FOUR pairings to prove that symmetry holds in practice:

  1. Postgres -> DuckDB   (cross-engine; the headline path. Generate in DuckDB, COPY to PG.)
  2. DuckDB   -> Postgres (reverse direction: confirm the cross-engine pairing is symmetric)
  3. Postgres -> Postgres (same engine; two tables orders_src / orders_dst, one DSN,
                           two PostgresConnector instances -> a read-replica / mirror)
  4. DuckDB   -> DuckDB   (same engine; two on-disk files -> same-engine baseline)

For EACH pair we:
  - load IDENTICAL data into both sides,
  - run a MATCHING compare (must be in_sync; this is the real cross-engine hash agreement
    test - if the two engines' SQL hashing recipes disagree, identical data shows FALSE
    drift here),
  - inject a known SPARSE drift set (9 rows: 3 changed / 3 missing / 3 extra) and confirm
    the engine finds EXACTLY those keys with the right kinds.

We drive ``compare()`` directly (no CLI) and measure rows_compared, segments_scanned and
the engine wall time around the call.

  ~500,000 rows; integer PK ``id``; columns: customer(text), amount(numeric/DECIMAL),
  status(text), updated_at(timestamp).

SNOWFLAKE: a SnowflakeConnector exists in src/, but it is GATED on a live Snowflake
account + the ``snowflake-connector-python`` package, neither of which is present in this
run. It is therefore UNVERIFIED here and is reported as such - never faked.

Run with the Xcode python (has psycopg v3 AND duckdb), with PG_DSN pointing at the
booted container:

    PG_DSN=postgresql://postgres:demo@localhost:55441/postgres \
      /Applications/Xcode.app/Contents/Developer/usr/bin/python3 \
      examples/scenarios/engine_pairs.py

Writes examples/scenarios/engine_pairs.json.
"""

import json
import os
import sys
import tempfile
import time

import duckdb
import psycopg

REPO = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
SRC = os.path.join(REPO, "src")
sys.path.insert(0, SRC)

from driftwatch.engine import compare  # noqa: E402
from driftwatch.config import ComparisonConfig, RecheckConfig  # noqa: E402
from driftwatch.connectors.duckdb import DuckDBConnector  # noqa: E402
from driftwatch.connectors.postgres import PostgresConnector  # noqa: E402

N = 500_000
HERE = os.path.dirname(os.path.abspath(__file__))
OUT_JSON = os.path.join(HERE, "engine_pairs.json")
DSN = os.environ.get("PG_DSN")

# The columns the engine will resolve from "*" (sorted, pk excluded): amount, customer,
# status, updated_at. We keep them identical in shape across both engines so the contract
# canonicalizes them to the same text on either side.
#   customer    text
#   amount      numeric(10,2) / DECIMAL(10,2)
#   status      text
#   updated_at  timestamp (naive)

# Deterministic sparse drift over the 500k id space. Disjoint id sets so the expected
# total is exactly 9 keys with known kinds.
CHANGED = [N // 10, N // 2, 9 * N // 10]          # exist on both, target value altered
MISSING = [N // 4, N // 3, 3 * N // 4]            # exist in source, DELETED from target
EXTRA = [N + 1, N + 2, N + 3]                     # exist only in target (id > N)


def make_config(source_table, target_table):
    """One comparison config. recheck disabled (no lag in this static scenario) so a
    single pass is the verdict."""
    return ComparisonConfig(
        name="engine_pair",
        source_table=source_table,
        target_table=target_table,
        primary_key=["id"],
        compare_columns=None,          # resolve "*" -> intersection, sorted
        exclude_columns=[],
        watermark_column=None,
        grace_seconds=0.0,
        segment_fanout=16,
        leaf_size=5000,
        float_precision=12,
        recheck=RecheckConfig(delay_seconds=0.0, rounds=0),
    )


# --- DuckDB data generation ---------------------------------------------------

DUCK_CREATE = (
    "CREATE TABLE {tbl} AS SELECT "
    "  i AS id, "
    "  'customer-' || (i % 97) AS customer, "
    "  CAST(((i * 7) % 100000) / 100.0 AS DECIMAL(10,2)) AS amount, "
    "  (['new','paid','shipped'])[(i % 3) + 1] AS status, "
    "  TIMESTAMP '2026-06-01 00:00:00' + (i * INTERVAL 1 MINUTE) AS updated_at "
    "FROM range(1, {n} + 1) t(i)"
)


def duck_build(path, table="orders"):
    """Create a fresh on-disk DuckDB warehouse with the canonical N-row table."""
    if os.path.exists(path):
        os.remove(path)
    wal = path + ".wal"
    if os.path.exists(wal):
        os.remove(wal)
    con = duckdb.connect(path)
    con.execute(DUCK_CREATE.format(tbl='"%s"' % table, n=N))
    (count,) = con.execute('SELECT COUNT(*) FROM "%s"' % table).fetchone()
    assert count == N, "duckdb %s rowcount %d != %d" % (table, count, N)
    con.close()


def duck_inject_drift(path, table="orders"):
    """Mutate a DuckDB table to inject the deterministic 9-row drift set.

    The mutated table is treated as the TARGET, so:
      - CHANGED ids get an altered amount,
      - MISSING ids are DELETED (present in source, absent in target),
      - EXTRA ids are INSERTED (present in target, absent in source).
    """
    con = duckdb.connect(path)
    for i in CHANGED:
        con.execute('UPDATE "%s" SET amount = amount + 1.00 WHERE id = ?' % table, [i])
    con.execute(
        'DELETE FROM "%s" WHERE id IN (%s)' % (table, ",".join(map(str, MISSING)))
    )
    for i in EXTRA:
        con.execute(
            'INSERT INTO "%s" VALUES (?, \'ghost\', 0.00, \'paid\', '
            "TIMESTAMP '2026-06-01 00:00:00')" % table,
            [i],
        )
    con.close()


# --- Postgres data loading (via DuckDB-exported CSV, like benchmark.py) --------


def pg_load_from_duck(duck_path, duck_table, pg_table, csv_path):
    """Generate identical data: export ``duck_table`` from DuckDB to CSV, COPY into a
    fresh Postgres table ``pg_table``. Mirrors examples/benchmark.py's loading pattern so
    both engines hold byte-identical logical data."""
    con = duckdb.connect(duck_path)
    con.execute("COPY \"%s\" TO '%s' (FORMAT CSV, HEADER FALSE)" % (duck_table, csv_path))
    con.close()

    with psycopg.connect(DSN, autocommit=True) as conn:
        conn.execute('DROP TABLE IF EXISTS "%s"' % pg_table)
        conn.execute(
            'CREATE TABLE "%s" (id int, customer text, '
            "amount numeric(10,2), status text, updated_at timestamp)" % pg_table
        )
        with conn.cursor() as cur:
            with cur.copy(
                'COPY "%s" (id,customer,amount,status,updated_at) FROM STDIN (FORMAT CSV)'
                % pg_table
            ) as cp, open(csv_path, "rb") as f:
                for chunk in iter(lambda: f.read(1 << 20), b""):
                    cp.write(chunk)
        conn.execute('ALTER TABLE "%s" ADD PRIMARY KEY (id)' % pg_table)


def pg_inject_drift(pg_table):
    """Inject the 9-row drift set into a Postgres table treated as the TARGET."""
    with psycopg.connect(DSN, autocommit=True) as conn:
        for i in CHANGED:
            conn.execute(
                'UPDATE "%s" SET amount = amount + 1.00 WHERE id = %%s' % pg_table, (i,)
            )
        conn.execute(
            'DELETE FROM "%s" WHERE id = ANY(%%s)' % pg_table, (MISSING,)
        )
        for i in EXTRA:
            conn.execute(
                'INSERT INTO "%s" VALUES (%%s, %%s, %%s, %%s, %%s)' % pg_table,
                (i, "ghost", 0.00, "paid", "2026-06-01 00:00:00"),
            )


def pg_drop(*tables):
    with psycopg.connect(DSN, autocommit=True) as conn:
        for t in tables:
            conn.execute('DROP TABLE IF EXISTS "%s"' % t)


# --- the expected drift set (kinds matter, not just keys) ----------------------

EXPECTED = (
    {(i,): "changed" for i in CHANGED}
    | {(i,): "missing" for i in MISSING}
    | {(i,): "extra" for i in EXTRA}
)


def verify(report):
    """Return (in_sync_after_drift, exact_correct, found_kinds_map, detail)."""
    found = {tuple(dk.key): dk.kind.value for dk in report.drift_keys}
    exact = found == EXPECTED
    detail = {}
    if not exact:
        detail["found_not_expected"] = sorted(
            "%s=%s" % (list(k), v) for k, v in found.items() if EXPECTED.get(k) != v
        )[:10]
        detail["expected_not_found"] = sorted(
            "%s=%s" % (list(k), v) for k, v in EXPECTED.items() if found.get(k) != v
        )[:10]
    return exact, found, detail


def run_pair(name, src_factory, tgt_factory, cfg):
    """Run a single matching pass and a single drift pass for one engine pair.

    ``src_factory`` / ``tgt_factory`` are zero-arg callables that open a FRESH connector
    each time (connections are read-only REPEATABLE READ snapshots, so we reopen between
    passes to observe the post-injection state on the target)."""
    # --- matching pass: identical data, must be in_sync ---
    src = src_factory()
    tgt = tgt_factory()
    try:
        t0 = time.time()
        rep_match = compare(src, tgt, cfg)
        match_s = time.time() - t0
    finally:
        src.close()
        tgt.close()

    match_in_sync = rep_match.in_sync

    return rep_match, match_s, match_in_sync


def run_drift(src_factory, tgt_factory, cfg):
    src = src_factory()
    tgt = tgt_factory()
    try:
        t0 = time.time()
        rep = compare(src, tgt, cfg)
        drift_s = time.time() - t0
    finally:
        src.close()
        tgt.close()
    return rep, drift_s


def main():
    if not DSN:
        sys.exit(
            "PG_DSN is not set. Export PG_DSN=postgresql://postgres:demo@localhost:55441/"
            "postgres (the booted container)."
        )

    work = tempfile.mkdtemp(prefix="dw-pairs-")
    duck_src = os.path.join(work, "source.duckdb")
    duck_tgt = os.path.join(work, "target.duckdb")
    duck_src2 = os.path.join(work, "source2.duckdb")  # for DuckDB->DuckDB second file
    csv_path = os.path.join(work, "data.csv")

    results = []

    print("=== building base data (%s rows) ===" % "{:,}".format(N), flush=True)
    # A pristine source-of-truth DuckDB warehouse (table 'orders'), reused to seed PG.
    duck_build(duck_src, "orders")

    snowflake_note = (
        "UNVERIFIED in this run: SnowflakeConnector exists in src/ but requires a live "
        "Snowflake account AND the snowflake-connector-python package. Neither is present "
        "(module not importable, no credentials). NOT exercised, NOT faked."
    )

    # ============================================================================
    # PAIR 1: Postgres -> DuckDB  (headline cross-engine path)
    #   source = Postgres(public.orders_pg), target = DuckDB(target.duckdb 'orders')
    # ============================================================================
    print("\n--- PAIR 1: Postgres -> DuckDB ---", flush=True)
    pg_load_from_duck(duck_src, "orders", "orders_pg", csv_path)   # PG seeded from duck
    duck_build(duck_tgt, "orders")                                  # identical duck target
    cfg1 = make_config("public.orders_pg", "orders")
    src1 = lambda: PostgresConnector(dsn=DSN)
    tgt1 = lambda: DuckDBConnector(path=duck_tgt, read_only=True)
    rep_m, match_s, in_sync = run_pair("pg->duck", src1, tgt1, cfg1)
    duck_inject_drift(duck_tgt, "orders")
    rep_d, drift_s = run_drift(src1, tgt1, cfg1)
    exact, found, detail = verify(rep_d)
    results.append(pair_record("Postgres -> DuckDB", rep_m, match_s, in_sync,
                               rep_d, drift_s, exact, found, detail))
    report_line(results[-1])

    # ============================================================================
    # PAIR 2: DuckDB -> Postgres  (reverse direction)
    #   source = DuckDB(source.duckdb 'orders'), target = Postgres(orders_pg2)
    #   We seed orders_pg2 from the SAME pristine duck source, then drift it on the PG side.
    # ============================================================================
    print("\n--- PAIR 2: DuckDB -> Postgres ---", flush=True)
    pg_load_from_duck(duck_src, "orders", "orders_pg2", csv_path)
    cfg2 = make_config("orders", "public.orders_pg2")
    src2 = lambda: DuckDBConnector(path=duck_src, read_only=True)   # pristine source
    tgt2 = lambda: PostgresConnector(dsn=DSN)
    rep_m, match_s, in_sync = run_pair("duck->pg", src2, tgt2, cfg2)
    pg_inject_drift("orders_pg2")
    rep_d, drift_s = run_drift(src2, tgt2, cfg2)
    exact, found, detail = verify(rep_d)
    results.append(pair_record("DuckDB -> Postgres", rep_m, match_s, in_sync,
                               rep_d, drift_s, exact, found, detail))
    report_line(results[-1])

    # ============================================================================
    # PAIR 3: Postgres -> Postgres  (same engine; two tables / one DSN = mirror)
    #   source = Postgres(orders_src), target = Postgres(orders_dst)
    #   Two PostgresConnector instances on the same DSN, different table names.
    # ============================================================================
    print("\n--- PAIR 3: Postgres -> Postgres ---", flush=True)
    pg_load_from_duck(duck_src, "orders", "orders_src", csv_path)
    pg_load_from_duck(duck_src, "orders", "orders_dst", csv_path)
    cfg3 = make_config("public.orders_src", "public.orders_dst")
    src3 = lambda: PostgresConnector(dsn=DSN)
    tgt3 = lambda: PostgresConnector(dsn=DSN)
    rep_m, match_s, in_sync = run_pair("pg->pg", src3, tgt3, cfg3)
    pg_inject_drift("orders_dst")
    rep_d, drift_s = run_drift(src3, tgt3, cfg3)
    exact, found, detail = verify(rep_d)
    results.append(pair_record("Postgres -> Postgres", rep_m, match_s, in_sync,
                               rep_d, drift_s, exact, found, detail))
    report_line(results[-1])

    # ============================================================================
    # PAIR 4: DuckDB -> DuckDB  (same engine; two files = baseline)
    # ============================================================================
    print("\n--- PAIR 4: DuckDB -> DuckDB ---", flush=True)
    duck_build(duck_src2, "orders")   # second pristine file (source)
    duck_build(duck_tgt, "orders")    # rebuild a fresh identical target
    cfg4 = make_config("orders", "orders")
    src4 = lambda: DuckDBConnector(path=duck_src2, read_only=True)
    tgt4 = lambda: DuckDBConnector(path=duck_tgt, read_only=True)
    rep_m, match_s, in_sync = run_pair("duck->duck", src4, tgt4, cfg4)
    duck_inject_drift(duck_tgt, "orders")
    rep_d, drift_s = run_drift(src4, tgt4, cfg4)
    exact, found, detail = verify(rep_d)
    results.append(pair_record("DuckDB -> DuckDB", rep_m, match_s, in_sync,
                               rep_d, drift_s, exact, found, detail))
    report_line(results[-1])

    # --- persist + cleanup ------------------------------------------------------
    payload = {
        "n_rows": N,
        "expected_drift": {
            "changed": CHANGED, "missing": MISSING, "extra": EXTRA, "total": len(EXPECTED),
        },
        "pairs": results,
        "snowflake": snowflake_note,
    }
    with open(OUT_JSON, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2)
    print("\nwrote %s" % OUT_JSON, flush=True)

    pg_drop("orders_pg", "orders_pg2", "orders_src", "orders_dst")
    for p in (duck_src, duck_tgt, duck_src2, csv_path,
              duck_src + ".wal", duck_tgt + ".wal", duck_src2 + ".wal"):
        try:
            os.remove(p)
        except OSError:
            pass

    # --- summary table ----------------------------------------------------------
    print("\n=== SUMMARY ===", flush=True)
    print("%-22s | %-13s | %-12s | %-13s | %s"
          % ("pair", "match in_sync", "drift correct", "rows_compared", "engine_s"))
    print("-" * 80)
    all_ok = True
    for r in results:
        print("%-22s | %-13s | %-12s | %13d | match=%.3f drift=%.3f"
              % (r["pair"], yn(r["match_in_sync"]), yn(r["drift_correct"]),
                 r["drift_rows_compared"], r["match_engine_s"], r["drift_engine_s"]))
        if not (r["match_in_sync"] and r["drift_correct"]):
            all_ok = False
    print("\nSnowflake: %s" % snowflake_note)
    print("\nALL PAIRS PASS: %s" % all_ok, flush=True)
    return results


def pair_record(pair, rep_m, match_s, in_sync, rep_d, drift_s, exact, found, detail):
    rec = {
        "pair": pair,
        "match_in_sync": bool(in_sync),
        "match_rows_compared": rep_m.rows_compared,
        "match_segments": rep_m.segments_scanned,
        "match_engine_s": round(match_s, 3),
        "drift_correct": bool(exact),
        "drift_in_sync": bool(rep_d.in_sync),
        "drift_found_total": len(found),
        "drift_found_by_kind": rep_d.counts_by_kind(),
        "drift_rows_compared": rep_d.rows_compared,
        "drift_segments": rep_d.segments_scanned,
        "drift_engine_s": round(drift_s, 3),
    }
    if detail:
        rec["mismatch_detail"] = detail
    return rec


def report_line(r):
    print(
        "  match: in_sync=%s rows_compared=%d segments=%d engine=%.3fs"
        % (r["match_in_sync"], r["match_rows_compared"], r["match_segments"],
           r["match_engine_s"]),
        flush=True,
    )
    print(
        "  drift: correct=%s found=%d %s rows_compared=%d segments=%d engine=%.3fs"
        % (r["drift_correct"], r["drift_found_total"], r["drift_found_by_kind"],
           r["drift_rows_compared"], r["drift_segments"], r["drift_engine_s"]),
        flush=True,
    )


def yn(b):
    return "yes" if b else "NO"


if __name__ == "__main__":
    main()
