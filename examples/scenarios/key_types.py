#!/usr/bin/env python3
"""Scenario: PRIMARY KEY TYPES - does driftwatch work, and prune well, for keys
other than a plain single-column integer?

driftwatch only does numeric range-splitting (pruning) for a SINGLE-COLUMN INTEGER
primary key. For composite keys (``len(pk) != 1``) and for non-integer single keys
(string / UUID-like), ``engine._split`` returns ``None`` and the top global segment
is given ``hi=None`` in ``engine.compare`` - so the WHOLE range resolves as one leaf
and every row hash is fetched from both sides (the documented v1 fallback).

This script MEASURES that, honestly, on identical ~500k-row tables for four key types:

  1. single integer PK (baseline)             -> expect strong pruning
  2. single integer PK with GAPS (id = i*7)    -> does interpolation still prune?
  3. composite PK (region text, id int)        -> documented whole-range fallback
  4. single string/UUID-like PK ('ord-…')      -> documented whole-range fallback

For each type it builds identical source.duckdb / target.duckdb tables, injects the
SAME sparse drift (3 changed, 3 missing, 3 extra = 9 rows), drives
``driftwatch.engine.compare`` directly (rounds=0, leaf_size=5000), and records
rows_compared, segments_scanned, engine duration, in_sync, and correctness (the set of
drift keys found EXACTLY equals the injected set).

USE DUCKDB ONLY - no Docker, no Postgres. Writes examples/scenarios/key_types.json.
Does NOT modify src/.
"""

import json
import os
import sys
import tempfile
import time

import duckdb

REPO = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
SRC = os.path.join(REPO, "src")
sys.path.insert(0, SRC)

from driftwatch.config import ComparisonConfig, RecheckConfig  # noqa: E402
from driftwatch.connectors.duckdb import DuckDBConnector  # noqa: E402
from driftwatch.engine import compare  # noqa: E402

N = 500_000
LEAF_SIZE = 5000


# --- table builders ----------------------------------------------------------
#
# Each builder creates table ``t`` in both the source and target DuckDB files with
# IDENTICAL data, then returns the expected drift-key set after injecting the same
# 9-row sparse drift into the TARGET only. Drift shape (mirrors examples/benchmark.py):
#   - CHANGED: 3 rows whose compare columns differ between source and target
#   - MISSING: 3 rows present in source, deleted from target
#   - EXTRA:   3 rows inserted into target that don't exist in source
#
# The expected keys are built as Python tuples in the SAME shape the engine returns
# (DuckDB hands back int for integer columns, str for text columns).


def _connect(path):
    con = duckdb.connect(path)
    con.execute("DROP TABLE IF EXISTS t")
    return con


def build_int(src_path, tgt_path):
    """1. single integer PK, contiguous ids 1..N."""
    body = (
        "CREATE TABLE t AS SELECT "
        "  i AS id, "
        "  'customer-' || (i % 97) AS customer, "
        "  CAST(((i * 7) % 100000) / 100.0 AS DECIMAL(10,2)) AS amount, "
        "  (['new','paid','shipped'])[(i % 3) + 1] AS status "
        f"FROM range(1, {N} + 1) t(i)"
    )
    for p in (src_path, tgt_path):
        con = _connect(p)
        con.execute(body)
        con.close()

    changed = [N // 10, N // 2, 9 * N // 10]
    missing = [N // 4, 3 * N // 4, N // 3]
    extra = [N + 1, N + 2, N + 3]
    con = duckdb.connect(tgt_path)
    for i in changed:
        con.execute(f"UPDATE t SET amount = -1.00 WHERE id = {i}")
    con.execute("DELETE FROM t WHERE id IN (%s)" % ",".join(map(str, missing)))
    for i in extra:
        con.execute(f"INSERT INTO t VALUES ({i}, 'ghost', 0.00, 'paid')")
    con.close()
    return ["id"], {(i,) for i in changed + missing + extra}


def build_int_gaps(src_path, tgt_path):
    """2. single integer PK with GAPS: id = i*7 (sparse, spaced by 7)."""
    body = (
        "CREATE TABLE t AS SELECT "
        "  i * 7 AS id, "
        "  'customer-' || (i % 97) AS customer, "
        "  CAST(((i * 7) % 100000) / 100.0 AS DECIMAL(10,2)) AS amount, "
        "  (['new','paid','shipped'])[(i % 3) + 1] AS status "
        f"FROM range(1, {N} + 1) t(i)"
    )
    for p in (src_path, tgt_path):
        con = _connect(p)
        con.execute(body)
        con.close()

    # pick real existing ids (multiples of 7) to change/delete; extras beyond max.
    changed = [(N // 10) * 7, (N // 2) * 7, (9 * N // 10) * 7]
    missing = [(N // 4) * 7, (3 * N // 4) * 7, (N // 3) * 7]
    extra = [(N + 1) * 7, (N + 2) * 7, (N + 3) * 7]
    con = duckdb.connect(tgt_path)
    for i in changed:
        con.execute(f"UPDATE t SET amount = -1.00 WHERE id = {i}")
    con.execute("DELETE FROM t WHERE id IN (%s)" % ",".join(map(str, missing)))
    for i in extra:
        con.execute(f"INSERT INTO t VALUES ({i}, 'ghost', 0.00, 'paid')")
    con.close()
    return ["id"], {(i,) for i in changed + missing + extra}


def build_composite(src_path, tgt_path):
    """3. composite PK (region text, id int): region in {EU,US,APAC}, id 1..M per region."""
    # 3 regions, N rows total split across them by i % 3.
    body = (
        "CREATE TABLE t AS SELECT "
        "  (['EU','US','APAC'])[(i % 3) + 1] AS region, "
        "  (i // 3) AS id, "
        "  'customer-' || (i % 97) AS customer, "
        "  CAST(((i * 7) % 100000) / 100.0 AS DECIMAL(10,2)) AS amount, "
        "  (['new','paid','shipped'])[(i % 3) + 1] AS status "
        f"FROM range(0, {N}) t(i)"
    )
    for p in (src_path, tgt_path):
        con = _connect(p)
        con.execute(body)
        con.close()

    # choose existing (region, id) pairs. With i = 3*id + region_idx:
    #   region_idx 0 -> EU, 1 -> US, 2 -> APAC. id runs 0..(N//3 - 1) per region.
    M = N // 3  # ~166k ids per region
    changed = [("EU", M // 10), ("US", M // 2), ("APAC", 9 * M // 10)]
    missing = [("EU", M // 4), ("US", 3 * M // 4), ("APAC", M // 3)]
    extra = [("EU", M + 1), ("US", M + 2), ("APAC", M + 3)]  # ids beyond range -> new
    con = duckdb.connect(tgt_path)
    for region, rid in changed:
        con.execute(f"UPDATE t SET amount = -1.00 WHERE region = '{region}' AND id = {rid}")
    for region, rid in missing:
        con.execute(f"DELETE FROM t WHERE region = '{region}' AND id = {rid}")
    for region, rid in extra:
        con.execute(
            f"INSERT INTO t VALUES ('{region}', {rid}, 'ghost', 0.00, 'paid')"
        )
    con.close()
    return ["region", "id"], {tuple(x) for x in changed + missing + extra}


def build_string(src_path, tgt_path):
    """4. single string/UUID-like PK: uid = 'ord-' || lpad(i, 8, '0')."""
    body = (
        "CREATE TABLE t AS SELECT "
        "  'ord-' || lpad(i::VARCHAR, 8, '0') AS uid, "
        "  'customer-' || (i % 97) AS customer, "
        "  CAST(((i * 7) % 100000) / 100.0 AS DECIMAL(10,2)) AS amount, "
        "  (['new','paid','shipped'])[(i % 3) + 1] AS status "
        f"FROM range(1, {N} + 1) t(i)"
    )
    for p in (src_path, tgt_path):
        con = _connect(p)
        con.execute(body)
        con.close()

    def uid(i):
        return "ord-" + str(i).zfill(8)

    changed = [uid(N // 10), uid(N // 2), uid(9 * N // 10)]
    missing = [uid(N // 4), uid(3 * N // 4), uid(N // 3)]
    extra = [uid(N + 1), uid(N + 2), uid(N + 3)]
    con = duckdb.connect(tgt_path)
    for u in changed:
        con.execute(f"UPDATE t SET amount = -1.00 WHERE uid = '{u}'")
    con.execute("DELETE FROM t WHERE uid IN (%s)" % ",".join("'%s'" % u for u in missing))
    for u in extra:
        con.execute(f"INSERT INTO t VALUES ('{u}', 'ghost', 0.00, 'paid')")
    con.close()
    return ["uid"], {(u,) for u in changed + missing + extra}


SCENARIOS = [
    ("single_integer", build_int),
    ("integer_with_gaps", build_int_gaps),
    ("composite_region_id", build_composite),
    ("string_uuid_like", build_string),
]


def run_one(name, builder, work):
    src_path = os.path.join(work, f"{name}-source.duckdb")
    tgt_path = os.path.join(work, f"{name}-target.duckdb")

    t0 = time.time()
    pk_cols, expected = builder(src_path, tgt_path)
    build_s = time.time() - t0

    # row count actually present in source (target has 3 deleted + 3 inserted).
    cnt_con = duckdb.connect(src_path)
    table_rows = cnt_con.execute("SELECT COUNT(*) FROM t").fetchone()[0]
    cnt_con.close()

    source = DuckDBConnector(path=src_path, read_only=True)
    target = DuckDBConnector(path=tgt_path, read_only=True)

    cmp = ComparisonConfig(
        name=name,
        source_table="t",
        target_table="t",
        primary_key=pk_cols,
        compare_columns=None,  # resolve "*" -> sorted(amount, customer, status)
        leaf_size=LEAF_SIZE,
        recheck=RecheckConfig(delay_seconds=0.0, rounds=0),
    )

    t0 = time.time()
    report = compare(source, target, cmp)
    engine_s = time.time() - t0

    source.close()
    target.close()

    found = {tuple(dk.key) for dk in report.drift_keys}
    correct = found == expected
    pct = round(100.0 * report.rows_compared / table_rows, 2)

    result = {
        "key_type": name,
        "primary_key": pk_cols,
        "table_rows": table_rows,
        "rows_compared": report.rows_compared,
        "rows_compared_pct": pct,
        "segments_scanned": report.segments_scanned,
        "engine_seconds": round(report.duration_seconds, 3),
        "engine_seconds_wall": round(engine_s, 3),
        "build_seconds": round(build_s, 2),
        "in_sync": report.in_sync,
        "drift_found": len(found),
        "drift_expected": len(expected),
        "correct": correct,
        "drift_by_kind": report.counts_by_kind(),
        "missing_from_found": sorted(str(k) for k in (expected - found)),
        "unexpected_in_found": sorted(str(k) for k in (found - expected)),
    }
    print(
        "  %-22s rows_compared=%d (%.2f%% of %d)  segments=%d  engine=%.3fs  "
        "in_sync=%s  found=%d/%d  correct=%s"
        % (
            name,
            report.rows_compared,
            pct,
            table_rows,
            report.segments_scanned,
            report.duration_seconds,
            report.in_sync,
            len(found),
            len(expected),
            correct,
        ),
        flush=True,
    )
    return result


def main():
    work = tempfile.mkdtemp(prefix="driftwatch-keytypes-")
    print("### PRIMARY KEY TYPES scenario (N=%d rows, leaf_size=%d) ###" % (N, LEAF_SIZE))
    print("work dir: %s" % work, flush=True)
    results = []
    for name, builder in SCENARIOS:
        results.append(run_one(name, builder, work))

    out_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "key_types.json")
    payload = {
        "scenario": "primary_key_types",
        "n_rows": N,
        "leaf_size": LEAF_SIZE,
        "recheck_rounds": 0,
        "injected_drift_per_type": {"changed": 3, "missing": 3, "extra": 3, "total": 9},
        "results": results,
    }
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2)
    print("\nwrote %s" % out_path, flush=True)


if __name__ == "__main__":
    main()
