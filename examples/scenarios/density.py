#!/usr/bin/env python3
"""DRIFT DENSITY scenario: how does driftwatch's cost grow as the amount of drift grows?

We build TWO on-disk DuckDB warehouses (source.duckdb, target.duckdb), each with an
identical 1,000,000-row ``orders`` table, then mutate the TARGET to inject a known set
of drift for each scenario. For every scenario we drive the engine directly (no CLI,
no Postgres, no Docker) and measure:

  - rows_compared       (rows the engine actually fetched at leaf segments)
  - segments_scanned    (segments the recursive walk touched)
  - engine duration     (wall time around the compare() call)
  - in_sync
  - correctness:
        * small/medium drift (scenarios 1-4): the found key set must EXACTLY equal
          the injected key set.
        * large drift (scenarios 5-7): the COUNT of found keys must equal the count
          of injected keys.

The drift ids are deterministic so every scenario is independently verifiable.

Run with the Xcode python (has duckdb):
    /Applications/Xcode.app/Contents/Developer/usr/bin/python3 examples/scenarios/density.py

Writes examples/scenarios/density.json.
"""

import json
import os
import sys
import time

import duckdb

REPO = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
SRC = os.path.join(REPO, "src")
sys.path.insert(0, SRC)

from driftwatch.engine import compare  # noqa: E402
from driftwatch.config import ComparisonConfig, RecheckConfig  # noqa: E402
from driftwatch.connectors.duckdb import DuckDBConnector  # noqa: E402

N = 1_000_000
HERE = os.path.dirname(os.path.abspath(__file__))
SRC_DB = os.path.join(HERE, "source.duckdb")
TGT_DB = os.path.join(HERE, "target.duckdb")
OUT_JSON = os.path.join(HERE, "density.json")


def make_config():
    return ComparisonConfig(
        name="t",
        source_table="orders",
        target_table="orders",
        primary_key=["id"],
        compare_columns=None,
        exclude_columns=[],
        watermark_column=None,
        grace_seconds=0.0,
        segment_fanout=16,
        leaf_size=5000,
        float_precision=12,
        recheck=RecheckConfig(delay_seconds=0.0, rounds=0),
    )


CREATE_SQL = (
    "CREATE TABLE orders AS SELECT "
    "  i AS id, "
    "  'customer-' || (i % 97) AS customer, "
    "  CAST(((i * 7) % 100000) / 100.0 AS DECIMAL(10,2)) AS amount, "
    "  (['new','paid','shipped'])[(i % 3) + 1] AS status, "
    "  TIMESTAMP '2026-06-01 00:00:00' + (i * INTERVAL 1 MINUTE) AS updated_at "
    "FROM range(1, {n} + 1) t(i)"
).format(n=N)


def build_base(path):
    """Create a fresh warehouse file with the canonical 1M-row orders table."""
    if os.path.exists(path):
        os.remove(path)
    con = duckdb.connect(path)
    con.execute(CREATE_SQL)
    (count,) = con.execute("SELECT COUNT(*) FROM orders").fetchone()
    assert count == N, "base table rowcount %d != %d" % (count, N)
    con.close()


def reset_target():
    """Rebuild the target so each scenario starts from a clean identical copy."""
    build_base(TGT_DB)


# --- drift injectors. Each returns (description, expected_keys_set). -------------
# expected_keys are 1-tuples (the PK is single-column integer ``id``).


def scenario_0_matching():
    # No mutation: target is identical to source.
    return ("0 drift (identical)", set())


def scenario_1_one_changed():
    con = duckdb.connect(TGT_DB)
    con.execute("UPDATE orders SET amount = amount + 1.00 WHERE id = 500000")
    con.close()
    return ("1 changed row", {(500000,)})


def scenario_2_ten_changed():
    ids = [i * 100003 % N + 1 for i in range(1, 11)]  # 10 scattered, distinct
    ids = sorted(set(ids))
    con = duckdb.connect(TGT_DB)
    for i in ids:
        con.execute("UPDATE orders SET status = 'CHANGED' WHERE id = ?", [i])
    con.close()
    return ("10 scattered changed rows", {(i,) for i in ids})


def scenario_3_thousand_mixed():
    # 1,000 scattered drifted rows: ~1/3 changed, ~1/3 missing (deleted from target),
    # ~1/3 extra (inserted into target only). Deterministic, disjoint id ranges.
    changed = sorted({(i * 521 % N) + 1 for i in range(0, 1200)})[:334]
    # ensure missing/extra do not collide with changed
    used = set(changed)
    missing = []
    j = 7
    while len(missing) < 333:
        cand = (j * 733 % N) + 1
        j += 1
        if cand not in used:
            used.add(cand)
            missing.append(cand)
    missing = sorted(missing)
    # extra ids are > N so they cannot exist in source
    extra = list(range(N + 1, N + 1 + 333))

    con = duckdb.connect(TGT_DB)
    for i in changed:
        con.execute("UPDATE orders SET amount = amount + 13.00 WHERE id = ?", [i])
    con.execute("DELETE FROM orders WHERE id IN (%s)" % ",".join(map(str, missing)))
    for i in extra:
        con.execute(
            "INSERT INTO orders VALUES (?, 'ghost', 0.00, 'paid', TIMESTAMP '2026-06-01 00:00:00')",
            [i],
        )
    con.close()
    expected = {(i,) for i in changed} | {(i,) for i in missing} | {(i,) for i in extra}
    # sanity: the three sets are disjoint, so total is exactly their sum
    assert len(expected) == len(changed) + len(missing) + len(extra)
    return ("1,000 mixed (changed/missing/extra)", expected)


def scenario_4_100k_scattered():
    # 100,000 (10%) scattered changed rows: every 10th id (10, 20, ... 1,000,000).
    con = duckdb.connect(TGT_DB)
    con.execute("UPDATE orders SET amount = amount + 0.01 WHERE id % 10 = 0")
    con.close()
    expected = {(i,) for i in range(10, N + 1, 10)}
    return ("100,000 scattered changed (10%)", expected)


def scenario_5_contiguous_gap():
    # Delete ids 400,000..449,999 (a 50,000-row contiguous backfill gap in target).
    con = duckdb.connect(TGT_DB)
    con.execute("DELETE FROM orders WHERE id >= 400000 AND id < 450000")
    con.close()
    expected = {(i,) for i in range(400000, 450000)}
    return ("contiguous gap: delete 400k..449,999 (50,000 rows)", expected)


def scenario_6_all_changed():
    # 100% changed: every row's amount changed.
    con = duckdb.connect(TGT_DB)
    con.execute("UPDATE orders SET amount = amount + 1.00")
    con.close()
    expected = {(i,) for i in range(1, N + 1)}
    return ("100% changed (every amount)", expected)


# Ordered list; scenarios 0-4 verified by EXACT key-set equality, 5-6 by COUNT.
# (Per the brief: small/medium -> exact set; large -> count. We treat the 1,000-row
#  mixed scenario as small/medium -> exact set; 100k / 50k-gap / 100% -> count.)
SCENARIOS = [
    (scenario_0_matching, "exact"),
    (scenario_1_one_changed, "exact"),
    (scenario_2_ten_changed, "exact"),
    (scenario_3_thousand_mixed, "exact"),
    (scenario_4_100k_scattered, "count"),
    (scenario_5_contiguous_gap, "count"),
    (scenario_6_all_changed, "count"),
]


def run():
    print("building source warehouse (%s rows)..." % "{:,}".format(N), flush=True)
    build_base(SRC_DB)

    cfg = make_config()
    results = []

    for idx, (injector, mode) in enumerate(SCENARIOS):
        # Fresh identical target, then inject this scenario's drift.
        reset_target()
        desc, expected = injector()

        src = DuckDBConnector(path=SRC_DB, read_only=True)
        tgt = DuckDBConnector(path=TGT_DB, read_only=True)
        try:
            t0 = time.time()
            report = compare(src, tgt, cfg)
            engine_s = time.time() - t0
        finally:
            src.close()
            tgt.close()

        found = {tuple(dk.key) for dk in report.drift_keys}
        if mode == "exact":
            correct = found == expected
        else:
            correct = len(found) == len(expected)

        row = {
            "scenario": desc,
            "drift_rows": len(expected),
            "rows_compared": report.rows_compared,
            "pct_of_table": round(100.0 * report.rows_compared / N, 4),
            "segments_scanned": report.segments_scanned,
            "engine_s": round(engine_s, 3),
            "in_sync": report.in_sync,
            "found": len(found),
            "verify_mode": mode,
            "correct": bool(correct),
        }
        # Record a sample of mismatch detail when wrong, to make failures legible.
        if not correct:
            missing_from_found = sorted(list(expected - found))[:5]
            spurious = sorted(list(found - expected))[:5]
            row["mismatch_expected_not_found_sample"] = [list(k) for k in missing_from_found]
            row["mismatch_found_not_expected_sample"] = [list(k) for k in spurious]
        results.append(row)
        print(
            "  [%d] %-45s rows_compared=%d (%.4f%%) segments=%d engine=%.3fs in_sync=%s correct=%s"
            % (idx, desc, row["rows_compared"], row["pct_of_table"], row["segments_scanned"],
               row["engine_s"], row["in_sync"], row["correct"]),
            flush=True,
        )

    with open(OUT_JSON, "w", encoding="utf-8") as f:
        json.dump({"n_rows": N, "results": results}, f, indent=2)
    print("\nwrote %s" % OUT_JSON, flush=True)

    # cleanup the big warehouse files (results are already persisted)
    for p in (SRC_DB, TGT_DB):
        try:
            os.remove(p)
        except OSError:
            pass

    return results


if __name__ == "__main__":
    run()
