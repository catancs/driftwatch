#!/usr/bin/env python3
"""DIFFERENTIATOR scenario: LAG AND RECHECK under realistic churn (DuckDB only).

Question this script answers for users:
  Does driftwatch really avoid false alarms on rows that are simply *still syncing*,
  while still catching real drift?

It models a SOURCE that is AHEAD of a LAGGING warehouse (TARGET):

  * source.duckdb / target.duckdb each hold ~500,000 `orders` rows with an
    `updated_at` timestamp.
  * target is a faithful copy "as of" a fixed SYNC_TIME.
  * the source then receives FRESH inserts/updates *after* SYNC_TIME (lag in flight).

A single fixed `now` is injected into engine.compare(...) so the watermark cutoff
(`cutoff = now - grace`) is deterministic. recheck delay is forced to 0 so tests are
instant.

Scenarios (see the module docstring of each scenario_N function):
  1. 5,000 brand-new source-only rows inside the grace window  -> IN SYNC at grace=15m,
     5,000 `missing` at grace=0. (core proof)
  2. 5,000 fresh UPDATEs source-only inside the grace window   -> ignored at 15m,
     5,000 `changed` at grace=0.
  3. 50 GENUINE drops from target with OLD updated_at           -> still 50 `missing`
     even at grace=15m (real drift is never hidden by the grace window).
  4. recheck: a reconciled candidate is DROPPED, a genuine divergence SURVIVES
     (rounds>=1). Implemented with the in-memory connector + a wrapper that returns
     a different hash on the SECOND fetch for one key (see note in scenario_4).
  5. grace sweep {0, 1m, 15m, 1h} over scenario 1 -> false-positive count per grace.

Run:
  PY=/Applications/Xcode.app/Contents/Developer/usr/bin/python3
  $PY examples/scenarios/lag_recheck.py

Writes examples/scenarios/lag_recheck.json and prints a results table.
Does not modify src/.
"""

from __future__ import annotations

import json
import os
import sys
from datetime import datetime, timedelta

import duckdb

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(os.path.dirname(HERE))
SRC = os.path.join(REPO, "src")
sys.path.insert(0, SRC)

from driftwatch.config import ComparisonConfig, RecheckConfig  # noqa: E402
from driftwatch.connectors.duckdb import DuckDBConnector  # noqa: E402
from driftwatch.connectors.memory import MemoryConnector  # noqa: E402
from driftwatch.engine import compare  # noqa: E402
from driftwatch.models import DriftKind  # noqa: E402

N = 500_000

# Fixed clock. Everything is reasoned relative to this single instant.
NOW = datetime(2026, 6, 21, 12, 0, 0)
# The target is a copy as of this sync time. Base rows are OLDER than this, so they
# are always inside any cutoff = NOW - grace (for grace up to 12h) and get compared.
SYNC_TIME = datetime(2026, 6, 21, 11, 50, 0)  # 10 minutes before NOW

WORK = os.path.join(HERE, "_work")
SRC_DB = os.path.join(WORK, "source.duckdb")
TGT_DB = os.path.join(WORK, "target.duckdb")

GRACE = {"0": 0.0, "1m": 60.0, "15m": 900.0, "1h": 3600.0}


# --------------------------------------------------------------------------- #
# data build
# --------------------------------------------------------------------------- #

def _amount(i: int) -> str:
    return "%d.%02d" % ((i * 7) % 100000, i % 100)


def build_base_dbs() -> None:
    """Build source.duckdb and target.duckdb as identical 500k-row copies.

    Base rows have updated_at spread BEFORE SYNC_TIME (well inside any grace cutoff),
    so the two sides are byte-for-byte identical and compare IN SYNC before any churn.
    """
    os.makedirs(WORK, exist_ok=True)
    for p in (SRC_DB, TGT_DB):
        if os.path.exists(p):
            os.remove(p)

    # Spread base updated_at across the hour ending 1 minute before SYNC_TIME so every
    # base row's watermark < SYNC_TIME < NOW - grace for all graces we test.
    base_start = SYNC_TIME - timedelta(hours=1)
    span_seconds = 3600 - 60  # last base row ~1 min before SYNC_TIME

    def seed(path: str) -> None:
        con = duckdb.connect(path)
        con.execute("DROP TABLE IF EXISTS orders")
        con.execute(
            "CREATE TABLE orders (id INTEGER, customer VARCHAR, "
            "amount DECIMAL(12,2), status VARCHAR, updated_at TIMESTAMP)"
        )
        # Generate the whole table in-engine for speed; updated_at deterministic per id.
        # Bind literals as parameters to avoid %-format clashing with SQL's % operator.
        con.execute(
            "INSERT INTO orders "
            "SELECT i AS id, "
            "'customer-' || (i % 997) AS customer, "
            "((i * 7) % 100000) + ((i % 100) / 100.0) AS amount, "
            "['new','paid','shipped'][(i % 3) + 1] AS status, "
            "?::TIMESTAMP + to_seconds(((i * 2654435761) % ?)) AS updated_at "
            "FROM range(1, ?) t(i)",
            [base_start.strftime("%Y-%m-%d %H:%M:%S"), span_seconds, N + 1],
        )
        con.close()

    seed(SRC_DB)
    seed(TGT_DB)


def reset_target_to_base() -> None:
    """Restore target.duckdb to an exact copy of the base (drops scenario mutations)."""
    if os.path.exists(TGT_DB):
        os.remove(TGT_DB)
    src = duckdb.connect(SRC_DB, read_only=True)
    con = duckdb.connect(TGT_DB)
    con.execute("DROP TABLE IF EXISTS orders")
    con.execute(
        "CREATE TABLE orders (id INTEGER, customer VARCHAR, "
        "amount DECIMAL(12,2), status VARCHAR, updated_at TIMESTAMP)"
    )
    rows = src.execute("SELECT * FROM orders").fetchall()
    con.executemany("INSERT INTO orders VALUES (?, ?, ?, ?, ?)", rows)
    con.close()
    src.close()


def reset_source_to_base() -> None:
    """Restore source.duckdb to an exact copy of the base (drops scenario mutations)."""
    if os.path.exists(SRC_DB):
        os.remove(SRC_DB)
    # Rebuild source deterministically the same way as target was just reset from.
    tgt = duckdb.connect(TGT_DB, read_only=True)
    con = duckdb.connect(SRC_DB)
    con.execute("DROP TABLE IF EXISTS orders")
    con.execute(
        "CREATE TABLE orders (id INTEGER, customer VARCHAR, "
        "amount DECIMAL(12,2), status VARCHAR, updated_at TIMESTAMP)"
    )
    rows = tgt.execute("SELECT * FROM orders").fetchall()
    con.executemany("INSERT INTO orders VALUES (?, ?, ?, ?, ?)", rows)
    con.close()
    tgt.close()


# --------------------------------------------------------------------------- #
# comparison harness
# --------------------------------------------------------------------------- #

def make_cfg(grace_seconds: float, watermark: bool = True, rounds: int = 0) -> ComparisonConfig:
    return ComparisonConfig(
        name="orders",
        source_table="orders",
        target_table="orders",
        primary_key=["id"],
        watermark_column="updated_at" if watermark else None,
        grace_seconds=grace_seconds,
        compare_columns="*" if False else None,  # None => resolve "*" at runtime
        recheck=RecheckConfig(delay_seconds=0.0, rounds=rounds),
    )


def run_duckdb(grace_seconds: float, rounds: int = 0):
    """Open fresh read-only connectors over the current db files and compare."""
    src = DuckDBConnector(SRC_DB, read_only=True)
    tgt = DuckDBConnector(TGT_DB, read_only=True)
    try:
        cfg = make_cfg(grace_seconds, watermark=True, rounds=rounds)
        report = compare(src, tgt, cfg, now=NOW, sleep=lambda _s: None)
    finally:
        src.close()
        tgt.close()
    return report


def counts(report):
    return report.counts_by_kind()


# --------------------------------------------------------------------------- #
# scenarios
# --------------------------------------------------------------------------- #

def scenario_1_inserts(results: list) -> None:
    """5,000 brand-new SOURCE-only rows with updated_at just BEFORE `now`.

    These are still propagating (target has not received them yet). With a 15m grace
    window they are excluded as lag -> IN SYNC. With grace=0 they show as 5,000 missing.
    """
    reset_target_to_base()
    reset_source_to_base()

    # Insert 5,000 fresh rows into SOURCE only, ids 600000001..600005000,
    # updated_at = NOW - 30s (inside 1m/15m/1h grace, OUTSIDE grace=0).
    fresh_ts = NOW - timedelta(seconds=30)
    con = duckdb.connect(SRC_DB)
    con.execute(
        "INSERT INTO orders "
        "SELECT 600000000 + i, 'fresh-' || i, (100000 + i) + 0.00, 'new', ?::TIMESTAMP "
        "FROM range(1, 5001) t(i)",
        [fresh_ts.strftime("%Y-%m-%d %H:%M:%S")],
    )
    con.close()

    r15 = run_duckdb(GRACE["15m"], rounds=0)
    r0 = run_duckdb(GRACE["0"], rounds=0)

    results.append(dict(
        scenario="1-inserts", grace="15m", rounds=0,
        reported=len(r15.drift_keys), by_kind=counts(r15),
        in_sync=r15.in_sync,
        expected="in_sync (0)", correct=(r15.in_sync and len(r15.drift_keys) == 0),
    ))
    results.append(dict(
        scenario="1-inserts", grace="0", rounds=0,
        reported=len(r0.drift_keys), by_kind=counts(r0),
        in_sync=r0.in_sync,
        expected="5000 missing",
        correct=(counts(r0)["missing"] == 5000 and len(r0.drift_keys) == 5000),
    ))


def scenario_2_updates(results: list) -> None:
    """5,000 existing rows UPDATED in SOURCE only with a fresh updated_at.

    grace=15m -> the fresh watermark pushes them past the cutoff, so BOTH sides'
    versions of these ids are excluded... wait: only the SOURCE row is fresh; the
    TARGET row still has the old (in-window) watermark. See findings for what that
    means. Expectation per the brief: 15m ignored, 0 reported; grace=0 -> 5,000 changed.
    """
    reset_target_to_base()
    reset_source_to_base()

    fresh_ts = NOW - timedelta(seconds=30)
    # Update ids 1..5000 in SOURCE only: bump amount AND set a fresh updated_at.
    con = duckdb.connect(SRC_DB)
    con.execute(
        "UPDATE orders SET amount = amount + 1000, updated_at = ?::TIMESTAMP "
        "WHERE id BETWEEN 1 AND 5000",
        [fresh_ts.strftime("%Y-%m-%d %H:%M:%S")],
    )
    con.close()

    r15 = run_duckdb(GRACE["15m"], rounds=0)
    r0 = run_duckdb(GRACE["0"], rounds=0)

    results.append(dict(
        scenario="2-updates", grace="15m", rounds=0,
        reported=len(r15.drift_keys), by_kind=counts(r15),
        in_sync=r15.in_sync,
        expected="ignored (0)", correct=(len(r15.drift_keys) == 0),
    ))
    results.append(dict(
        scenario="2-updates", grace="0", rounds=0,
        reported=len(r0.drift_keys), by_kind=counts(r0),
        in_sync=r0.in_sync,
        expected="5000 changed",
        correct=(counts(r0)["changed"] == 5000 and len(r0.drift_keys) == 5000),
    ))


def scenario_3_genuine_drop(results: list) -> None:
    """Delete 50 rows from TARGET whose updated_at is OLD (well before the cutoff).

    Real drift: these ids exist in source, are gone from target, and their watermark
    is old, so the grace window must NOT hide them. Expect 50 `missing` even at 15m.
    """
    reset_target_to_base()
    reset_source_to_base()

    # Delete 50 target rows whose watermark is well before the grace cutoff (NOW - 15m =
    # 11:45), so the grace window must NOT hide them. Picking by watermark, not by id,
    # guarantees all 50 are old enough to be compared and therefore detected.
    con = duckdb.connect(TGT_DB)
    con.execute(
        "DELETE FROM orders WHERE id IN ("
        "  SELECT id FROM orders WHERE updated_at < TIMESTAMP '2026-06-21 11:30:00'"
        "  ORDER BY id LIMIT 50)"
    )
    con.close()

    r15 = run_duckdb(GRACE["15m"], rounds=0)

    results.append(dict(
        scenario="3-genuine-drop", grace="15m", rounds=0,
        reported=len(r15.drift_keys), by_kind=counts(r15),
        in_sync=r15.in_sync,
        expected="50 missing",
        correct=(counts(r15)["missing"] == 50 and len(r15.drift_keys) == 50),
    ))


# --- scenario 4: recheck, with a wrapper that mutates between pass and recheck -----

class _SecondFetchWrapper(MemoryConnector):
    """Memory connector whose recheck fetch reconciles ONE key.

    The first pass uses fetch_row_hashes (leaf diff). The recheck pass calls
    fetch_row_hashes_for_keys with cutoff=None. We override ONLY the for_keys path so
    that, on the recheck, the wrapped table presents the *reconciled* (post-lag) value
    for the chosen key, while leaving a genuinely diverging key still diverging.

    This is the faithful way to demonstrate recheck via DuckDB files would require
    mutating the file mid-run; instead we use the in-memory reference connector and a
    wrapper that returns a different (reconciled) hash for `reconciling_key` on the
    second (for_keys / recheck) fetch. Stated explicitly in the findings.
    """

    def __init__(self, tables, recheck_overrides):
        super().__init__(tables)
        # recheck_overrides: {table: {key_tuple: replacement_row}} applied only in
        # fetch_row_hashes_for_keys (the recheck path), simulating the lag catching up.
        self._overrides = recheck_overrides

    def fetch_row_hashes_for_keys(self, table, pk_cols, compare_cols, keys, watermark_column, cutoff, float_precision):
        over = self._overrides.get(table, {})
        if not over:
            return super().fetch_row_hashes_for_keys(
                table, pk_cols, compare_cols, keys, watermark_column, cutoff, float_precision
            )
        wanted = set(keys)
        out = {}
        for key, row in self._selected(table, pk_cols, None, watermark_column, cutoff):
            if key in wanted:
                eff = over.get(key, row)
                out[key] = self._row_hash(eff, pk_cols, compare_cols, float_precision)
        # also surface override-only rows that were absent before but now present
        for key, eff in over.items():
            if key in wanted and key not in out:
                out[key] = self._row_hash(eff, pk_cols, compare_cols, float_precision)
        return out


def scenario_4_recheck(results: list) -> None:
    """Show a reconciled candidate is DROPPED and a genuine divergence SURVIVES.

    METHOD (stated plainly): in-memory reference connector + a wrapper that returns the
    *reconciled* value for one key on the recheck (for_keys) fetch. A DuckDB-file
    simulation would require mutating the file between the first pass and the recheck,
    which is not expressible through the read-only connectors in a single compare()
    call -- so we use MemoryConnector here. The recheck LOGIC under test is identical
    (engine._recheck calls the same fetch_row_hashes_for_keys on both connectors).

    Setup:
      key (1,): source amount=10, target amount=999  -> CHANGED candidate, but on
                recheck the TARGET reconciles to amount=10 (lag caught up) -> DROPPED.
      key (2,): source amount=20, target amount=777  -> CHANGED candidate, stays
                diverged on recheck (target still 777) -> SURVIVES.
    """
    src_rows = [
        {"id": 1, "amount": 10, "status": "x"},
        {"id": 2, "amount": 20, "status": "x"},
    ]
    tgt_rows = [
        {"id": 1, "amount": 999, "status": "x"},
        {"id": 2, "amount": 777, "status": "x"},
    ]
    source = MemoryConnector({"orders": src_rows})
    # On recheck, target's row for id=1 reconciles to amount=10 (matches source).
    target = _SecondFetchWrapper(
        {"orders": tgt_rows},
        recheck_overrides={"orders": {(1,): {"id": 1, "amount": 10, "status": "x"}}},
    )

    cfg = make_cfg(grace_seconds=0.0, watermark=False, rounds=1)
    rounds1 = compare(source, target, cfg, now=NOW, sleep=lambda _s: None)

    # rounds=0 control: no recheck, both candidates survive.
    cfg0 = make_cfg(grace_seconds=0.0, watermark=False, rounds=0)
    source0 = MemoryConnector({"orders": list(src_rows)})
    target0 = _SecondFetchWrapper(
        {"orders": list(tgt_rows)},
        recheck_overrides={"orders": {(1,): {"id": 1, "amount": 10, "status": "x"}}},
    )
    rounds0 = compare(source0, target0, cfg0, now=NOW, sleep=lambda _s: None)

    survivors = sorted(dk.key[0] for dk in rounds1.drift_keys)
    results.append(dict(
        scenario="4-recheck-rounds=1", grace="0", rounds=1,
        reported=len(rounds1.drift_keys), by_kind=counts(rounds1),
        candidates_before_recheck=rounds1.candidates_before_recheck,
        survivors=survivors,
        expected="2 candidates -> 1 survives (id=2)",
        correct=(rounds1.candidates_before_recheck == 2
                 and survivors == [2]
                 and len(rounds1.drift_keys) == 1),
        method="MemoryConnector + recheck-fetch wrapper (DuckDB-file mid-run mutation not expressible)",
    ))
    results.append(dict(
        scenario="4-recheck-rounds=0", grace="0", rounds=0,
        reported=len(rounds0.drift_keys), by_kind=counts(rounds0),
        candidates_before_recheck=rounds0.candidates_before_recheck,
        survivors=sorted(dk.key[0] for dk in rounds0.drift_keys),
        expected="no recheck -> both survive (id=1,2)",
        correct=(len(rounds0.drift_keys) == 2),
        method="MemoryConnector control (rounds=0)",
    ))


def scenario_5_grace_sweep(results: list) -> None:
    """Sweep grace = {0, 1m, 15m, 1h} over scenario 1's setup; report false positives.

    The 5,000 fresh source-only rows have updated_at = NOW - 30s. False-positive count =
    number of those still-syncing rows wrongly reported. Should be 0 for any grace > 30s.
    """
    reset_target_to_base()
    reset_source_to_base()

    fresh_ts = NOW - timedelta(seconds=30)
    con = duckdb.connect(SRC_DB)
    con.execute(
        "INSERT INTO orders "
        "SELECT 600000000 + i, 'fresh-' || i, (100000 + i) + 0.00, 'new', ?::TIMESTAMP "
        "FROM range(1, 5001) t(i)",
        [fresh_ts.strftime("%Y-%m-%d %H:%M:%S")],
    )
    con.close()

    sweep = []
    # grace=0 (no window) is covered authoritatively by scenario 1 (-> 5000 missing). Here we
    # sweep the real grace windows and confirm the fresh, still-syncing rows are 0 false positives.
    for label in ("1m", "15m", "1h"):
        r = run_duckdb(GRACE[label], rounds=0)
        fp = counts(r)["missing"]  # these fresh rows would show as missing if not handled
        sweep.append(dict(grace=label, false_positives=fp, in_sync=r.in_sync))
        results.append(dict(
            scenario="5-grace-sweep", grace=label, rounds=0,
            reported=len(r.drift_keys), by_kind=counts(r),
            in_sync=r.in_sync,
            expected="in_sync (0)",
            correct=(fp == 0 and r.in_sync),
        ))
    return sweep


# --------------------------------------------------------------------------- #
# main
# --------------------------------------------------------------------------- #

def fmt_table(results: list) -> str:
    header = ["scenario", "grace", "recheck", "reported", "expected", "correct"]
    rows = [header]
    for r in results:
        rows.append([
            r["scenario"],
            r["grace"],
            str(r["rounds"]),
            "%d %s" % (r["reported"], _kind_brief(r.get("by_kind", {}))),
            r["expected"],
            "yes" if r["correct"] else "NO",
        ])
    widths = [max(len(row[i]) for row in rows) for i in range(len(header))]
    lines = []
    for ri, row in enumerate(rows):
        line = " | ".join(c.ljust(widths[i]) for i, c in enumerate(row))
        lines.append(line)
        if ri == 0:
            lines.append("-+-".join("-" * w for w in widths))
    return "\n".join(lines)


def _kind_brief(by_kind: dict) -> str:
    parts = [f"{k}={v}" for k, v in by_kind.items() if v]
    return "(" + ",".join(parts) + ")" if parts else ""


def main() -> None:
    print("Building base 500,000-row source.duckdb and target.duckdb (identical copies)...")
    build_base_dbs()
    print("  NOW       =", NOW.isoformat())
    print("  SYNC_TIME =", SYNC_TIME.isoformat())
    print("  base rows: updated_at in [%s, ~%s)  (all OLDER than SYNC_TIME)" % (
        (SYNC_TIME - timedelta(hours=1)).isoformat(), (SYNC_TIME - timedelta(minutes=1)).isoformat()))

    results: list = []

    print("\n[1/5] inserts: 5,000 fresh source-only rows (updated_at = NOW-30s)")
    scenario_1_inserts(results)
    print("\n[2/5] updates: 5,000 source-only updates with fresh updated_at")
    scenario_2_updates(results)
    print("\n[3/5] genuine drop: 50 OLD rows deleted from target")
    scenario_3_genuine_drop(results)
    print("\n[4/5] recheck: reconciled candidate dropped, genuine divergence survives")
    scenario_4_recheck(results)
    print("\n[5/5] grace sweep over scenario 1")
    sweep = scenario_5_grace_sweep(results)

    table = fmt_table(results)
    print("\n" + "=" * 78)
    print("RESULTS")
    print("=" * 78)
    print(table)

    fp_headline = {s["grace"]: s["false_positives"] for s in sweep}
    print("\nHeadline false-positive counts (scenario 1 fresh rows, by grace window):")
    for g in ("1m", "15m", "1h"):
        print("  grace=%-3s -> %d false positives" % (g, fp_headline[g]))

    all_correct = all(r["correct"] for r in results)
    print("\nAll scenarios correct:", "YES" if all_correct else "NO")

    out = {
        "now": NOW.isoformat(),
        "sync_time": SYNC_TIME.isoformat(),
        "rows": N,
        "all_correct": all_correct,
        "false_positive_counts_scenario1": fp_headline,
        "results": results,
    }
    out_path = os.path.join(HERE, "lag_recheck.json")
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(out, f, indent=2, default=str)
    print("\nWrote", out_path)


if __name__ == "__main__":
    main()
