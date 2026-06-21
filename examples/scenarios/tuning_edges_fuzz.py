#!/usr/bin/env python3
"""CONFIG TUNING, ROBUSTNESS EDGES, and a FUZZ test for driftwatch.

Three independent investigations, all driven through ``driftwatch.engine.compare``
with NO Docker and NO external database:

PART A - CONFIG TUNING (DuckDB)
    Build one 500,000-row ``orders`` table on each side (source.duckdb / target.duckdb)
    and inject a FIXED sparse drift of ~50 scattered rows into the target. Then sweep
    the two knobs that govern the recursive hash-segmentation walk and measure cost:

        leaf_size      in {500, 2000, 5000, 20000, 100000}  (segment_fanout fixed 16)
        segment_fanout in {4, 16, 64}                       (leaf_size fixed 5000)

    For each setting we record rows_compared (rows actually fetched at leaves),
    segments_scanned (segments the walk touched), and engine wall time. The fixed
    drift means correctness is identical across the sweep; only COST moves, which is
    exactly the trade-off we want to expose.

PART B - ROBUSTNESS EDGES (MemoryConnector + a couple of tiny DuckDB tables)
    Nine hand-built edge cases, each asserting EXACT classification:
      empty/empty, source-only, target-only, identical single row, changed single row,
      a NULL on one side (changed), a value-swap that a naive checksum could miss, and
      compare_columns="*" resolving to the column INTERSECTION when one side carries an
      extra column.

PART C - FUZZ (MemoryConnector, fast)
    50 random trials. Each trial builds an identical source+target of random size with
    random integer PKs and a few random columns, injects a random truth set of
    changed/missing/extra rows, runs compare() (with leaf_size/fanout randomized), and
    asserts the returned drift key+kind set EXACTLY equals the injected truth. Any
    failure prints the seed and inputs needed to reproduce it. (leaf_size is floored
    relative to N so the reference connector's full-scan-per-segment doesn't make a
    single trial O(N^2/leaf_size); see run_one_fuzz_trial for the rationale.)

Run with the Xcode python (has duckdb):
    /Applications/Xcode.app/Contents/Developer/usr/bin/python3 \
        examples/scenarios/tuning_edges_fuzz.py

Writes examples/scenarios/tuning_edges_fuzz.json.
"""

import json
import os
import random
import sys
import time

import duckdb

REPO = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
SRC = os.path.join(REPO, "src")
sys.path.insert(0, SRC)

from driftwatch.engine import compare  # noqa: E402
from driftwatch.config import ComparisonConfig, RecheckConfig  # noqa: E402
from driftwatch.connectors.duckdb import DuckDBConnector  # noqa: E402
from driftwatch.connectors.memory import MemoryConnector  # noqa: E402
from driftwatch.models import DriftKind  # noqa: E402

HERE = os.path.dirname(os.path.abspath(__file__))
SRC_DB = os.path.join(HERE, "tuning_source.duckdb")
TGT_DB = os.path.join(HERE, "tuning_target.duckdb")
OUT_JSON = os.path.join(HERE, "tuning_edges_fuzz.json")

# No recheck: every connector here is static, so recheck rounds add nothing but time.
NO_RECHECK = RecheckConfig(delay_seconds=0.0, rounds=0)


def make_config(leaf_size=5000, segment_fanout=16, compare_columns=None,
                exclude_columns=None, primary_key=("id",)):
    return ComparisonConfig(
        name="t",
        source_table="orders",
        target_table="orders",
        primary_key=list(primary_key),
        compare_columns=list(compare_columns) if compare_columns is not None else None,
        exclude_columns=list(exclude_columns) if exclude_columns else [],
        watermark_column=None,
        grace_seconds=0.0,
        segment_fanout=segment_fanout,
        leaf_size=leaf_size,
        float_precision=12,
        recheck=NO_RECHECK,
    )


# =====================================================================================
# PART A - CONFIG TUNING
# =====================================================================================

N_TUNE = 500_000

CREATE_SQL = (
    "CREATE TABLE orders AS SELECT "
    "  i AS id, "
    "  'customer-' || (i % 97) AS customer, "
    "  CAST(((i * 7) % 100000) / 100.0 AS DECIMAL(10,2)) AS amount, "
    "  (['new','paid','shipped'])[(i % 3) + 1] AS status, "
    "  TIMESTAMP '2026-06-01 00:00:00' + (i * INTERVAL 1 MINUTE) AS updated_at "
    "FROM range(1, {n} + 1) t(i)"
).format(n=N_TUNE)


def build_tune_base(path):
    if os.path.exists(path):
        os.remove(path)
    con = duckdb.connect(path)
    con.execute(CREATE_SQL)
    (count,) = con.execute("SELECT COUNT(*) FROM orders").fetchone()
    assert count == N_TUNE, "base table rowcount %d != %d" % (count, N_TUNE)
    con.close()


def inject_fixed_sparse_drift(path):
    """Inject a FIXED, reproducible sparse drift of ~50 scattered rows into the target.

    20 changed + 15 missing + 15 extra = 50 distinct drifted keys, evenly scattered
    across the whole 1..500,000 id space so the recursive walk must descend into many
    different parts of the key range to find them. Returns the expected key set.
    """
    # 20 changed: evenly spaced across the range, distinct.
    changed = sorted({(i * (N_TUNE // 20)) + 1 for i in range(20)})
    used = set(changed)
    # 15 missing: spaced offset from the changed ids, no collisions.
    missing = []
    step = N_TUNE // 15
    cand = 7
    while len(missing) < 15:
        c = (cand % N_TUNE) + 1
        cand += step
        if c not in used:
            used.add(c)
            missing.append(c)
    missing = sorted(missing)
    # 15 extra: ids above N so they cannot exist in source.
    extra = list(range(N_TUNE + 1, N_TUNE + 1 + 15))

    con = duckdb.connect(path)
    for i in changed:
        con.execute("UPDATE orders SET amount = amount + 17.00 WHERE id = ?", [i])
    con.execute("DELETE FROM orders WHERE id IN (%s)" % ",".join(map(str, missing)))
    for i in extra:
        con.execute(
            "INSERT INTO orders VALUES (?, 'ghost', 0.00, 'paid', "
            "TIMESTAMP '2026-06-01 00:00:00')",
            [i],
        )
    con.close()

    expected = {(i,) for i in changed} | {(i,) for i in missing} | {(i,) for i in extra}
    assert len(expected) == len(changed) + len(missing) + len(extra), "drift ids overlap"
    return expected


def run_tuning():
    print("\n=== PART A: CONFIG TUNING (%s rows, fixed ~50-row sparse drift) ===" %
          "{:,}".format(N_TUNE), flush=True)
    print("building source warehouse...", flush=True)
    build_tune_base(SRC_DB)
    print("building target warehouse + injecting fixed sparse drift...", flush=True)
    build_tune_base(TGT_DB)
    expected = inject_fixed_sparse_drift(TGT_DB)
    n_drift = len(expected)
    print("  injected %d drifted keys" % n_drift, flush=True)

    sweeps = []
    # leaf_size sweep (fanout fixed 16)
    for leaf in (500, 2000, 5000, 20000, 100000):
        sweeps.append(("leaf_size", leaf, 16))
    # fanout sweep (leaf_size fixed 5000)
    for fan in (4, 16, 64):
        sweeps.append(("segment_fanout", 5000, fan))

    rows = []
    for knob, leaf, fan in sweeps:
        cfg = make_config(leaf_size=leaf, segment_fanout=fan)
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
        correct = found == expected
        row = {
            "sweep": knob,
            "leaf_size": leaf,
            "segment_fanout": fan,
            "rows_compared": report.rows_compared,
            "segments_scanned": report.segments_scanned,
            "engine_s": round(engine_s, 3),
            "found": len(found),
            "drift_expected": n_drift,
            "correct": bool(correct),
        }
        rows.append(row)
        print(
            "  leaf=%-6d fanout=%-3d -> rows_compared=%-7d segments=%-5d engine=%6.3fs "
            "correct=%s" % (leaf, fan, row["rows_compared"], row["segments_scanned"],
                            row["engine_s"], row["correct"]),
            flush=True,
        )

    for p in (SRC_DB, TGT_DB):
        try:
            os.remove(p)
        except OSError:
            pass

    return {"n_rows": N_TUNE, "drift_rows": n_drift, "sweeps": rows}


# =====================================================================================
# PART B - ROBUSTNESS EDGES
# =====================================================================================

def _drift_set(report):
    """Return {(key_tuple, kind_str)} for a report - key+kind, for exact comparison."""
    return {(tuple(dk.key), dk.kind.value) for dk in report.drift_keys}


def _mem_compare(src_rows, tgt_rows, cfg):
    src = MemoryConnector({"orders": src_rows})
    tgt = MemoryConnector({"orders": tgt_rows})
    return compare(src, tgt, cfg)


def run_edges():
    print("\n=== PART B: ROBUSTNESS EDGES ===", flush=True)
    cases = []

    def record(name, expected, report, extra=None):
        actual = _drift_set(report)
        ok = (actual == expected) and (report.in_sync == (len(expected) == 0))
        row = {
            "case": name,
            "expected": sorted([list(k) + [kind] for (k, kind) in expected]),
            "actual": sorted([list(k) + [kind] for (k, kind) in actual]),
            "in_sync": report.in_sync,
            "ok": bool(ok),
        }
        if extra:
            row.update(extra)
        cases.append(row)
        print("  [%s] %-52s expected=%d actual=%d" %
              ("OK" if ok else "FAIL", name, len(expected), len(actual)), flush=True)
        return ok

    cfg = make_config()

    # 1. empty source + empty target -> in_sync, no drift.
    record("empty source + empty target (in_sync)",
           set(), _mem_compare([], [], cfg))

    # 2. source has rows, target empty -> all MISSING.
    src_rows = [{"id": i, "v": i * 2} for i in range(1, 6)]
    rep = _mem_compare(src_rows, [], cfg)
    record("source rows, target empty (all missing)",
           {((i,), "missing") for i in range(1, 6)}, rep)

    # 3. target has rows, source empty -> all EXTRA.
    tgt_rows = [{"id": i, "v": i * 2} for i in range(1, 6)]
    rep = _mem_compare([], tgt_rows, cfg)
    record("target rows, source empty (all extra)",
           {((i,), "extra") for i in range(1, 6)}, rep)

    # 4. single row identical -> in_sync.
    row = [{"id": 1, "v": 42}]
    record("single row identical (in_sync)",
           set(), _mem_compare(row, list(row), cfg))

    # 5. single row changed -> CHANGED.
    rep = _mem_compare([{"id": 1, "v": 42}], [{"id": 1, "v": 43}], cfg)
    record("single row changed",
           {((1,), "changed")}, rep)

    # 6. a NULL in a compared column on one side -> CHANGED.
    rep = _mem_compare([{"id": 1, "v": 7}], [{"id": 1, "v": None}], cfg)
    record("NULL on one side of a compared column (changed)",
           {((1,), "changed")}, rep)

    # 7. duplicate-safe / value-swap: two rows swap their non-key values. A naive
    #    aggregate that ignored the PK could see an unchanged SUM; because the PK is
    #    part of every row hash, BOTH rows must be reported CHANGED.
    src_rows = [{"id": 1, "v": "A"}, {"id": 2, "v": "B"}]
    tgt_rows = [{"id": 1, "v": "B"}, {"id": 2, "v": "A"}]  # values swapped between keys
    rep = _mem_compare(src_rows, tgt_rows, cfg)
    record("value-swap between two keys (both changed, checksum not fooled)",
           {((1,), "changed"), ((2,), "changed")}, rep)

    # 8. compare_columns="*" resolves to the INTERSECTION when one side has an extra
    #    column. Source rows carry an extra column "only_src" that the target lacks;
    #    that column must be EXCLUDED from the comparison, so rows that differ ONLY in
    #    it are reported in_sync. We use compare_columns=None (i.e. "*").
    cfg_star = make_config(compare_columns=None)
    src_rows = [
        {"id": 1, "shared": 100, "only_src": "x"},
        {"id": 2, "shared": 200, "only_src": "y"},
    ]
    tgt_rows = [
        {"id": 1, "shared": 100},  # same shared value, no only_src column at all
        {"id": 2, "shared": 200},
    ]
    rep = _mem_compare(src_rows, tgt_rows, cfg_star)
    record("compare_columns='*' -> column intersection (extra col excluded, in_sync)",
           set(), rep,
           extra={"note": "source-only column 'only_src' must be excluded from '*'"})

    # 8b. ...and the intersection still DETECTS a real diff on the shared column.
    src_rows = [{"id": 1, "shared": 100, "only_src": "x"}]
    tgt_rows = [{"id": 1, "shared": 999}]  # shared differs
    rep = _mem_compare(src_rows, tgt_rows, cfg_star)
    record("compare_columns='*' intersection still detects shared-col change",
           {((1,), "changed")}, rep)

    # 9. cross-check the SAME swap/NULL/intersection facts on DuckDB, to prove the SQL
    #    connector reproduces the contract (not just the Python reference).
    duck_ok = run_edges_duckdb(record)

    n_ok = sum(1 for c in cases if c["ok"])
    print("  edges passed: %d/%d (duckdb cross-check ok=%s)" %
          (n_ok, len(cases), duck_ok), flush=True)
    return {"cases": cases, "passed": n_ok, "total": len(cases),
            "duckdb_crosscheck_ok": duck_ok}


def run_edges_duckdb(record):
    """Re-run the load-bearing edges on a real DuckDB table to confirm the SQL
    connector reproduces the hashing contract for NULL, value-swap and '*' intersection.
    """
    db = os.path.join(HERE, "edges.duckdb")
    db2 = os.path.join(HERE, "edges2.duckdb")
    for p in (db, db2):
        if os.path.exists(p):
            os.remove(p)
    all_ok = True
    try:
        # value-swap on DuckDB
        csrc = duckdb.connect(db)
        csrc.execute("CREATE TABLE orders (id INTEGER, v VARCHAR)")
        csrc.execute("INSERT INTO orders VALUES (1, 'A'), (2, 'B')")
        csrc.close()
        ctgt = duckdb.connect(db2)
        ctgt.execute("CREATE TABLE orders (id INTEGER, v VARCHAR)")
        ctgt.execute("INSERT INTO orders VALUES (1, 'B'), (2, 'A')")  # swapped
        ctgt.close()
        s = DuckDBConnector(path=db, read_only=True)
        t = DuckDBConnector(path=db2, read_only=True)
        try:
            rep = compare(s, t, make_config())
        finally:
            s.close()
            t.close()
        all_ok &= record("[duckdb] value-swap between two keys (both changed)",
                         {((1,), "changed"), ((2,), "changed")}, rep)

        # NULL on one side on DuckDB
        for p in (db, db2):
            os.remove(p)
        csrc = duckdb.connect(db)
        csrc.execute("CREATE TABLE orders (id INTEGER, v INTEGER)")
        csrc.execute("INSERT INTO orders VALUES (1, 7)")
        csrc.close()
        ctgt = duckdb.connect(db2)
        ctgt.execute("CREATE TABLE orders (id INTEGER, v INTEGER)")
        ctgt.execute("INSERT INTO orders VALUES (1, NULL)")
        ctgt.close()
        s = DuckDBConnector(path=db, read_only=True)
        t = DuckDBConnector(path=db2, read_only=True)
        try:
            rep = compare(s, t, make_config())
        finally:
            s.close()
            t.close()
        all_ok &= record("[duckdb] NULL on one side of compared column (changed)",
                         {((1,), "changed")}, rep)

        # '*' intersection on DuckDB: source has an extra column the target lacks.
        for p in (db, db2):
            os.remove(p)
        csrc = duckdb.connect(db)
        csrc.execute("CREATE TABLE orders (id INTEGER, shared INTEGER, only_src VARCHAR)")
        csrc.execute("INSERT INTO orders VALUES (1, 100, 'x'), (2, 200, 'y')")
        csrc.close()
        ctgt = duckdb.connect(db2)
        ctgt.execute("CREATE TABLE orders (id INTEGER, shared INTEGER)")
        ctgt.execute("INSERT INTO orders VALUES (1, 100), (2, 200)")
        ctgt.close()
        s = DuckDBConnector(path=db, read_only=True)
        t = DuckDBConnector(path=db2, read_only=True)
        try:
            rep = compare(s, t, make_config(compare_columns=None))
        finally:
            s.close()
            t.close()
        all_ok &= record("[duckdb] compare_columns='*' intersection (extra col excluded)",
                         set(), rep)
    finally:
        for p in (db, db2):
            try:
                os.remove(p)
            except OSError:
                pass
    return bool(all_ok)


# =====================================================================================
# PART C - FUZZ
# =====================================================================================

N_TRIALS = 50
COLUMN_POOL = ["a", "b", "c", "d", "e"]


def _rand_value(rng):
    """A random comparable value drawn from a few types the contract canonicalizes."""
    kind = rng.randint(0, 4)
    if kind == 0:
        return rng.randint(-1000, 1000)
    if kind == 1:
        return round(rng.uniform(-1000, 1000), 4)
    if kind == 2:
        return rng.choice(["alpha", "beta", "gamma", "delta", "", "x y"])
    if kind == 3:
        return rng.choice([True, False])
    return None  # exercise NULLs


def _make_row(pk, cols, rng):
    row = {"id": pk}
    for c in cols:
        row[c] = _rand_value(rng)
    return row


def _mutate_value(rng, current):
    """Return a value guaranteed to differ from ``current`` (so a CHANGED is real)."""
    for _ in range(20):
        v = _rand_value(rng)
        if v != current:
            return v
    # extremely unlikely fallback: a sentinel string that differs from any pool value
    return "::changed-sentinel::"


def run_one_fuzz_trial(seed):
    """Run a single fuzz trial. Returns a result dict with ok flag and repro inputs."""
    rng = random.Random(seed)

    n = rng.randint(1000, 50000)
    # a few random columns (1..len(pool)); the compared set is the column intersection.
    n_cols = rng.randint(1, len(COLUMN_POOL))
    cols = COLUMN_POOL[:n_cols]

    # distinct random integer PKs across a sparse space, so splitting is exercised.
    pk_hi = n * rng.randint(2, 5)
    pks = rng.sample(range(1, pk_hi + 1), n)

    # identical source + target.
    src_by_key = {}
    for pk in pks:
        src_by_key[pk] = _make_row(pk, cols, rng)
    tgt_by_key = {pk: dict(row) for pk, row in src_by_key.items()}

    # decide how many of each drift kind, keeping the three disjoint and < n.
    max_each = max(1, n // 50)
    n_changed = rng.randint(0, max_each)
    n_missing = rng.randint(0, max_each)
    n_extra = rng.randint(0, max_each)

    truth = set()  # {(key_tuple, kind_str)}

    existing = list(src_by_key.keys())
    rng.shuffle(existing)
    cursor = 0

    # CHANGED: pick an existing key, mutate >=1 compared column in the TARGET.
    changed_keys = existing[cursor:cursor + n_changed]
    cursor += n_changed
    for pk in changed_keys:
        col = rng.choice(cols)
        tgt_by_key[pk][col] = _mutate_value(rng, tgt_by_key[pk][col])
        truth.add(((pk,), "changed"))

    # MISSING: present in source, delete from target.
    missing_keys = existing[cursor:cursor + n_missing]
    cursor += n_missing
    for pk in missing_keys:
        del tgt_by_key[pk]
        truth.add(((pk,), "missing"))

    # EXTRA: present only in target. Use fresh PKs not already used.
    used = set(src_by_key.keys())
    extra_keys = []
    attempts = 0
    while len(extra_keys) < n_extra and attempts < n_extra * 50 + 100:
        cand = rng.randint(pk_hi + 1, pk_hi * 3 + 10)
        attempts += 1
        if cand not in used:
            used.add(cand)
            extra_keys.append(cand)
            tgt_by_key[cand] = _make_row(cand, cols, rng)
            truth.add(((cand,), "extra"))

    src_rows = list(src_by_key.values())
    tgt_rows = list(tgt_by_key.values())

    # Randomize both knobs, but keep a single trial's COST bounded. The MemoryConnector
    # does a full O(N) scan per segment (it is the simple reference impl, not an indexed
    # store), so total work scales with segments * N. With drift present and a tiny
    # leaf_size the walk subdivides down to ~N/leaf_size leaves -> O(N^2/leaf_size),
    # which is pathological for the harness (NOT an engine bug - the engine is correct,
    # just doing many tiny full-scan segments). We therefore floor leaf_size so the
    # number of leaves stays modest, while still exercising small leaves on small N and
    # large leaves on large N. This keeps full knob coverage across the 50 trials.
    # Bound the leaf COUNT (~= key_span / leaf_size) to keep per-trial scans modest.
    # The reference connector rescans the whole table per segment, so each trial costs
    # roughly leaves * N row-visits; capping leaves at ~64 keeps the 50-trial suite to
    # ~1 minute. Small N still gets small leaves; large N is floored to a larger leaf.
    leaf_choices = [1, 50, 500, 2000, 5000, 20000]
    min_leaf = max(1, pk_hi // 64)
    leaf_size = rng.choice([c for c in leaf_choices if c >= min_leaf] or [leaf_choices[-1]])
    fanout = rng.choice([2, 4, 8, 16, 64])
    cfg = make_config(leaf_size=leaf_size, segment_fanout=fanout)

    src = MemoryConnector({"orders": src_rows})
    tgt = MemoryConnector({"orders": tgt_rows})
    report = compare(src, tgt, cfg)

    found = _drift_set(report)
    ok = found == truth

    result = {
        "seed": seed,
        "n": n,
        "cols": cols,
        "leaf_size": leaf_size,
        "fanout": fanout,
        "n_changed": len(changed_keys),
        "n_missing": len(missing_keys),
        "n_extra": len(extra_keys),
        "truth_total": len(truth),
        "found_total": len(found),
        "ok": bool(ok),
    }
    if not ok:
        miss = sorted([list(k) + [kind] for (k, kind) in (truth - found)])[:10]
        spur = sorted([list(k) + [kind] for (k, kind) in (found - truth)])[:10]
        result["expected_not_found_sample"] = miss
        result["found_not_expected_sample"] = spur
    return result


def run_fuzz():
    print("\n=== PART C: FUZZ (%d random trials, MemoryConnector) ===" % N_TRIALS,
          flush=True)
    results = []
    failures = []
    for i in range(N_TRIALS):
        seed = 1_000_000 + i  # stable, reproducible seeds
        r = run_one_fuzz_trial(seed)
        results.append(r)
        if not r["ok"]:
            failures.append(r)
            print("  [FAIL] seed=%d n=%d leaf=%d fanout=%d truth=%d found=%d" %
                  (seed, r["n"], r["leaf_size"], r["fanout"], r["truth_total"],
                   r["found_total"]), flush=True)
    n_pass = sum(1 for r in results if r["ok"])
    print("  fuzz pass rate: %d/%d" % (n_pass, N_TRIALS), flush=True)
    if failures:
        print("  FAILING SEEDS (reproduce with run_one_fuzz_trial(seed)):", flush=True)
        for f in failures:
            print("    seed=%d  %s" % (f["seed"], json.dumps(f)), flush=True)
    return {"trials": N_TRIALS, "passed": n_pass, "results": results,
            "failures": failures}


# =====================================================================================
# DRIVER
# =====================================================================================

def main():
    out = {}
    out["tuning"] = run_tuning()
    out["edges"] = run_edges()
    out["fuzz"] = run_fuzz()

    with open(OUT_JSON, "w", encoding="utf-8") as f:
        json.dump(out, f, indent=2)
    print("\nwrote %s" % OUT_JSON, flush=True)

    # concise final verdicts
    print("\n--- SUMMARY ---", flush=True)
    print("edges: %d/%d passed" % (out["edges"]["passed"], out["edges"]["total"]),
          flush=True)
    print("fuzz:  %d/%d passed" % (out["fuzz"]["passed"], out["fuzz"]["trials"]),
          flush=True)
    return out


if __name__ == "__main__":
    main()
