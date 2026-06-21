#!/usr/bin/env python3
"""Scenario: CROSS-ENGINE COLUMN-TYPE CONFORMANCE AT SCALE.

The question: across all the common column types, does a Postgres row and its DuckDB
copy of the SAME data really compare EQUAL (no false drift), and is per-type drift
caught? The float canonicalization is the documented sharp edge (hashing.py line 29);
this scenario probes it hard with tricky values (0.1, 1/3, 1e-7, 1e12, negative, -0.0).

Method (mirrors examples/benchmark.py's "generate in DuckDB, export CSV, COPY into PG"
recipe so both sides hold byte-identical data):

  1. Boot a throwaway Postgres in Docker on a UNIQUE port (55440), trap-cleaned on exit.
  2. Build a WIDE ~500k-row table in DuckDB covering: int PK, bigint, text (unicode +
     empty), nullable text (~30% NULL), numeric(12,2), int, double/float8 (tricky
     values), boolean, date, timestamp (microsecond), bytea/blob.
  3. Export that table to CSV and COPY the SAME CSV into an identically-typed Postgres
     table -> both sides hold identical data.
  4. TEST 1 (MATCHING): drive driftwatch.engine.compare with PostgresConnector (source)
     and DuckDBConnector (target) over the identical tables. MUST be in_sync. If not,
     a cross-engine canonicalization bug is found; drill into the diverging column(s).
  5. TEST 2 (PER-TYPE DRIFT): mutate one value of each type in the TARGET (duckdb) at
     known ids - numeric, double, timestamp, bool, NULL<->value both ways, text, bytea -
     and confirm each id shows as `changed`.
  6. TEST 3 (COLUMN SELECTION): inject a float-only drift, then prove that
     exclude_columns/compare_columns that drop the float column makes it disappear.

The bytea round-trip is made byte-exact by materialising the blob from a deterministic
hex string and writing that hex (Postgres ``\\x...`` escape form) into the CSV, then
loading it on both sides - so the only thing under test is the hashing contract, never
a CSV-encoding artefact.

Drives driftwatch.engine.compare directly (recheck rounds=0, leaf_size=5000). Writes
examples/scenarios/types_cross_engine.json. Does NOT modify src/.
"""

import json
import os
import subprocess
import sys
import tempfile
import time

import duckdb
import psycopg

REPO = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
SRC = os.path.join(REPO, "src")
sys.path.insert(0, SRC)

from driftwatch.config import ComparisonConfig, RecheckConfig  # noqa: E402
from driftwatch.connectors.duckdb import DuckDBConnector  # noqa: E402
from driftwatch.connectors.postgres import PostgresConnector  # noqa: E402
from driftwatch.engine import compare  # noqa: E402

N = 500_000
LEAF_SIZE = 5000
PG_PORT = 55440
PG_CONTAINER = "dw-types-pg"
PG_DSN = "postgresql://postgres:demo@localhost:%d/postgres" % PG_PORT

# Compare columns the engine will resolve from "*" (sorted, pk excluded). Listed here
# only for the column-selection test; for the matching/per-type tests we let the engine
# resolve "*" so the FULL set of types is exercised.
ALL_COMPARE = sorted(
    ["big", "name", "note", "price", "qty", "ratio", "active",
     "created_d", "created_ts", "payload"]
)

# Tricky float values cycled across rows by (id % len). -0.0 is the headline sharp edge.
TRICKY_FLOATS = [0.1, 1.0 / 3.0, 1e-7, 1e12, -2.5, -0.0, 123456.789, 9.999999999e11]


# --- Docker lifecycle --------------------------------------------------------


def _sh(args, **kw):
    return subprocess.run(args, capture_output=True, text=True, **kw)


def start_pg():
    _sh(["docker", "rm", "-f", PG_CONTAINER])  # clear any stale container
    r = _sh([
        "docker", "run", "-d", "--rm", "--name", PG_CONTAINER,
        "-e", "POSTGRES_PASSWORD=demo", "-p", "%d:5432" % PG_PORT, "postgres:16",
    ])
    if r.returncode != 0:
        raise RuntimeError("docker run failed: %s" % r.stderr.strip())
    # Wait for readiness via pg_isready inside the container.
    for _ in range(60):
        ready = _sh(["docker", "exec", PG_CONTAINER, "pg_isready", "-U", "postgres"])
        if ready.returncode == 0:
            break
        time.sleep(1)
    else:
        raise RuntimeError("postgres did not become ready in time")
    # pg_isready can pass a beat before the server accepts TCP auth; retry a real connect.
    for _ in range(30):
        try:
            with psycopg.connect(PG_DSN, connect_timeout=3) as c:
                c.execute("SELECT 1")
            return
        except Exception:
            time.sleep(1)
    raise RuntimeError("postgres accepted pg_isready but refused real connections")


def stop_pg():
    _sh(["docker", "rm", "-f", PG_CONTAINER])


# --- table build + load ------------------------------------------------------


def build_duckdb(path):
    """Create the wide ``t`` table in DuckDB and return (table_rows)."""
    con = duckdb.connect(path)
    con.execute("DROP TABLE IF EXISTS t")
    # Build the tricky-float lookup as a SQL list indexed by (id % k).
    flist = ", ".join(repr(f) for f in TRICKY_FLOATS)
    k = len(TRICKY_FLOATS)
    # name: mostly ascii, but every 5000th row gets unicode, every 7000th is empty ''.
    # note: ~30% NULL via (i % 10) < 3.
    # payload: deterministic 8-byte blob from the id, as a real BLOB.
    con.execute(
        "CREATE TABLE t AS SELECT "
        "  i AS id, "
        "  (i::BIGINT * 1000003) AS big, "
        "  CASE WHEN i %% 7000 = 0 THEN '' "
        "       WHEN i %% 5000 = 0 THEN 'náme-'||i||'-éñ☃' "
        "       ELSE 'name-'||i END AS name, "
        "  CASE WHEN (i %% 10) < 3 THEN NULL ELSE 'note-'||i END AS note, "
        "  CAST(((i * 7) %% 100000) / 100.0 AS DECIMAL(12,2)) AS price, "
        "  (i %% 1000) AS qty, "
        "  ([%s])[(i %% %d) + 1]::DOUBLE AS ratio, "
        "  (i %% 2 = 0) AS active, "
        "  (DATE '2020-01-01' + ((i %% 3000)::INTEGER)) AS created_d, "
        "  (TIMESTAMP '2026-06-01 00:00:00' + (i * INTERVAL 1 MICROSECOND) "
        "      + (i * INTERVAL 1 SECOND)) AS created_ts, "
        "  encode(printf('%%08x', i)) AS payload "  # 8-char hex string -> BLOB of its bytes
        "FROM range(1, %d + 1) t(i)" % (flist, k, N)
    )
    rows = con.execute("SELECT COUNT(*) FROM t").fetchone()[0]
    con.close()
    return rows


def export_csv_and_load_pg(duck_path, csv_path):
    """Export DuckDB ``t`` to CSV, create an identically-typed PG table, COPY it in.

    The blob column is exported as Postgres ``\\x``-prefixed hex so PG's bytea CSV input
    reads it byte-for-byte; DuckDB re-imports the same bytes on its side already (it owns
    the source table), so both sides are guaranteed identical.
    """
    con = duckdb.connect(duck_path)
    # For the CSV we render payload as PG bytea hex text: '\x' || hex(blob).
    # Everything else exports in its natural text form; timestamps keep microseconds.
    con.execute(
        "COPY (SELECT id, big, name, note, price, qty, ratio, active, created_d, "
        "      created_ts, '\\x' || lower(hex(payload)) AS payload "
        "      FROM t ORDER BY id) "
        "TO '%s' (FORMAT CSV, HEADER FALSE, NULLSTR '__NULL__')" % csv_path
    )
    con.close()

    with psycopg.connect(PG_DSN, autocommit=True) as conn:
        conn.execute("DROP TABLE IF EXISTS t")
        conn.execute(
            "CREATE TABLE t ("
            "  id int, big bigint, name text, note text, "
            "  price numeric(12,2), qty int, ratio double precision, "
            "  active boolean, created_d date, created_ts timestamp, payload bytea)"
        )
        with conn.cursor() as cur:
            with cur.copy(
                "COPY t (id,big,name,note,price,qty,ratio,active,created_d,created_ts,payload) "
                "FROM STDIN (FORMAT CSV, NULL '__NULL__')"
            ) as cp, open(csv_path, "rb") as f:
                for chunk in iter(lambda: f.read(1 << 20), b""):
                    cp.write(chunk)
        conn.execute("ALTER TABLE t ADD PRIMARY KEY (id)")


# --- comparison helpers ------------------------------------------------------


def make_cfg(name, compare_columns=None, exclude_columns=None):
    return ComparisonConfig(
        name=name,
        source_table="public.t",
        target_table="t",
        primary_key=["id"],
        compare_columns=compare_columns,  # None => "*"
        exclude_columns=exclude_columns or [],
        leaf_size=LEAF_SIZE,
        recheck=RecheckConfig(delay_seconds=0.0, rounds=0),
    )


def run_compare(cfg):
    source = PostgresConnector(dsn=PG_DSN)
    target = DuckDBConnector(path=DUCK_PATH, read_only=True)
    try:
        report = compare(source, target, cfg)
    finally:
        source.close()
        target.close()
    return report


# --- per-column isolation (used only if the matching test fails) -------------


def isolate_failing_columns():
    """If the full matching compare fails, find WHICH column(s) diverge by comparing
    one compare-column at a time (pk + single column). Returns a list of column names
    whose single-column compare is NOT in sync."""
    bad = []
    for col in ALL_COMPARE:
        cfg = make_cfg("isolate_%s" % col, compare_columns=[col])
        rep = run_compare(cfg)
        if not rep.in_sync:
            bad.append({"column": col, "drift_rows": len(rep.drift_keys)})
    return bad


def sample_divergence(col, limit=5):
    """For a diverging column, pull a few ids whose single-column row-hash differs and
    show the raw value on each side, so the report can name exact failing values."""
    cfg = make_cfg("sample_%s" % col, compare_columns=[col])
    source = PostgresConnector(dsn=PG_DSN)
    target = DuckDBConnector(path=DUCK_PATH, read_only=True)
    from driftwatch.models import KeyRange
    try:
        src = source.fetch_row_hashes("public.t", ["id"], [col], KeyRange(), None, None, 12)
        tgt = target.fetch_row_hashes("t", ["id"], [col], KeyRange(), None, None, 12)
        diff_ids = [k[0] for k in src if k in tgt and src[k] != tgt[k]][:limit]
        out = []
        for i in diff_ids:
            pv = source._conn  # noqa: SLF001 - read-only raw peek for evidence only
            with pv.cursor() as cur:
                cur.execute('SELECT %s FROM public.t WHERE id = %%s' % col, (i,))
                pgval = cur.fetchone()[0]
            pv.rollback()
            dv = target._con.execute(  # noqa: SLF001
                'SELECT %s FROM t WHERE id = ?' % col, [i]).fetchone()[0]
            out.append({"id": i, "postgres": repr(pgval), "duckdb": repr(dv)})
        return out
    finally:
        source.close()
        target.close()


# --- TEST 2: per-type drift injection ----------------------------------------


def inject_per_type_drift():
    """Mutate one value of each type in the TARGET (duckdb) at distinct known ids.
    Returns {id: type_label} for the rows we expect to be reported as `changed`."""
    con = duckdb.connect(DUCK_PATH)
    plan = {}  # id -> human label

    # pick well-separated ids that are guaranteed present (1..N) and distinct.
    ids = {
        "numeric": 1000,
        "double": 2000,
        "timestamp": 3000,
        "bool": 4000,
        "null_to_value": 5001,  # ids with (i%10)<3 are NULL note; 5001%10==1 -> NULL
        "value_to_null": 6005,  # 6005%10==5 -> non-NULL note -> set NULL
        "text": 7000,
        "bytea": 8000,
    }
    # Verify the NULL/non-NULL precondition on note for the flip ids.
    nv = con.execute("SELECT note FROM t WHERE id = ?", [ids["null_to_value"]]).fetchone()[0]
    vv = con.execute("SELECT note FROM t WHERE id = ?", [ids["value_to_null"]]).fetchone()[0]
    assert nv is None, "expected NULL note at id %d, got %r" % (ids["null_to_value"], nv)
    assert vv is not None, "expected non-NULL note at id %d" % ids["value_to_null"]

    con.execute("UPDATE t SET price = price + 0.01 WHERE id = ?", [ids["numeric"]])
    con.execute("UPDATE t SET ratio = ratio + 1.0 WHERE id = ?", [ids["double"]])
    con.execute(
        "UPDATE t SET created_ts = created_ts + INTERVAL 1 MICROSECOND WHERE id = ?",
        [ids["timestamp"]],
    )
    con.execute("UPDATE t SET active = NOT active WHERE id = ?", [ids["bool"]])
    con.execute("UPDATE t SET note = 'filled-in' WHERE id = ?", [ids["null_to_value"]])
    con.execute("UPDATE t SET note = NULL WHERE id = ?", [ids["value_to_null"]])
    con.execute("UPDATE t SET name = name || '-DRIFT' WHERE id = ?", [ids["text"]])
    # Replace the blob with a clearly different byte sequence (same length region) so the
    # bytea/blob hash must change. Original payload at this id is encode('%08x' % id).
    con.execute(
        "UPDATE t SET payload = encode('ZZ' || printf('%%06x', id)) WHERE id = ?",
        [ids["bytea"]],
    )
    con.close()

    for label, i in ids.items():
        plan[i] = label
    return ids, plan


# --- main --------------------------------------------------------------------

DUCK_PATH = None  # set in main; referenced by helpers


def main():
    global DUCK_PATH
    work = tempfile.mkdtemp(prefix="driftwatch-types-")
    DUCK_PATH = os.path.join(work, "warehouse.duckdb")
    csv_path = os.path.join(work, "data.csv")

    print("### CROSS-ENGINE COLUMN-TYPE CONFORMANCE (N=%d) ###" % N, flush=True)
    print("work dir: %s" % work, flush=True)

    out = {
        "scenario": "types_cross_engine",
        "n_rows": N,
        "leaf_size": LEAF_SIZE,
        "recheck_rounds": 0,
        "pg_port": PG_PORT,
        "columns": ["id", "big", "name", "note", "price", "qty", "ratio",
                    "active", "created_d", "created_ts", "payload"],
        "tricky_floats": TRICKY_FLOATS,
    }

    started = False
    try:
        print("booting postgres on port %d ..." % PG_PORT, flush=True)
        start_pg()
        started = True

        t0 = time.time()
        rows = build_duckdb(DUCK_PATH)
        export_csv_and_load_pg(DUCK_PATH, csv_path)
        out["table_rows"] = rows
        out["load_seconds"] = round(time.time() - t0, 2)
        print("loaded both sides (%d rows) in %.1fs" % (rows, time.time() - t0), flush=True)

        # --- TEST 1: MATCHING -------------------------------------------------
        print("\n[TEST 1] matching identical wide table (must be in_sync) ...", flush=True)
        rep = run_compare(make_cfg("matching"))
        match = {
            "in_sync": rep.in_sync,
            "rows_compared": rep.rows_compared,
            "segments_scanned": rep.segments_scanned,
            "drift_rows": len(rep.drift_keys),
            "engine_seconds": round(rep.duration_seconds, 3),
        }
        print("  in_sync=%s  rows_compared=%d  segments=%d  drift_rows=%d  engine=%.3fs"
              % (rep.in_sync, rep.rows_compared, rep.segments_scanned,
                 len(rep.drift_keys), rep.duration_seconds), flush=True)
        if not rep.in_sync:
            print("  !! MATCHING TABLE IS NOT IN SYNC - isolating diverging column(s)...",
                  flush=True)
            bad = isolate_failing_columns()
            match["diverging_columns"] = bad
            samples = {}
            for b in bad:
                samples[b["column"]] = sample_divergence(b["column"])
            match["divergence_samples"] = samples
            for b in bad:
                print("     diverging column %r (%d rows): %s"
                      % (b["column"], b["drift_rows"], samples.get(b["column"])), flush=True)
        out["test1_matching"] = match

        # --- FLOAT VERDICT (isolate the ratio column alone) -------------------
        print("\n[FLOAT] isolating the float8 'ratio' column on identical data ...",
              flush=True)
        frep = run_compare(make_cfg("float_only", compare_columns=["ratio"]))
        float_verdict = {
            "column": "ratio",
            "tricky_values": TRICKY_FLOATS,
            "in_sync": frep.in_sync,
            "drift_rows": len(frep.drift_keys),
            "exact_match": frep.in_sync,
        }
        if not frep.in_sync:
            float_verdict["failing_samples"] = sample_divergence("ratio")
        # Demonstrate the documented sharp edge directly: a sub-precision delta on a
        # large-magnitude float is INVISIBLE at 12 significant digits, so PG and DuckDB
        # both canonicalise it identically and driftwatch (correctly, by contract) reports
        # no drift. We compute this in Python against hashing.canonical's exact rule.
        from driftwatch.hashing import canonical
        big = 9.999999999e11
        masked = big + 0.5
        float_verdict["precision_masking_demo"] = {
            "value": big,
            "value_plus_0_5": masked,
            "canonical_value": canonical(big),
            "canonical_value_plus_0_5": canonical(masked),
            "delta_visible_at_12_sig_digits": canonical(big) != canonical(masked),
            "note": ("A 0.5 change to a ~1e12 float is below 12 significant digits, so "
                     "both engines emit the same %g text and the row hashes match - this "
                     "is the documented float sharp edge, not a cross-engine bug."),
        }
        print("  ratio exact_match=%s  drift_rows=%d" % (frep.in_sync, len(frep.drift_keys)),
              flush=True)
        print("  sharp-edge demo: canon(%r)=%r ; canon(%r+0.5)=%r ; visible=%s"
              % (big, canonical(big), big, canonical(masked),
                 canonical(big) != canonical(masked)), flush=True)
        out["float_verdict"] = float_verdict

        # --- TEST 2: PER-TYPE DRIFT ------------------------------------------
        print("\n[TEST 2] per-type drift injection into target (each must be `changed`) ...",
              flush=True)
        ids, plan = inject_per_type_drift()
        rep2 = run_compare(make_cfg("per_type_drift"))
        found = {dk.key[0]: dk.kind.value for dk in rep2.drift_keys}
        per_type = []
        # map id -> type label for readable table
        type_for_id = {i: label for label, i in ids.items()}
        for label, i in ids.items():
            kind = found.get(i)
            per_type.append({
                "type": label,
                "id": i,
                "reported_kind": kind,
                "caught": kind == "changed",
            })
            print("  %-16s id=%-6d reported=%-8s caught=%s"
                  % (label, i, kind, kind == "changed"), flush=True)
        # any UNEXPECTED drift keys (should be exactly the injected ids)?
        unexpected = sorted(k for k in found if k not in type_for_id)
        out["test2_per_type"] = {
            "results": per_type,
            "all_caught": all(p["caught"] for p in per_type),
            "drift_total": len(found),
            "unexpected_drift_ids": unexpected[:20],
            "unexpected_count": len(unexpected),
        }
        print("  all_caught=%s  total_drift=%d  unexpected=%d"
              % (out["test2_per_type"]["all_caught"], len(found), len(unexpected)),
              flush=True)

        # --- TEST 3: COLUMN SELECTION (exclude/subset the float) -------------
        # At this point the target already has per-type drift incl. the double at id 2000.
        # Re-build a CLEAN warehouse + a float-only drift to prove column selection in
        # isolation (otherwise other-type drift would mask the effect).
        print("\n[TEST 3] column selection: excluding the float makes float-only drift vanish",
              flush=True)
        # rebuild clean duck warehouse (PG side stays the identical original)
        rows2 = build_duckdb(DUCK_PATH)
        assert rows2 == rows
        # Inject ONLY a float drift at ids whose canonical %g form PROVABLY changes.
        # We pick ids landing on float-bucket 0 (ratio == 0.1) and add a delta that is
        # visible at 12 significant digits, so the row hash must differ. (Picking ids on
        # huge-magnitude buckets like 9.999e11 would add a sub-precision delta that %g
        # rounds away - a real, documented sharp edge demonstrated in the report - so we
        # deliberately avoid those here to make the column-selection proof unambiguous.)
        k = len(TRICKY_FLOATS)
        # ids with (id % k) == 0 sit on TRICKY_FLOATS[0] == 0.1 (a small, fully-precise val)
        float_drift_ids = [i for i in (40000, 80000, 120000) if i % k == 0][:3]
        if len(float_drift_ids) < 3:  # fallback: just walk to find 3 bucket-0 ids
            float_drift_ids = [i for i in range(1, N) if i % k == 0][:3]
        fcon = duckdb.connect(DUCK_PATH)
        for i in float_drift_ids:
            # +0.25 to 0.1 -> 0.35, visibly different at 12 sig digits.
            fcon.execute("UPDATE t SET ratio = ratio + 0.25 WHERE id = ?", [i])
        fcon.close()

        with_float = run_compare(make_cfg("with_float"))  # "*"
        excl_float = run_compare(make_cfg("exclude_float", exclude_columns=["ratio"]))
        subset_no_float = run_compare(
            make_cfg("subset_no_float", compare_columns=[c for c in ALL_COMPARE if c != "ratio"])
        )
        out["test3_column_selection"] = {
            "float_drift_ids": float_drift_ids,
            "with_float_in_sync": with_float.in_sync,
            "with_float_drift_rows": len(with_float.drift_keys),
            "exclude_float_in_sync": excl_float.in_sync,
            "exclude_float_drift_rows": len(excl_float.drift_keys),
            "subset_without_float_in_sync": subset_no_float.in_sync,
            "subset_without_float_drift_rows": len(subset_no_float.drift_keys),
            "proves_selection": (not with_float.in_sync) and excl_float.in_sync
                                 and subset_no_float.in_sync,
        }
        s = out["test3_column_selection"]
        print("  with float:        in_sync=%s (drift=%d)"
              % (s["with_float_in_sync"], s["with_float_drift_rows"]), flush=True)
        print("  exclude_columns:   in_sync=%s (drift=%d)"
              % (s["exclude_float_in_sync"], s["exclude_float_drift_rows"]), flush=True)
        print("  compare subset:    in_sync=%s (drift=%d)"
              % (s["subset_without_float_in_sync"], s["subset_without_float_drift_rows"]),
              flush=True)
        print("  proves_selection=%s" % s["proves_selection"], flush=True)

    finally:
        if started:
            print("\ncleaning up postgres container ...", flush=True)
        stop_pg()

    out_path = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                            "types_cross_engine.json")
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(out, f, indent=2)
    print("\nwrote %s" % out_path, flush=True)


if __name__ == "__main__":
    main()
