"""End-to-end: drive the real CLI through DuckDB on both sides.

Proves the whole pipeline (config -> connector registry -> engine -> reporter -> exit
code) on a self-contained local stand-in, no cloud creds. The Postgres->warehouse path
is covered by the connectors' own conformance tests; this proves the wiring.

Runnable via pytest and `python3 tests/test_e2e.py`.
"""

import contextlib
import datetime as dt
import io
import json
import os
import sys
import tempfile

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

try:
    import duckdb  # noqa: F401
    HAVE_DUCKDB = True
except Exception:  # noqa: BLE001
    HAVE_DUCKDB = False

if HAVE_DUCKDB:
    from driftwatch.cli import main  # noqa: E402


def _seed(path):
    con = duckdb.connect(path)
    try:
        con.execute(
            "CREATE TABLE orders (id INTEGER, name VARCHAR, amount DECIMAL(10,2), updated_at TIMESTAMP)"
        )
        base = dt.datetime(2026, 1, 1, 0, 0, 0)
        rows = [(i, "name-%d" % i, i * 1.5, base + dt.timedelta(minutes=i)) for i in range(1, 51)]
        con.executemany("INSERT INTO orders VALUES (?, ?, ?, ?)", rows)
    finally:
        con.close()


def _write_config(cfg_path, src_path, tgt_path):
    with open(cfg_path, "w", encoding="utf-8") as f:
        f.write(
            "connections:\n"
            "  source:\n"
            "    driver: duckdb\n"
            "    path: %s\n"
            "  target:\n"
            "    driver: duckdb\n"
            "    path: %s\n"
            "comparisons:\n"
            "  - name: orders\n"
            "    source_table: orders\n"
            "    target_table: orders\n"
            "    primary_key: [id]\n"
            "    compare_columns: \"*\"\n"
            "    recheck:\n"
            "      delay: 0s\n"
            "      rounds: 1\n" % (src_path, tgt_path)
        )


def _run_json(cfg_path):
    """Run the CLI, return (exit_code, parsed_json_report)."""
    buf = io.StringIO()
    with contextlib.redirect_stdout(buf):
        code = main(["run", "-c", cfg_path, "--format", "json"])
    out = buf.getvalue().strip()
    return code, json.loads(out)


def _drift_set(report):
    return {(tuple(d["key"]), d["kind"]) for d in report["drift_keys"]}


def test_e2e_in_sync_then_drift():
    if not HAVE_DUCKDB:
        print("SKIP test_e2e — duckdb not installed (pip install driftwatch[duckdb])")
        return
    with tempfile.TemporaryDirectory() as d:
        src = os.path.join(d, "src.duckdb")
        tgt = os.path.join(d, "tgt.duckdb")
        cfg = os.path.join(d, "driftwatch.yaml")
        _seed(src)
        _seed(tgt)
        _write_config(cfg, src, tgt)

        # 1) identical -> in sync, exit 0
        code, report = _run_json(cfg)
        assert code == 0, "expected exit 0 when in sync, got %d (%s)" % (code, report)
        assert report["in_sync"] is True
        assert report["drift_total"] == 0

        # 2) introduce three kinds of drift in the target
        con = duckdb.connect(tgt)
        try:
            con.execute("UPDATE orders SET name = 'CHANGED' WHERE id = 10")  # CHANGED
            con.execute("DELETE FROM orders WHERE id = 20")                   # MISSING in target
            con.execute(
                "INSERT INTO orders VALUES (999, 'phantom', 0.0, TIMESTAMP '2026-02-01 00:00:00')"
            )  # EXTRA in target
        finally:
            con.close()

        code, report = _run_json(cfg)
        assert code == 1, "expected exit 1 on drift, got %d (%s)" % (code, report)
        assert report["in_sync"] is False
        found = _drift_set(report)
        assert ((10,), "changed") in found, found
        assert ((20,), "missing") in found, found   # present in source, absent in target
        assert ((999,), "extra") in found, found     # present in target, absent in source
        assert report["drift_total"] == 3, found


def test_e2e_config_error_exit_3():
    if not HAVE_DUCKDB:
        print("SKIP test_e2e_config_error — duckdb not installed")
        return
    with tempfile.TemporaryDirectory() as d:
        cfg = os.path.join(d, "bad.yaml")
        with open(cfg, "w", encoding="utf-8") as f:
            f.write("connections: {}\n")  # missing source/target + comparisons
        buf = io.StringIO()
        with contextlib.redirect_stdout(buf):
            code = main(["run", "-c", cfg])
        assert code == 3, "expected exit 3 on bad config, got %d" % code


def test_e2e_init_prints_config():
    buf = io.StringIO()
    with contextlib.redirect_stdout(buf):
        code = main(["init"])
    assert code == 0
    assert "connections:" in buf.getvalue() and "comparisons:" in buf.getvalue()


if __name__ == "__main__":
    failures = 0
    for name, fn in sorted(globals().items()):
        if name.startswith("test_") and callable(fn):
            try:
                fn()
                print("PASS", name)
            except AssertionError as e:
                failures += 1
                print("FAIL", name, "-", e)
            except Exception as e:  # noqa: BLE001
                failures += 1
                print("ERROR", name, "-", type(e).__name__, e)
    print("\n%d failure(s)" % failures)
    sys.exit(1 if failures else 0)
