"""Snowflake connector conformance test - GATED on live credentials.

This test verifies that :class:`SnowflakeConnector` reproduces the
:mod:`driftwatch.hashing` contract *in-engine*, by comparing its outputs against
the Python reference (:class:`MemoryConnector`) over the same data.

It is entirely gated: it SKIPS cleanly (and loudly) unless BOTH

  1. ``snowflake-connector-python`` is importable, and
  2. the live-connection environment variables are present
     (at minimum ``SNOWFLAKE_ACCOUNT``, ``SNOWFLAKE_USER``, ``SNOWFLAKE_PASSWORD``).

Without a live Snowflake, conformance is UNVERIFIED - the test reports that and
skips rather than fabricating a pass.

Runnable two ways, like ``tests/test_foundation.py``:
    pytest tests/test_snowflake_connector.py
    python3 tests/test_snowflake_connector.py
"""

from __future__ import annotations

import datetime as dt
import os
import sys
import uuid
from decimal import Decimal

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from driftwatch.connectors.memory import MemoryConnector  # noqa: E402
from driftwatch.models import KeyRange  # noqa: E402

# --- gating ------------------------------------------------------------------

_REQUIRED_ENV = ("SNOWFLAKE_ACCOUNT", "SNOWFLAKE_USER", "SNOWFLAKE_PASSWORD")


def _driver_available() -> bool:
    try:
        import snowflake.connector  # noqa: F401
        return True
    except Exception:
        return False


def _creds_present() -> bool:
    return all(os.environ.get(k) for k in _REQUIRED_ENV)


def _skip_reason() -> str:
    if not _driver_available():
        return "snowflake-connector-python not importable (pip install driftwatch[snowflake])"
    missing = [k for k in _REQUIRED_ENV if not os.environ.get(k)]
    if missing:
        return "missing Snowflake credentials in env: %s" % ", ".join(missing)
    return ""


GATED_OUT = bool(_skip_reason())

try:  # pytest is optional (python3 entrypoint must also work)
    import pytest  # type: ignore

    pytestmark = pytest.mark.skipif(GATED_OUT, reason=_skip_reason() or "ungated")
except Exception:  # pragma: no cover - pytest not installed
    pytest = None  # type: ignore


# --- connection helpers ------------------------------------------------------

def _connect():
    from driftwatch.connectors.snowflake import SnowflakeConnector

    return SnowflakeConnector(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        password=os.environ["SNOWFLAKE_PASSWORD"],
        warehouse=os.environ.get("SNOWFLAKE_WAREHOUSE"),
        database=os.environ.get("SNOWFLAKE_DATABASE"),
        schema=os.environ.get("SNOWFLAKE_SCHEMA"),
        role=os.environ.get("SNOWFLAKE_ROLE"),
    )


def _sample_rows():
    return [
        {
            "id": 1,
            "name": "alice",
            "score": Decimal("1.2300"),
            "active": True,
            "updated_at": dt.datetime(2026, 1, 1, 0, 0, 0),
        },
        {
            "id": 2,
            "name": "bob",
            "score": Decimal("100"),
            "active": False,
            "updated_at": dt.datetime(2026, 1, 2, 12, 30, 45, 123456),
        },
        {
            "id": 3,
            "name": "carol",
            "score": Decimal("0.5"),
            "active": True,
            "updated_at": dt.datetime(2026, 1, 3, 0, 0, 0),
        },
    ]


_COMPARE_COLS = ["name", "score", "active"]
_PK = ["id"]


def _create_and_load(conn, table: str, rows):
    """Create a throwaway table and load the sample rows. Returns nothing.

    This is the ONLY place the test writes to Snowflake; the connector under test
    stays strictly read-only. We reuse the connector's live connection handle.
    """
    cur = conn._conn.cursor()  # noqa: SLF001 - test fixture reaches into the handle
    try:
        cur.execute(
            "CREATE OR REPLACE TEMPORARY TABLE {t} ("
            " id INTEGER, name STRING, score NUMBER(12,4),"
            " active BOOLEAN, updated_at TIMESTAMP_NTZ)".format(t=table)
        )
        cur.executemany(
            "INSERT INTO {t} (id, name, score, active, updated_at)"
            " VALUES (%s, %s, %s, %s, %s)".format(t=table),
            [
                (r["id"], r["name"], r["score"], r["active"], r["updated_at"])
                for r in rows
            ],
        )
    finally:
        cur.close()


# --- the conformance assertions ----------------------------------------------

def _assert_matches_reference(conn, table, rows):
    """The heart of the test: every connector method agrees with the reference."""
    ref = MemoryConnector({table: rows})
    full = KeyRange()

    # columns(): lowercased, includes our schema columns.
    cols = set(conn.columns(table))
    expected_cols = {"id", "name", "score", "active", "updated_at"}
    assert expected_cols.issubset(cols), (cols, expected_cols)

    # pk_bounds(): inclusive [min, max].
    assert conn.pk_bounds(table, _PK, None, None) == ref.pk_bounds(table, _PK, None, None)

    # checksum() over the whole table: count AND aggregate must match the reference.
    got = conn.checksum(table, _PK, _COMPARE_COLS, full, None, None, 12)
    exp = ref.checksum(table, _PK, _COMPARE_COLS, full, None, None, 12)
    assert got == exp, ("checksum", got, exp)

    # fetch_row_hashes(): per-row hashes must match exactly.
    got_h = conn.fetch_row_hashes(table, _PK, _COMPARE_COLS, full, None, None, 12)
    exp_h = ref.fetch_row_hashes(table, _PK, _COMPARE_COLS, full, None, None, 12)
    assert got_h == exp_h, ("row_hashes", got_h, exp_h)

    # half-open range [1,3) -> ids 1,2 only.
    rng = KeyRange(lo=(1,), hi=(3,))
    got_r = conn.fetch_row_hashes(table, _PK, _COMPARE_COLS, rng, None, None, 12)
    assert sorted(got_r.keys()) == [(1,), (2,)], got_r

    # watermark cutoff excludes id=3.
    cutoff = dt.datetime(2026, 1, 2, 13, 0, 0)
    got_c = conn.checksum(table, _PK, _COMPARE_COLS, full, "updated_at", cutoff, 12)
    exp_c = ref.checksum(table, _PK, _COMPARE_COLS, full, "updated_at", cutoff, 12)
    assert got_c == exp_c and got_c.count == 2, (got_c, exp_c)

    # recheck by explicit keys.
    got_k = conn.fetch_row_hashes_for_keys(table, _PK, _COMPARE_COLS, [(1,), (3,)], None, None, 12)
    exp_k = ref.fetch_row_hashes_for_keys(table, _PK, _COMPARE_COLS, [(1,), (3,)], None, None, 12)
    assert got_k == exp_k and sorted(got_k.keys()) == [(1,), (3,)], (got_k, exp_k)

    # split_points(): boundaries must match the reference oracle exactly (same
    # row-value ordering, same floor-division bucket positions), be strictly inside
    # (lo, hi) and strictly increasing. n=2 over 3 rows -> single boundary (2,).
    for n in (2, 3, 4):
        got_sp = conn.split_points(table, _PK, full, None, None, n)
        exp_sp = ref.split_points(table, _PK, full, None, None, n)
        assert got_sp == exp_sp, ("split_points", n, got_sp, exp_sp)
    # A range with <= 1 row cannot be split -> None on both sides.
    one_row = KeyRange(lo=(1,), hi=(2,))
    assert conn.split_points(table, _PK, one_row, None, None, 4) is None
    assert ref.split_points(table, _PK, one_row, None, None, 4) is None

    # keys_above_watermark(): strictly-greater-than cutoff, in range.
    # cutoff between id=2 and id=3 -> only id=3 is "above" (too fresh).
    wm_cutoff = dt.datetime(2026, 1, 2, 13, 0, 0)
    got_w = sorted(conn.keys_above_watermark(table, _PK, full, "updated_at", wm_cutoff))
    exp_w = sorted(ref.keys_above_watermark(table, _PK, full, "updated_at", wm_cutoff))
    assert got_w == exp_w == [(3,)], (got_w, exp_w)
    # No watermark column / no cutoff -> [] on both sides.
    assert conn.keys_above_watermark(table, _PK, full, None, None) == []
    assert conn.keys_above_watermark(table, _PK, full, "updated_at", None) == []


def test_snowflake_conformance():
    """End-to-end: load a temp table, assert the connector matches the reference."""
    if GATED_OUT:
        if pytest is not None:
            pytest.skip(_skip_reason())
        return  # python3 path: caller prints SKIP
    table = "DRIFTWATCH_CONF_" + uuid.uuid4().hex[:8].upper()
    conn = _connect()
    try:
        _create_and_load(conn, table, _sample_rows())
        _assert_matches_reference(conn, table, _sample_rows())
    finally:
        # TEMPORARY table is dropped on session close; close() is also exercised here.
        conn.close()


def test_snowflake_detects_change():
    """A changed compare-column must change the row hash / checksum."""
    if GATED_OUT:
        if pytest is not None:
            pytest.skip(_skip_reason())
        return
    table = "DRIFTWATCH_CHG_" + uuid.uuid4().hex[:8].upper()
    rows = _sample_rows()
    changed = [dict(r) for r in rows]
    changed[1]["name"] = "BOBBY"  # change id=2
    conn = _connect()
    try:
        _create_and_load(conn, table, rows)
        full = KeyRange()
        base = conn.fetch_row_hashes(table, _PK, _COMPARE_COLS, full, None, None, 12)
        _create_and_load(conn, table, changed)
        after = conn.fetch_row_hashes(table, _PK, _COMPARE_COLS, full, None, None, 12)
        assert base[(1,)] == after[(1,)]
        assert base[(2,)] != after[(2,)]
        assert base[(3,)] == after[(3,)]
    finally:
        conn.close()


def test_snowflake_split_points_composite():
    """split_points / keys_above_watermark over a COMPOSITE (text+int) key.

    Exercises tuple boundaries and lexicographic (row-value) ordering so the SQL
    ``ROW_NUMBER() OVER (ORDER BY a, b)`` path is validated against Python tuple
    comparison in the reference. GATED - SKIPS without a live Snowflake.
    """
    if GATED_OUT:
        if pytest is not None:
            pytest.skip(_skip_reason())
        return
    table = "DRIFTWATCH_SP_" + uuid.uuid4().hex[:8].upper()
    # (tenant, seq) composite PK; deliberately out of insertion order so ORDER BY matters.
    rows = [
        {"tenant": "acme", "seq": 2, "v": "b", "updated_at": dt.datetime(2026, 1, 5)},
        {"tenant": "acme", "seq": 1, "v": "a", "updated_at": dt.datetime(2026, 1, 1)},
        {"tenant": "acme", "seq": 10, "v": "c", "updated_at": dt.datetime(2026, 1, 9)},
        {"tenant": "beta", "seq": 1, "v": "d", "updated_at": dt.datetime(2026, 1, 2)},
        {"tenant": "beta", "seq": 3, "v": "e", "updated_at": dt.datetime(2026, 1, 8)},
    ]
    pk = ["tenant", "seq"]
    conn = _connect()
    try:
        cur = conn._conn.cursor()  # noqa: SLF001 - fixture writes; connector stays read-only
        try:
            cur.execute(
                "CREATE OR REPLACE TEMPORARY TABLE {t} ("
                " tenant STRING, seq INTEGER, v STRING, updated_at TIMESTAMP_NTZ)".format(t=table)
            )
            cur.executemany(
                "INSERT INTO {t} (tenant, seq, v, updated_at) VALUES (%s, %s, %s, %s)".format(t=table),
                [(r["tenant"], r["seq"], r["v"], r["updated_at"]) for r in rows],
            )
        finally:
            cur.close()

        ref = MemoryConnector({table: rows})
        full = KeyRange()
        for n in (2, 3, 5):
            got = conn.split_points(table, pk, full, None, None, n)
            exp = ref.split_points(table, pk, full, None, None, n)
            assert got == exp, ("composite split_points", n, got, exp)
            # boundaries strictly increasing tuples, strictly inside the full range
            if got:
                assert got == sorted(set(got)) and len(got) == len(set(got)), got

        cutoff = dt.datetime(2026, 1, 4)
        got_w = sorted(conn.keys_above_watermark(table, pk, full, "updated_at", cutoff))
        exp_w = sorted(ref.keys_above_watermark(table, pk, full, "updated_at", cutoff))
        # acme/10 (1-09), beta/3 (1-08) are strictly after 1-04 -> in flight.
        assert got_w == exp_w == [("acme", 10), ("beta", 3)], (got_w, exp_w)
    finally:
        conn.close()


if __name__ == "__main__":
    if GATED_OUT:
        print("SKIP test_snowflake_connector - UNVERIFIED:", _skip_reason())
        print("  (set %s + install driftwatch[snowflake] to run live)" % ", ".join(_REQUIRED_ENV))
        sys.exit(0)
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
