"""Conformance test: DuckDBConnector must match MemoryConnector byte-for-byte.

The MemoryConnector is the canonical Python implementation of the hashing contract.
This test loads *identical* data into both a MemoryConnector and an in-memory
DuckDBConnector and asserts that ``checksum`` (count AND checksum), ``fetch_row_hashes``,
``fetch_row_hashes_for_keys``, ``pk_bounds``, and ``columns`` agree exactly across a
variety of types, ranges, watermark cutoffs, and half-open boundaries - including a
composite-key table.

If the two connectors disagree on a single digest, the contract is not reproduced and
the test fails. DuckDB is an optional dependency, so the whole module SKIPs cleanly when
it is not importable (after attempting an install); it never fakes success.

Runnable two ways:
    pytest tests/test_duckdb_connector.py
    python3 tests/test_duckdb_connector.py
"""

import datetime as dt
import os
import sys
from decimal import Decimal

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from driftwatch.connectors.memory import MemoryConnector  # noqa: E402
from driftwatch.models import KeyRange  # noqa: E402


# --- DuckDB availability (attempt install, else skip; never fake) --------------

def _ensure_duckdb():
    try:
        import duckdb  # noqa: F401
        return True
    except ImportError:
        pass
    # Best-effort install, then retry the import once.
    try:
        import subprocess
        subprocess.run(
            [sys.executable, "-m", "pip", "install", "duckdb"],
            check=False, capture_output=True,
        )
        import duckdb  # noqa: F401
        return True
    except Exception:
        return False


_DUCKDB_OK = _ensure_duckdb()

if not _DUCKDB_OK:
    import pytest  # type: ignore
    pytestmark = pytest.mark.skip(reason="duckdb not installable in this environment")


# --- test dataset --------------------------------------------------------------

# A single-key "orders" table exercising every canonicalization branch:
# int pk, text (with a NULL), a DECIMAL (trailing-zero trimming), a TIMESTAMP, a
# BOOLEAN, a DOUBLE (the float sharp edge), a DATE, and a BLOB. ``updated_at`` is the
# watermark column.
_ORDERS = [
    {
        "id": 1, "name": "alice", "amount": Decimal("1.2300"),
        "updated_at": dt.datetime(2026, 1, 1, 0, 0, 0),
        "flag": True, "score": 3.14159265358979,
        "d": dt.date(2026, 1, 1), "payload": b"\x00\xff",
    },
    {
        "id": 2, "name": "bob", "amount": Decimal("100"),
        "updated_at": dt.datetime(2026, 1, 2, 12, 30, 45, 6),
        "flag": False, "score": 0.1,
        "d": dt.date(2026, 2, 14), "payload": b"\xde\xad\xbe\xef",
    },
    {
        "id": 3, "name": None, "amount": Decimal("0.5000"),
        "updated_at": dt.datetime(2026, 1, 3, 23, 59, 59, 999999),
        "flag": True, "score": 1234567.891234567,
        "d": dt.date(2026, 12, 31), "payload": None,
    },
    {
        "id": 4, "name": "diana", "amount": Decimal("-12.340"),
        "updated_at": None,  # NULL watermark -> excluded under any cutoff
        "flag": False, "score": 1e20,
        "d": dt.date(2025, 6, 15), "payload": b"",
    },
    {
        "id": 5, "name": "eve", "amount": None,
        "updated_at": dt.datetime(2026, 1, 5, 8, 0, 0),
        "flag": None, "score": -0.0,
        "d": None, "payload": b"\x10\x20\x30",
    },
]

# A composite-key (region, id) table for half-open tuple-range coverage.
_EVENTS = [
    {"region": "eu", "id": 1, "kind": "click", "n": 10},
    {"region": "eu", "id": 2, "kind": "view", "n": 20},
    {"region": "us", "id": 1, "kind": "click", "n": 30},
    {"region": "us", "id": 2, "kind": "scroll", "n": 40},
    {"region": "us", "id": 3, "kind": "view", "n": 50},
]


def _make_duckdb():
    """Build an in-memory DuckDB with the same rows, typed to match the contract."""
    from driftwatch.connectors.duckdb import DuckDBConnector

    c = DuckDBConnector(path=":memory:")
    con = c._con
    con.execute(
        "CREATE TABLE orders("
        "id INTEGER, name VARCHAR, amount DECIMAL(10,4), updated_at TIMESTAMP, "
        "flag BOOLEAN, score DOUBLE, d DATE, payload BLOB)"
    )
    for r in _ORDERS:
        con.execute(
            "INSERT INTO orders VALUES (?,?,?,?,?,?,?,?)",
            [r["id"], r["name"], r["amount"], r["updated_at"],
             r["flag"], r["score"], r["d"], r["payload"]],
        )
    con.execute(
        "CREATE TABLE events(region VARCHAR, id INTEGER, kind VARCHAR, n INTEGER)"
    )
    for r in _EVENTS:
        con.execute(
            "INSERT INTO events VALUES (?,?,?,?)",
            [r["region"], r["id"], r["kind"], r["n"]],
        )
    return c


def _make_memory():
    return MemoryConnector({"orders": list(_ORDERS), "events": list(_EVENTS)})


# --- assertions ----------------------------------------------------------------

def _assert_checksum_equal(mem, duck, table, pk, cmp, rng, wm, cutoff, fp=12):
    m = mem.checksum(table, pk, cmp, rng, wm, cutoff, fp)
    d = duck.checksum(table, pk, cmp, rng, wm, cutoff, fp)
    assert m.count == d.count, (
        f"count mismatch on {table} rng={rng} cutoff={cutoff}: mem={m.count} duck={d.count}"
    )
    assert m.checksum == d.checksum, (
        f"checksum mismatch on {table} rng={rng} cutoff={cutoff}: "
        f"mem={m.checksum} duck={d.checksum}"
    )


def _assert_hashes_equal(mem, duck, table, pk, cmp, rng, wm, cutoff, fp=12):
    m = mem.fetch_row_hashes(table, pk, cmp, rng, wm, cutoff, fp)
    d = duck.fetch_row_hashes(table, pk, cmp, rng, wm, cutoff, fp)
    assert m == d, (
        f"row hashes mismatch on {table} rng={rng} cutoff={cutoff}:\n"
        f"  mem ={m}\n  duck={d}"
    )


# --- tests ---------------------------------------------------------------------

def test_columns_match():
    mem, duck = _make_memory(), _make_duckdb()
    assert duck.columns("orders") == mem.columns("orders")
    assert duck.columns("orders") == sorted(
        ["id", "name", "amount", "updated_at", "flag", "score", "d", "payload"]
    )
    assert duck.columns("events") == mem.columns("events")
    duck.close()


def test_pk_bounds_match():
    mem, duck = _make_memory(), _make_duckdb()
    assert duck.pk_bounds("orders", ["id"], None, None) == \
        mem.pk_bounds("orders", ["id"], None, None)
    assert duck.pk_bounds("orders", ["id"], None, None) == KeyRange(lo=(1,), hi=(5,))
    # composite key bounds
    assert duck.pk_bounds("events", ["region", "id"], None, None) == \
        mem.pk_bounds("events", ["region", "id"], None, None)
    # bounds under a cutoff (excludes id=4 NULL watermark + id>cutoff)
    cutoff = dt.datetime(2026, 1, 3, 0, 0, 0)
    assert duck.pk_bounds("orders", ["id"], "updated_at", cutoff) == \
        mem.pk_bounds("orders", ["id"], "updated_at", cutoff)
    duck.close()


def test_pk_bounds_empty_under_cutoff():
    mem, duck = _make_memory(), _make_duckdb()
    early = dt.datetime(2020, 1, 1, 0, 0, 0)  # before any row
    assert duck.pk_bounds("orders", ["id"], "updated_at", early) is None
    assert mem.pk_bounds("orders", ["id"], "updated_at", early) is None
    duck.close()


def test_full_range_all_types():
    mem, duck = _make_memory(), _make_duckdb()
    cmp = ["name", "amount", "updated_at", "flag", "score", "d", "payload"]
    full = KeyRange()
    _assert_checksum_equal(mem, duck, "orders", ["id"], cmp, full, None, None)
    _assert_hashes_equal(mem, duck, "orders", ["id"], cmp, full, None, None)
    # also confirm the full segment count is all 5 rows
    assert duck.checksum("orders", ["id"], cmp, full, None, None, 12).count == 5
    duck.close()


def test_sub_ranges():
    mem, duck = _make_memory(), _make_duckdb()
    cmp = ["name", "amount", "score"]
    for rng in [
        KeyRange(lo=(2,), hi=(4,)),     # [2,4) -> ids 2,3
        KeyRange(lo=(1,), hi=(2,)),     # [1,2) -> id 1 only
        KeyRange(lo=(3,), hi=None),     # [3, +inf) -> ids 3,4,5
        KeyRange(lo=None, hi=(3,)),     # (-inf, 3) -> ids 1,2
        KeyRange(lo=(5,), hi=(5,)),     # empty (lo==hi, half-open)
        KeyRange(lo=(10,), hi=(20,)),   # empty (out of range)
    ]:
        _assert_checksum_equal(mem, duck, "orders", ["id"], cmp, rng, None, None)
        _assert_hashes_equal(mem, duck, "orders", ["id"], cmp, rng, None, None)
    duck.close()


def test_half_open_boundaries():
    """Boundary rows: lo is inclusive, hi is exclusive."""
    mem, duck = _make_memory(), _make_duckdb()
    cmp = ["name"]
    rng = KeyRange(lo=(2,), hi=(4,))
    m = mem.fetch_row_hashes("orders", ["id"], cmp, rng, None, None, 12)
    d = duck.fetch_row_hashes("orders", ["id"], cmp, rng, None, None, 12)
    assert sorted(m.keys()) == [(2,), (3,)]  # 2 included, 4 excluded
    assert m == d
    duck.close()


def test_watermark_cutoff():
    mem, duck = _make_memory(), _make_duckdb()
    cmp = ["name", "amount"]
    for cutoff in [
        dt.datetime(2026, 1, 2, 12, 30, 45, 6),   # includes ids 1,2 (exact boundary)
        dt.datetime(2026, 1, 3, 0, 0, 0),          # includes ids 1,2 (3 is later that day)
        dt.datetime(2026, 1, 5, 8, 0, 0),          # includes 1,2,3,5 (4 has NULL wm)
    ]:
        _assert_checksum_equal(mem, duck, "orders", ["id"], cmp, KeyRange(), "updated_at", cutoff)
        _assert_hashes_equal(mem, duck, "orders", ["id"], cmp, KeyRange(), "updated_at", cutoff)
    # NULL-watermark row (id=4) is always excluded when a cutoff is set
    far = dt.datetime(2030, 1, 1)
    d = duck.fetch_row_hashes("orders", ["id"], cmp, KeyRange(), "updated_at", far, 12)
    assert (4,) not in d
    assert (4,) not in mem.fetch_row_hashes("orders", ["id"], cmp, KeyRange(), "updated_at", far, 12)
    duck.close()


def test_range_and_cutoff_combined():
    mem, duck = _make_memory(), _make_duckdb()
    cmp = ["name", "amount", "flag"]
    rng = KeyRange(lo=(1,), hi=(5,))
    cutoff = dt.datetime(2026, 1, 3, 23, 59, 59, 999999)
    _assert_checksum_equal(mem, duck, "orders", ["id"], cmp, rng, "updated_at", cutoff)
    _assert_hashes_equal(mem, duck, "orders", ["id"], cmp, rng, "updated_at", cutoff)
    duck.close()


def test_fetch_row_hashes_for_keys():
    mem, duck = _make_memory(), _make_duckdb()
    cmp = ["name", "amount", "score"]
    keys = [(1,), (3,), (5,), (99,)]  # 99 absent
    m = mem.fetch_row_hashes_for_keys("orders", ["id"], cmp, keys, None, None, 12)
    d = duck.fetch_row_hashes_for_keys("orders", ["id"], cmp, keys, None, None, 12)
    assert m == d
    assert sorted(d.keys()) == [(1,), (3,), (5,)]
    # with cutoff applied
    cutoff = dt.datetime(2026, 1, 3, 0, 0, 0)
    m2 = mem.fetch_row_hashes_for_keys("orders", ["id"], cmp, keys, "updated_at", cutoff, 12)
    d2 = duck.fetch_row_hashes_for_keys("orders", ["id"], cmp, keys, "updated_at", cutoff, 12)
    assert m2 == d2
    duck.close()


def test_composite_key_full_and_ranges():
    mem, duck = _make_memory(), _make_duckdb()
    pk = ["region", "id"]
    cmp = ["kind", "n"]
    _assert_checksum_equal(mem, duck, "events", pk, cmp, KeyRange(), None, None)
    _assert_hashes_equal(mem, duck, "events", pk, cmp, KeyRange(), None, None)
    for rng in [
        KeyRange(lo=("eu", 2), hi=("us", 2)),   # (eu,2),(us,1)
        KeyRange(lo=("us", 1), hi=None),         # all us
        KeyRange(lo=None, hi=("us", 1)),         # all eu
        KeyRange(lo=("eu", 1), hi=("eu", 2)),    # just (eu,1)
        KeyRange(lo=("us", 2), hi=("us", 2)),    # empty
    ]:
        _assert_checksum_equal(mem, duck, "events", pk, cmp, rng, None, None)
        _assert_hashes_equal(mem, duck, "events", pk, cmp, rng, None, None)
    duck.close()


def test_composite_key_for_keys():
    mem, duck = _make_memory(), _make_duckdb()
    pk = ["region", "id"]
    cmp = ["kind", "n"]
    keys = [("eu", 1), ("us", 3), ("xx", 9)]  # last absent
    m = mem.fetch_row_hashes_for_keys("events", pk, cmp, keys, None, None, 12)
    d = duck.fetch_row_hashes_for_keys("events", pk, cmp, keys, None, None, 12)
    assert m == d
    assert sorted(d.keys()) == [("eu", 1), ("us", 3)]
    duck.close()


def test_schema_qualified_table():
    """A schema-qualified name (main.orders) must behave like the bare name."""
    mem, duck = _make_memory(), _make_duckdb()
    cmp = ["name", "amount"]
    full = KeyRange()
    m = mem.checksum("orders", ["id"], cmp, full, None, None, 12)
    d = duck.checksum("main.orders", ["id"], cmp, full, None, None, 12)
    assert (m.count, m.checksum) == (d.count, d.checksum)
    assert duck.columns("main.orders") == duck.columns("orders")
    duck.close()


def test_float_precision_variants():
    """The float sharp edge across several precision settings."""
    mem, duck = _make_memory(), _make_duckdb()
    cmp = ["score"]
    full = KeyRange()
    for fp in [6, 8, 12, 15, 17]:
        _assert_checksum_equal(mem, duck, "orders", ["id"], cmp, full, None, None, fp=fp)
        _assert_hashes_equal(mem, duck, "orders", ["id"], cmp, full, None, None, fp=fp)
    duck.close()


def test_empty_segment_checksum_zero():
    mem, duck = _make_memory(), _make_duckdb()
    rng = KeyRange(lo=(100,), hi=(200,))
    m = mem.checksum("orders", ["id"], ["name"], rng, None, None, 12)
    d = duck.checksum("orders", ["id"], ["name"], rng, None, None, 12)
    assert m.count == d.count == 0
    assert m.checksum == d.checksum == 0
    duck.close()


if __name__ == "__main__":
    if not _DUCKDB_OK:
        print("SKIP: duckdb not available; could NOT verify conformance.")
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
