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


# --- pruning / lag helpers: split_points & keys_above_watermark ----------------
#
# These exercise the two pruning/lag methods at SCALE (~50k rows) across an
# integer-PK table, a composite-PK table, and a text-PK table, in BOTH the
# MemoryConnector (canonical Python reference) and the DuckDBConnector (SQL).
#
#   * keys_above_watermark must equal the Memory reference EXACTLY (as a set) for
#     several cutoffs - the in-flight set the engine drops must be identical.
#   * split_points boundaries must be strictly increasing, strictly inside (lo, hi),
#     and the half-open sub-ranges they form must PARTITION the rows exactly: the
#     per-sub-range counts sum to the total with no gaps and no overlaps. Boundaries
#     need not equal Memory's (the spec only requires a valid equal-ish split).
#   * split_points returns None on empty and single-row ranges.

_BIG_N = 50_000


def _big_int_rows():
    """integer PK table: id 1..N, a watermark (one third in flight), a payload."""
    rows = []
    for i in range(1, _BIG_N + 1):
        rows.append({
            "id": i,
            "val": (i * 7919) % 100003,        # arbitrary compared column
            # watermark: ids divisible by 3 sit "in the future" relative to a mid cutoff
            "updated_at": dt.datetime(2026, 1, 1) + dt.timedelta(seconds=(i % 90000)),
        })
    return rows


def _big_composite_rows():
    """composite PK (region, id): a few regions x many ids, watermark spread."""
    regions = ["af", "ap", "eu", "na", "sa"]
    rows = []
    n_per = _BIG_N // len(regions)
    for r_idx, region in enumerate(regions):
        for i in range(1, n_per + 1):
            seq = r_idx * n_per + i
            rows.append({
                "region": region,
                "id": i,
                "kind": "k%d" % (seq % 7),
                "updated_at": dt.datetime(2026, 1, 1) + dt.timedelta(seconds=(seq % 90000)),
            })
    return rows


def _big_text_rows():
    """text PK: zero-padded string keys so lexicographic order is well-defined."""
    rows = []
    for i in range(1, _BIG_N + 1):
        rows.append({
            "k": "key-%08d" % i,               # 'key-00000001' ...
            "val": (i * 104729) % 100003,
            "updated_at": dt.datetime(2026, 1, 1) + dt.timedelta(seconds=(i % 90000)),
        })
    return rows


_BIG_INT = _big_int_rows()
_BIG_COMPOSITE = _big_composite_rows()
_BIG_TEXT = _big_text_rows()


# The 50k-row tables are READ-ONLY for both methods under test (split_points /
# keys_above_watermark do no DML), so we build each connector ONCE and share it across
# every test. Re-loading 50k rows into DuckDB per test was the dominant cost; caching
# turns a multi-minute run into seconds.
_BIG_MEMORY = None
_BIG_DUCKDB = None


def _make_big_memory():
    global _BIG_MEMORY
    if _BIG_MEMORY is None:
        _BIG_MEMORY = MemoryConnector({
            "big_int": list(_BIG_INT),
            "big_composite": list(_BIG_COMPOSITE),
            "big_text": list(_BIG_TEXT),
        })
    return _BIG_MEMORY


def _make_big_duckdb():
    global _BIG_DUCKDB
    if _BIG_DUCKDB is not None:
        return _BIG_DUCKDB
    from driftwatch.connectors.duckdb import DuckDBConnector

    c = DuckDBConnector(path=":memory:")
    con = c._con
    # Bulk-load via a registered Python relation: one columnar INSERT ... SELECT per
    # table is far faster than 50k parameterized INSERTs.
    con.execute("CREATE TABLE big_int(id BIGINT, val BIGINT, updated_at TIMESTAMP)")
    _int_rows = [(r["id"], r["val"], r["updated_at"]) for r in _BIG_INT]
    con.executemany("INSERT INTO big_int VALUES (?,?,?)", _int_rows)
    con.execute(
        "CREATE TABLE big_composite(region VARCHAR, id BIGINT, kind VARCHAR, updated_at TIMESTAMP)"
    )
    _comp_rows = [(r["region"], r["id"], r["kind"], r["updated_at"]) for r in _BIG_COMPOSITE]
    con.executemany("INSERT INTO big_composite VALUES (?,?,?,?)", _comp_rows)
    con.execute("CREATE TABLE big_text(k VARCHAR, val BIGINT, updated_at TIMESTAMP)")
    _text_rows = [(r["k"], r["val"], r["updated_at"]) for r in _BIG_TEXT]
    con.executemany("INSERT INTO big_text VALUES (?,?,?)", _text_rows)
    _BIG_DUCKDB = c
    return c


def _count_in_range(conn, table, pk, rng):
    """Count rows whose key is in the half-open range (no watermark filter).

    Counts WITHOUT hashing (``checksum`` would md5 every row, which is far too slow at
    50k rows x many sub-ranges). We reuse each connector's own range-predicate logic so
    the count semantics are identical to what the methods under test see:
      * DuckDB: ``SELECT COUNT(*)`` with the connector's lexicographic range predicate.
      * Memory: its ``_selected`` generator with the same KeyRange.
    """
    from driftwatch.connectors.duckdb import DuckDBConnector

    if isinstance(conn, DuckDBConnector):
        params = []
        where = conn._where(conn._range_predicate(list(pk), rng, params))
        qtable = conn._quote_table(table)
        sql = f"SELECT COUNT(*) FROM {qtable}{where}"
        return int(conn._con.execute(sql, params).fetchone()[0])
    # MemoryConnector
    return sum(1 for _ in conn._selected(table, list(pk), rng, None, None))


def _assert_partition(conn, table, pk, full_range, bounds):
    """The sub-ranges [lo,b1),[b1,b2),...,[bk,hi) must partition `full_range` exactly."""
    lo, hi = full_range.lo, full_range.hi
    total = _count_in_range(conn, table, pk, full_range)
    # boundaries strictly increasing
    assert bounds == sorted(bounds), f"{table}: boundaries not increasing: {bounds}"
    assert len(bounds) == len(set(bounds)), f"{table}: duplicate boundaries: {bounds}"
    # strictly inside (lo, hi)
    for b in bounds:
        bt = tuple(b)
        if lo is not None:
            assert bt > tuple(lo), f"{table}: boundary {bt} <= lo {lo}"
        if hi is not None:
            assert bt < tuple(hi), f"{table}: boundary {bt} >= hi {hi}"
    # sub-ranges sum to total with no gaps/overlaps
    edges = [lo] + [tuple(b) for b in bounds] + [hi]
    summed = 0
    for a, b in zip(edges[:-1], edges[1:]):
        summed += _count_in_range(conn, table, pk, KeyRange(lo=a, hi=b))
    assert summed == total, (
        f"{table}: sub-range counts {summed} != total {total} (gaps/overlaps)"
    )
    return total


def _run_split_points_case(table, pk, full_range):
    mem, duck = _make_big_memory(), _make_big_duckdb()
    for n in (4, 16):
        mb = mem.split_points(table, pk, full_range, None, None, n)
        db = duck.split_points(table, pk, full_range, None, None, n)
        assert mb is not None, f"{table}: memory split returned None for n={n}"
        assert db is not None, f"{table}: duckdb split returned None for n={n}"
        # 1..n-1 boundaries, both sides
        assert 1 <= len(mb) <= n - 1, f"{table}: memory produced {len(mb)} bounds (n={n})"
        assert 1 <= len(db) <= n - 1, f"{table}: duckdb produced {len(db)} bounds (n={n})"
        # both must partition the data exactly (verified against DuckDB's own counts,
        # which the rest of the suite already proved match Memory's)
        total_d = _assert_partition(duck, table, pk, full_range, db)
        total_m = _assert_partition(mem, table, pk, full_range, mb)
        assert total_d == total_m, f"{table}: totals differ mem={total_m} duck={total_d}"
        # buckets should be roughly balanced: with n=16 and 50k rows every sub-range
        # has many rows, so no sub-range may be empty (that would mean a wasted split).
        edges = [full_range.lo] + [tuple(b) for b in db] + [full_range.hi]
        for a, b in zip(edges[:-1], edges[1:]):
            cnt = _count_in_range(duck, table, pk, KeyRange(lo=a, hi=b))
            assert cnt > 0, f"{table}: empty sub-range [{a},{b}) for n={n}"


def test_split_points_int_pk_partitions():
    _run_split_points_case("big_int", ["id"], KeyRange())


def test_split_points_composite_pk_partitions():
    _run_split_points_case("big_composite", ["region", "id"], KeyRange())


def test_split_points_text_pk_partitions():
    _run_split_points_case("big_text", ["k"], KeyRange())


def test_split_points_bounded_subrange_partitions():
    """A non-trivial bounded range must also split and partition exactly."""
    # interior window of the integer table
    _run_split_points_case("big_int", ["id"], KeyRange(lo=(10_000,), hi=(40_000,)))


def test_split_points_none_on_empty_and_single():
    mem, duck = _make_big_memory(), _make_big_duckdb()
    # empty range (out of bounds)
    empty = KeyRange(lo=(10**9,), hi=(10**9 + 5,))
    assert duck.split_points("big_int", ["id"], empty, None, None, 4) is None
    assert mem.split_points("big_int", ["id"], empty, None, None, 4) is None
    # exactly one row: [k, k+1) selects only id==k
    single = KeyRange(lo=(7,), hi=(8,))
    assert _count_in_range(duck, "big_int", ["id"], single) == 1
    assert duck.split_points("big_int", ["id"], single, None, None, 4) is None
    assert mem.split_points("big_int", ["id"], single, None, None, 4) is None
    # n < 2 can never split
    assert duck.split_points("big_int", ["id"], KeyRange(), None, None, 1) is None
    # composite single-row range
    csingle = KeyRange(lo=("eu", 3), hi=("eu", 4))
    assert _count_in_range(duck, "big_composite", ["region", "id"], csingle) == 1
    assert duck.split_points("big_composite", ["region", "id"], csingle, None, None, 8) is None


def _watermark_cutoffs():
    base = dt.datetime(2026, 1, 1)
    return [
        base + dt.timedelta(seconds=10_000),
        base + dt.timedelta(seconds=45_000),
        base + dt.timedelta(seconds=80_000),
        base + dt.timedelta(seconds=89_999),   # almost everything below
    ]


def _assert_keys_above_equal(mem, duck, table, pk, rng):
    for cutoff in _watermark_cutoffs():
        m = set(mem.keys_above_watermark(table, pk, rng, "updated_at", cutoff))
        d = set(duck.keys_above_watermark(table, pk, rng, "updated_at", cutoff))
        assert m == d, (
            f"{table} keys_above_watermark mismatch at cutoff={cutoff} rng={rng}: "
            f"only-mem={len(m - d)} only-duck={len(d - m)} (mem={len(m)} duck={len(d)})"
        )
    # no watermark column / no cutoff -> [] on both sides
    assert duck.keys_above_watermark(table, pk, rng, None, None) == []
    assert mem.keys_above_watermark(table, pk, rng, None, None) == []
    assert duck.keys_above_watermark(table, pk, rng, "updated_at", None) == []
    assert mem.keys_above_watermark(table, pk, rng, "updated_at", None) == []


def test_keys_above_watermark_int_pk():
    mem, duck = _make_big_memory(), _make_big_duckdb()
    _assert_keys_above_equal(mem, duck, "big_int", ["id"], KeyRange())
    # bounded range too
    _assert_keys_above_equal(mem, duck, "big_int", ["id"], KeyRange(lo=(5_000,), hi=(25_000,)))


def test_keys_above_watermark_composite_pk():
    mem, duck = _make_big_memory(), _make_big_duckdb()
    pk = ["region", "id"]
    _assert_keys_above_equal(mem, duck, "big_composite", pk, KeyRange())
    _assert_keys_above_equal(
        mem, duck, "big_composite", pk, KeyRange(lo=("eu", 100), hi=("na", 200))
    )


def test_keys_above_watermark_text_pk():
    mem, duck = _make_big_memory(), _make_big_duckdb()
    _assert_keys_above_equal(mem, duck, "big_text", ["k"], KeyRange())
    _assert_keys_above_equal(
        mem, duck, "big_text", ["k"],
        KeyRange(lo=("key-00005000",), hi=("key-00025000",)),
    )


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
