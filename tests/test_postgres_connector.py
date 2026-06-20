"""Conformance test: PostgresConnector must agree with MemoryConnector bit-for-bit.

MemoryConnector is the reference implementation of the ``driftwatch.hashing`` contract
(pure Python). This test loads *identical* data into a real PostgreSQL table and into a
MemoryConnector, then asserts that every Connector method returns the same row counts,
checksums and ``{key: row_hash}`` maps across:

  * the full table range,
  * sub-ranges and half-open boundary conditions,
  * with and without a watermark cutoff,
  * single and composite primary keys,
  * ``pk_bounds`` / ``columns`` / ``fetch_row_hashes_for_keys``.

Connection: set ``DRIFTWATCH_TEST_PG_DSN`` (e.g.
``postgresql://user:pass@localhost:5432/db``) or rely on standard libpq env vars
(``PGHOST``/``PGUSER``/``PGPASSWORD``/``PGDATABASE``/...). If psycopg v3 is not
installed, or no PostgreSQL is reachable, the test SKIPS cleanly and says so - it never
fakes a pass.

Runnable two ways:
  * ``pytest tests/test_postgres_connector.py``
  * ``python3 tests/test_postgres_connector.py``  (prints SKIP/PASS/FAIL and exits)
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

try:
    import pytest  # type: ignore
except Exception:  # pragma: no cover - allow running without pytest
    pytest = None


# --- environment / connectivity probe -----------------------------------------


class _Skip(Exception):
    """Raised to signal a clean, explained skip when no Postgres is available."""


def _connect():
    """Open a PostgresConnector or raise _Skip with a clear reason.

    Resolution order: explicit DSN env var, then standard libpq env (PGHOST etc.),
    then a localhost default so a developer with a stock local server "just works".
    """
    try:
        import psycopg  # noqa: F401
    except ImportError:
        raise _Skip(
            "psycopg (v3) is not installed; run `pip install \"psycopg[binary]\"` "
            "to enable the Postgres conformance test."
        )

    from driftwatch.connectors.postgres import PostgresConnector

    dsn = os.environ.get("DRIFTWATCH_TEST_PG_DSN")
    has_pg_env = any(k in os.environ for k in ("PGHOST", "PGURL", "PGDATABASE", "PGUSER"))
    try:
        if dsn:
            conn = PostgresConnector(dsn=dsn)
        elif has_pg_env:
            conn = PostgresConnector()  # libpq reads PG* env vars
        else:
            # No configuration at all: try a plain localhost connection as a courtesy,
            # but treat failure as a skip (not a failure) - there may simply be no PG.
            conn = PostgresConnector()
        return conn
    except Exception as exc:  # psycopg.OperationalError and friends
        raise _Skip(
            "no reachable PostgreSQL (set DRIFTWATCH_TEST_PG_DSN or PG* env vars). "
            "Underlying error: %s: %s" % (type(exc).__name__, exc)
        )


# --- fixture data --------------------------------------------------------------

# A varied set of rows exercising every canonical branch: bool, int, numeric (trailing
# zeros + integral), double (sig-digit formatting), timestamp (naive + tz), date, bytea,
# text (incl. unicode), and NULLs in compared columns. ``updated_at`` is the watermark.
_ROWS = [
    {
        "id": 1,
        "region": "us",
        "flag": True,
        "qty": 10,
        "price": Decimal("1.2300"),
        "ratio": 3.14159265358979,
        "ts": dt.datetime(2026, 1, 1, 8, 30, 0, 123456),
        "d": dt.date(2026, 1, 1),
        "blob": b"\x00\xff\x10",
        "label": "alice",
        "updated_at": dt.datetime(2026, 1, 1, 0, 0, 0),
    },
    {
        "id": 2,
        "region": "us",
        "flag": False,
        "qty": -5,
        "price": Decimal("100"),
        "ratio": 0.0001,
        "ts": dt.datetime(2026, 2, 15, 23, 59, 59, 999999),
        "d": dt.date(2026, 2, 15),
        "blob": b"",
        "label": "bob",
        "updated_at": dt.datetime(2026, 1, 2, 0, 0, 0),
    },
    {
        "id": 3,
        "region": "eu",
        "flag": True,
        "qty": 0,
        "price": Decimal("0.00"),
        "ratio": 6.022e23,
        "ts": dt.datetime(2026, 3, 10, 12, 0, 0, 0),
        "d": dt.date(2026, 3, 10),
        "blob": b"\xde\xad\xbe\xef",
        "label": "café",  # unicode
        "updated_at": dt.datetime(2026, 1, 3, 0, 0, 0),
    },
    {
        "id": 4,
        "region": "eu",
        "flag": None,        # NULL bool
        "qty": None,         # NULL int
        "price": None,       # NULL numeric
        "ratio": None,       # NULL double
        "ts": None,          # NULL timestamp
        "d": None,           # NULL date
        "blob": None,        # NULL bytea
        "label": None,       # NULL text
        "updated_at": dt.datetime(2026, 1, 4, 0, 0, 0),
    },
    {
        "id": 5,
        "region": "eu",
        "flag": False,
        "qty": 2147483647,
        "price": Decimal("999999.999"),
        "ratio": -1.602e-19,
        "ts": dt.datetime(2025, 12, 31, 0, 0, 0, 1),
        "d": dt.date(2025, 12, 31),
        "blob": b"\x7f",
        "label": "",         # empty string (distinct from NULL)
        "updated_at": None,  # NULL watermark -> excluded by any cutoff
    },
]

_DDL_COLUMNS = (
    "id integer NOT NULL, "
    "region text NOT NULL, "
    "flag boolean, "
    "qty integer, "
    "price numeric(20, 6), "
    "ratio double precision, "
    "ts timestamp without time zone, "
    "d date, "
    "blob bytea, "
    "label text, "
    "updated_at timestamp without time zone"
)

_INSERT_COLS = [
    "id", "region", "flag", "qty", "price", "ratio", "ts", "d", "blob", "label",
    "updated_at",
]

# Columns compared (everything except the pk). Sorted to match how the engine resolves
# "*" - but here we pass them explicitly so both sides hash the same set in the same
# order.
_COMPARE = ["region", "flag", "qty", "price", "ratio", "ts", "d", "blob", "label", "updated_at"]
_FP = 12


def _load_pg(pg, schema_table):
    """Create the test table and load _ROWS using the connector's own raw connection.

    We borrow the connector's connection only to set up the fixture; the *read* methods
    under test go through the public Connector interface exactly as the engine uses them.
    """
    from psycopg import sql as S

    raw = pg._conn  # the live psycopg connection
    # Setup requires writes; temporarily leave the read-only snapshot.
    raw.rollback()
    prev_ro = raw.read_only
    raw.read_only = False
    raw.autocommit = True
    schema, _, table = schema_table.partition(".")
    ident = S.Identifier(schema, table) if schema else S.Identifier(table)
    with raw.cursor() as cur:
        cur.execute(S.SQL("DROP TABLE IF EXISTS {}").format(ident))
        cur.execute(
            S.SQL("CREATE TABLE {tbl} ({cols})").format(
                tbl=ident, cols=S.SQL(_DDL_COLUMNS)
            )
        )
        col_idents = S.SQL(", ").join(S.Identifier(c) for c in _INSERT_COLS)
        placeholders = S.SQL(", ").join(S.Placeholder() for _ in _INSERT_COLS)
        insert = S.SQL("INSERT INTO {tbl} ({cols}) VALUES ({ph})").format(
            tbl=ident, cols=col_idents, ph=placeholders
        )
        for row in _ROWS:
            cur.execute(insert, [row[c] for c in _INSERT_COLS])
    raw.commit()
    # Restore the read-only snapshot for the methods under test.
    raw.autocommit = False
    raw.read_only = prev_ro
    raw.rollback()


def _assert_eq(label, a, b):
    if a != b:
        raise AssertionError("%s mismatch:\n  pg=%r\n  mem=%r" % (label, a, b))


def _run_conformance(pg, table):
    """The shared body: assert PG agrees with Memory across every scenario."""
    mem = MemoryConnector({table: _ROWS})

    pk = ["id"]
    full = KeyRange()

    # 1. columns -------------------------------------------------------------
    pg_cols = pg.columns(table)
    mem_cols = mem.columns(table)
    _assert_eq("columns", pg_cols, mem_cols)
    assert pg_cols == sorted(_INSERT_COLS), pg_cols

    # 2. pk_bounds (no cutoff) ----------------------------------------------
    _assert_eq("pk_bounds full", pg.pk_bounds(table, pk, None, None),
               mem.pk_bounds(table, pk, None, None))
    assert pg.pk_bounds(table, pk, None, None) == KeyRange(lo=(1,), hi=(5,))

    # 3. checksum + row hashes over the full range --------------------------
    _assert_eq("checksum full",
               pg.checksum(table, pk, _COMPARE, full, None, None, _FP),
               mem.checksum(table, pk, _COMPARE, full, None, None, _FP))
    _assert_eq("row_hashes full",
               pg.fetch_row_hashes(table, pk, _COMPARE, full, None, None, _FP),
               mem.fetch_row_hashes(table, pk, _COMPARE, full, None, None, _FP))

    # 4. sub-ranges and half-open boundaries --------------------------------
    for lo, hi in [
        ((2,), (4,)),     # [2,4): ids 2,3
        (None, (3,)),     # [-inf,3): ids 1,2
        ((3,), None),     # [3,+inf): ids 3,4,5
        ((1,), (2,)),     # [1,2): id 1 only
        ((5,), (5,)),     # [5,5): empty (lo==hi, half-open)
        ((1,), (6,)),     # [1,6): all rows (hi past max)
    ]:
        rng = KeyRange(lo=lo, hi=hi)
        _assert_eq("checksum range %r" % (rng,),
                   pg.checksum(table, pk, _COMPARE, rng, None, None, _FP),
                   mem.checksum(table, pk, _COMPARE, rng, None, None, _FP))
        _assert_eq("row_hashes range %r" % (rng,),
                   pg.fetch_row_hashes(table, pk, _COMPARE, rng, None, None, _FP),
                   mem.fetch_row_hashes(table, pk, _COMPARE, rng, None, None, _FP))

    # Explicit half-open check: id 5 (lo) included, id at hi excluded.
    half = KeyRange(lo=(2,), hi=(5,))  # ids 2,3,4 - NOT 5
    got = pg.fetch_row_hashes(table, pk, _COMPARE, half, None, None, _FP)
    assert sorted(got.keys()) == [(2,), (3,), (4,)], sorted(got.keys())

    # 5. watermark cutoff (with and without) --------------------------------
    cutoff = dt.datetime(2026, 1, 3, 0, 0, 0)  # includes ids 1,2,3; excludes 4; 5 has NULL wm
    _assert_eq("checksum cutoff",
               pg.checksum(table, pk, _COMPARE, full, "updated_at", cutoff, _FP),
               mem.checksum(table, pk, _COMPARE, full, "updated_at", cutoff, _FP))
    pg_cut = pg.fetch_row_hashes(table, pk, _COMPARE, full, "updated_at", cutoff, _FP)
    mem_cut = mem.fetch_row_hashes(table, pk, _COMPARE, full, "updated_at", cutoff, _FP)
    _assert_eq("row_hashes cutoff", pg_cut, mem_cut)
    assert sorted(pg_cut.keys()) == [(1,), (2,), (3,)], sorted(pg_cut.keys())

    # pk_bounds also honours the cutoff (max becomes id 3).
    _assert_eq("pk_bounds cutoff",
               pg.pk_bounds(table, pk, "updated_at", cutoff),
               mem.pk_bounds(table, pk, "updated_at", cutoff))

    # 6. fetch_row_hashes_for_keys (recheck pass) ---------------------------
    keys = [(1,), (3,), (5,), (99,)]  # 99 absent -> must be absent on both sides
    _assert_eq("for_keys",
               pg.fetch_row_hashes_for_keys(table, pk, _COMPARE, keys, None, None, _FP),
               mem.fetch_row_hashes_for_keys(table, pk, _COMPARE, keys, None, None, _FP))
    # for_keys with a cutoff (id 5 has NULL watermark -> excluded under cutoff)
    _assert_eq("for_keys cutoff",
               pg.fetch_row_hashes_for_keys(table, pk, _COMPARE, keys, "updated_at", cutoff, _FP),
               mem.fetch_row_hashes_for_keys(table, pk, _COMPARE, keys, "updated_at", cutoff, _FP))

    # 7. a single changed cell must change exactly one row hash -------------
    changed_rows = [dict(r) for r in _ROWS]
    changed_rows[1]["label"] = "BOBBY"  # id=2
    mem2 = MemoryConnector({table: changed_rows})
    base = mem.fetch_row_hashes(table, pk, _COMPARE, full, None, None, _FP)
    bumped = mem2.fetch_row_hashes(table, pk, _COMPARE, full, None, None, _FP)
    diff = [k for k in base if base[k] != bumped[k]]
    assert diff == [(2,)], diff
    # And PG agrees with the unchanged baseline (the change lives only in Memory here).
    _assert_eq("baseline vs pg", base,
               pg.fetch_row_hashes(table, pk, _COMPARE, full, None, None, _FP))

    # 8. composite primary key ---------------------------------------------
    cpk = ["region", "id"]
    ccompare = [c for c in _COMPARE if c != "region"]
    _assert_eq("composite pk_bounds",
               pg.pk_bounds(table, cpk, None, None),
               mem.pk_bounds(table, cpk, None, None))
    _assert_eq("composite checksum full",
               pg.checksum(table, cpk, ccompare, full, None, None, _FP),
               mem.checksum(table, cpk, ccompare, full, None, None, _FP))
    _assert_eq("composite row_hashes full",
               pg.fetch_row_hashes(table, cpk, ccompare, full, None, None, _FP),
               mem.fetch_row_hashes(table, cpk, ccompare, full, None, None, _FP))
    # composite half-open range over (region, id): [('eu',3), ('us',1))
    crng = KeyRange(lo=("eu", 3), hi=("us", 1))
    _assert_eq("composite range",
               pg.fetch_row_hashes(table, cpk, ccompare, crng, None, None, _FP),
               mem.fetch_row_hashes(table, cpk, ccompare, crng, None, None, _FP))

    # 9. compare a subset / different float_precision -----------------------
    for fp in (6, 12, 15):
        _assert_eq("checksum fp=%d" % fp,
                   pg.checksum(table, pk, ["ratio"], full, None, None, fp),
                   mem.checksum(table, pk, ["ratio"], full, None, None, fp))


# --- pytest entrypoints --------------------------------------------------------


def _table_name():
    # Unique per run so parallel CI jobs don't collide; schema-qualified to also exercise
    # identifier quoting of "schema.table".
    return "public.driftwatch_conf_%s" % uuid.uuid4().hex[:8]


def test_postgres_conformance():
    """pytest entrypoint; skips cleanly when no Postgres is reachable."""
    try:
        pg = _connect()
    except _Skip as s:
        if pytest is not None:
            pytest.skip(str(s))
        else:  # pragma: no cover
            print("SKIP:", s)
            return
    table = _table_name()
    try:
        _load_pg(pg, table)
        _run_conformance(pg, table)
    finally:
        _drop(pg, table)
        pg.close()


def _drop(pg, schema_table):
    from psycopg import sql as S

    raw = pg._conn
    try:
        raw.rollback()
        raw.read_only = False
        raw.autocommit = True
        schema, _, table = schema_table.partition(".")
        ident = S.Identifier(schema, table) if schema else S.Identifier(table)
        with raw.cursor() as cur:
            cur.execute(S.SQL("DROP TABLE IF EXISTS {}").format(ident))
    except Exception:
        pass


# --- bare-interpreter entrypoint ----------------------------------------------

if __name__ == "__main__":
    try:
        pg = _connect()
    except _Skip as s:
        print("SKIP: %s" % s)
        print("\nConformance UNVERIFIED in this environment (no live Postgres).")
        sys.exit(0)

    table = _table_name()
    failures = 0
    try:
        _load_pg(pg, table)
        _run_conformance(pg, table)
        print("PASS test_postgres_conformance (table=%s)" % table)
    except AssertionError as e:
        failures += 1
        print("FAIL test_postgres_conformance -", e)
    except Exception as e:  # noqa: BLE001
        failures += 1
        print("ERROR test_postgres_conformance -", type(e).__name__, e)
    finally:
        _drop(pg, table)
        pg.close()

    print("\n%d failure(s)" % failures)
    sys.exit(1 if failures else 0)
