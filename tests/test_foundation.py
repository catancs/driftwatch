"""Foundation tests: hashing contract + MemoryConnector reference behaviour.

Runnable two ways: ``pytest`` (CI) or ``python3 tests/test_foundation.py`` (no deps),
so the foundation can be validated on a bare interpreter before the rest exists.
"""

import datetime as dt
import os
import sys
from decimal import Decimal

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from driftwatch.connectors.memory import MemoryConnector  # noqa: E402
from driftwatch.hashing import canonical, row_hash, segment_checksum  # noqa: E402
from driftwatch.models import KeyRange  # noqa: E402


def test_canonical_forms():
    assert canonical(None) == "\\N"
    assert canonical(True) == "1" and canonical(False) == "0"
    assert canonical(42) == "42"
    assert canonical(Decimal("1.2300")) == "1.23"
    assert canonical(Decimal("100")) == "100"
    assert canonical(b"\x00\xff") == "00ff"
    # naive and tz-aware datetimes pointing at the same instant canonicalize equally
    aware = dt.datetime(2026, 1, 2, 3, 4, 5, 6, tzinfo=dt.timezone.utc)
    naive = dt.datetime(2026, 1, 2, 3, 4, 5, 6)
    assert canonical(aware) == canonical(naive) == "2026-01-02 03:04:05.000006"


def test_segment_checksum_order_independent():
    a = [row_hash([1, "x"]), row_hash([2, "y"]), row_hash([3, "z"])]
    assert segment_checksum(a) == segment_checksum(list(reversed(a)))


def _rows():
    return [
        {"id": 1, "name": "alice", "updated_at": dt.datetime(2026, 1, 1, 0, 0, 0)},
        {"id": 2, "name": "bob", "updated_at": dt.datetime(2026, 1, 2, 0, 0, 0)},
        {"id": 3, "name": "carol", "updated_at": dt.datetime(2026, 1, 3, 0, 0, 0)},
    ]


def test_memory_identical_tables_match():
    c = MemoryConnector({"src": _rows(), "dst": _rows()})
    full = KeyRange()
    s = c.checksum("src", ["id"], ["name"], full, None, None, 12)
    d = c.checksum("dst", ["id"], ["name"], full, None, None, 12)
    assert s == d
    assert s.count == 3


def test_memory_detects_change():
    dst = _rows()
    dst[1] = {**dst[1], "name": "BOBBY"}  # change id=2
    c = MemoryConnector({"src": _rows(), "dst": dst})
    full = KeyRange()
    assert c.checksum("src", ["id"], ["name"], full, None, None, 12) != \
        c.checksum("dst", ["id"], ["name"], full, None, None, 12)
    src_h = c.fetch_row_hashes("src", ["id"], ["name"], full, None, None, 12)
    dst_h = c.fetch_row_hashes("dst", ["id"], ["name"], full, None, None, 12)
    changed = [k for k in src_h if k in dst_h and src_h[k] != dst_h[k]]
    assert changed == [(2,)]


def test_memory_pk_bounds_and_columns():
    c = MemoryConnector({"src": _rows()})
    assert c.pk_bounds("src", ["id"], None, None) == KeyRange(lo=(1,), hi=(3,))
    assert c.columns("src") == ["id", "name", "updated_at"]
    assert c.pk_bounds("empty", ["id"], None, None) if "empty" in c._tables else True


def test_memory_half_open_range():
    c = MemoryConnector({"src": _rows()})
    rng = KeyRange(lo=(1,), hi=(3,))  # should include id 1,2 but not 3
    got = c.fetch_row_hashes("src", ["id"], ["name"], rng, None, None, 12)
    assert sorted(got.keys()) == [(1,), (2,)]


def test_memory_watermark_cutoff():
    c = MemoryConnector({"src": _rows()})
    cutoff = dt.datetime(2026, 1, 2, 0, 0, 0)  # excludes id=3 (2026-01-03)
    res = c.checksum("src", ["id"], ["name"], KeyRange(), "updated_at", cutoff, 12)
    assert res.count == 2


def test_memory_recheck_keys():
    c = MemoryConnector({"src": _rows()})
    got = c.fetch_row_hashes_for_keys("src", ["id"], ["name"], [(1,), (3,)], None, None, 12)
    assert sorted(got.keys()) == [(1,), (3,)]


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
