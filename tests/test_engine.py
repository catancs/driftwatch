"""Engine tests: recursive hash-segmentation + lag handling, over MemoryConnector.

Runnable two ways: ``pytest`` (CI) or ``python3 tests/test_engine.py`` (no deps),
mirroring tests/test_foundation.py so the engine can be validated on a bare
interpreter with zero installed dependencies.
"""

import datetime as dt
import os
import sys
from typing import Any, Dict, List, Optional, Sequence

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from driftwatch.config import ComparisonConfig, RecheckConfig  # noqa: E402
from driftwatch.connectors.memory import MemoryConnector  # noqa: E402
from driftwatch.engine import compare  # noqa: E402
from driftwatch.models import DriftKind  # noqa: E402


# --- helpers -----------------------------------------------------------------


def _no_sleep(_seconds: float) -> None:
    """A sleep stub that never actually sleeps - keeps tests instant."""
    raise AssertionError("sleep should not be called when delay<=0 or rounds==0")


def _cmp(**kwargs: Any) -> ComparisonConfig:
    """Build a ComparisonConfig with sane test defaults, overridable via kwargs."""
    params: Dict[str, Any] = dict(
        name="t",
        source_table="src",
        target_table="dst",
        primary_key=["id"],
        compare_columns=["name"],
        segment_fanout=4,
        leaf_size=2,
        recheck=RecheckConfig(delay_seconds=0.0, rounds=0),
    )
    params.update(kwargs)
    return ComparisonConfig(**params)


def _drift_map(report) -> Dict[Any, DriftKind]:
    return {dk.key: dk.kind for dk in report.drift_keys}


def _rows():
    return [
        {"id": 1, "name": "alice"},
        {"id": 2, "name": "bob"},
        {"id": 3, "name": "carol"},
    ]


# --- core diff cases ---------------------------------------------------------


def test_identical_tables_in_sync():
    c = MemoryConnector({"src": _rows(), "dst": _rows()})
    report = compare(c, c, _cmp(), sleep=_no_sleep)
    assert report.in_sync is True
    assert report.drift_keys == []
    assert report.candidates_before_recheck == 0
    assert report.segments_scanned >= 1  # at least the root checksum happened
    assert report.cutoff is None  # no watermark configured


def test_missing_key():
    # id=2 present in source, absent in target → MISSING
    dst = [r for r in _rows() if r["id"] != 2]
    c = MemoryConnector({"src": _rows(), "dst": dst})
    report = compare(c, c, _cmp(), sleep=_no_sleep)
    assert report.in_sync is False
    assert _drift_map(report) == {(2,): DriftKind.MISSING}


def test_extra_key():
    # id=4 present in target, absent in source → EXTRA
    dst = _rows() + [{"id": 4, "name": "dave"}]
    c = MemoryConnector({"src": _rows(), "dst": dst})
    report = compare(c, c, _cmp(), sleep=_no_sleep)
    assert report.in_sync is False
    assert _drift_map(report) == {(4,): DriftKind.EXTRA}


def test_changed_row():
    dst = _rows()
    dst[1] = {**dst[1], "name": "BOBBY"}  # change id=2's content
    c = MemoryConnector({"src": _rows(), "dst": dst})
    report = compare(c, c, _cmp(), sleep=_no_sleep)
    assert report.in_sync is False
    assert _drift_map(report) == {(2,): DriftKind.CHANGED}


def test_mixed_drift_kinds():
    # one of each kind in a single run
    src = [
        {"id": 1, "name": "alice"},
        {"id": 2, "name": "bob"},
        {"id": 3, "name": "carol"},  # will be missing from dst
    ]
    dst = [
        {"id": 1, "name": "alice"},
        {"id": 2, "name": "BOBBY"},  # changed
        {"id": 5, "name": "erin"},  # extra
    ]
    c = MemoryConnector({"src": src, "dst": dst})
    report = compare(c, c, _cmp(), sleep=_no_sleep)
    assert _drift_map(report) == {
        (2,): DriftKind.CHANGED,
        (3,): DriftKind.MISSING,
        (5,): DriftKind.EXTRA,
    }


# --- empties -----------------------------------------------------------------


def test_both_empty_in_sync():
    c = MemoryConnector({"src": [], "dst": []})
    report = compare(c, c, _cmp(), sleep=_no_sleep)
    assert report.in_sync is True
    assert report.drift_keys == []


def test_source_empty_all_extra():
    c = MemoryConnector({"src": [], "dst": _rows()})
    report = compare(c, c, _cmp(), sleep=_no_sleep)
    assert report.in_sync is False
    assert _drift_map(report) == {
        (1,): DriftKind.EXTRA,
        (2,): DriftKind.EXTRA,
        (3,): DriftKind.EXTRA,
    }


def test_target_empty_all_missing():
    c = MemoryConnector({"src": _rows(), "dst": []})
    report = compare(c, c, _cmp(), sleep=_no_sleep)
    assert report.in_sync is False
    assert _drift_map(report) == {
        (1,): DriftKind.MISSING,
        (2,): DriftKind.MISSING,
        (3,): DriftKind.MISSING,
    }


# --- composite primary key ---------------------------------------------------


def test_composite_primary_key():
    src = [
        {"tenant": "a", "id": 1, "name": "alice"},
        {"tenant": "a", "id": 2, "name": "bob"},
        {"tenant": "b", "id": 1, "name": "carol"},
    ]
    dst = [
        {"tenant": "a", "id": 1, "name": "alice"},
        {"tenant": "a", "id": 2, "name": "BOBBY"},  # changed
        # ("b", 1) missing
        {"tenant": "b", "id": 9, "name": "zed"},  # extra
    ]
    c = MemoryConnector({"src": src, "dst": dst})
    cmp = _cmp(primary_key=["tenant", "id"], compare_columns=["name"], leaf_size=2)
    report = compare(c, c, cmp, sleep=_no_sleep)
    assert _drift_map(report) == {
        ("a", 2): DriftKind.CHANGED,
        ("b", 1): DriftKind.MISSING,
        ("b", 9): DriftKind.EXTRA,
    }
    # composite key is not numerically splittable → falls back to a leaf fetch.
    assert report.in_sync is False


# --- compare-column resolution ("*") ----------------------------------------


def test_star_columns_resolved_and_excludes_applied():
    # compare_columns=None → resolve sorted intersection minus pk minus excludes.
    src = [
        {"id": 1, "name": "alice", "email": "a@x", "ignore_me": "S1"},
        {"id": 2, "name": "bob", "email": "b@x", "ignore_me": "S2"},
    ]
    dst = [
        {"id": 1, "name": "alice", "email": "a@x", "ignore_me": "T1"},  # differs only in excluded col
        {"id": 2, "name": "BOBBY", "email": "b@x", "ignore_me": "T2"},  # name differs (compared)
    ]
    c = MemoryConnector({"src": src, "dst": dst})
    cmp = _cmp(compare_columns=None, exclude_columns=["ignore_me"], leaf_size=10)
    report = compare(c, c, cmp, sleep=_no_sleep)
    # id=1 differs ONLY in the excluded column → not drift.
    # id=2 differs in name → CHANGED.
    assert _drift_map(report) == {(2,): DriftKind.CHANGED}


# --- large table: pruning + sparse drift -------------------------------------


def test_large_table_sparse_drift_with_pruning():
    n = 6000
    src = [{"id": i, "name": "row-%d" % i} for i in range(n)]
    dst = [dict(r) for r in src]
    # inject three sparse divergences across the key space
    changed_key = 1234
    missing_key = 4321
    extra_key = n + 7  # beyond the source max → EXTRA on target
    dst[changed_key]["name"] = "MUTATED"
    dst = [r for r in dst if r["id"] != missing_key]
    dst.append({"id": extra_key, "name": "phantom"})

    c = MemoryConnector({"src": src, "dst": dst})
    cmp = _cmp(segment_fanout=8, leaf_size=100)
    report = compare(c, c, cmp, sleep=_no_sleep)

    assert _drift_map(report) == {
        (changed_key,): DriftKind.CHANGED,
        (missing_key,): DriftKind.MISSING,
        (extra_key,): DriftKind.EXTRA,
    }
    # Pruning proof: a full scan would touch every one of the ~6000 rows at leaves.
    # With segmentation only the segments leading to the three divergences are
    # subdivided/fetched, so rows_compared must be a small fraction of n, and the
    # number of segments scanned must be far below n.
    assert report.rows_compared < n, "no pruning happened - fetched everything"
    assert report.rows_compared < 2000, (
        "expected sparse drift to prune most rows, compared %d" % report.rows_compared
    )
    assert report.segments_scanned < n
    assert report.segments_scanned > 1, "no subdivision happened at all"


# --- watermark cutoff (lag safety-net #1) ------------------------------------


def test_watermark_cutoff_excludes_fresh_divergence():
    # A fresh row exists in source but hasn't propagated to target yet. Because its
    # watermark is NEWER than (now - grace), the cutoff excludes it on BOTH sides,
    # so within the grace window it is invisible → no drift reported.
    now = dt.datetime(2026, 6, 20, 12, 0, 0, tzinfo=dt.timezone.utc)
    old = dt.datetime(2026, 6, 20, 10, 0, 0, tzinfo=dt.timezone.utc)  # well before cutoff
    fresh = dt.datetime(2026, 6, 20, 11, 59, 30, tzinfo=dt.timezone.utc)  # 30s ago, inside grace

    src = [
        {"id": 1, "name": "alice", "updated_at": old},
        {"id": 2, "name": "bob", "updated_at": old},
        {"id": 3, "name": "fresh", "updated_at": fresh},  # not yet in target
    ]
    dst = [
        {"id": 1, "name": "alice", "updated_at": old},
        {"id": 2, "name": "bob", "updated_at": old},
    ]
    c = MemoryConnector({"src": src, "dst": dst})
    cmp = _cmp(
        watermark_column="updated_at",
        grace_seconds=300.0,  # 5 min grace → cutoff = 11:55:00, excludes the 11:59:30 row
        leaf_size=10,
    )
    report = compare(c, c, cmp, now=now, sleep=_no_sleep)
    assert report.in_sync is True, "fresh divergence within grace must not be reported"
    assert report.drift_keys == []
    assert report.cutoff == (now - dt.timedelta(seconds=300)).isoformat()

    # Sanity: shrink the grace so the fresh row IS included → it surfaces as drift.
    cmp2 = _cmp(watermark_column="updated_at", grace_seconds=5.0, leaf_size=10)
    report2 = compare(c, c, cmp2, now=now, sleep=_no_sleep)
    assert _drift_map(report2) == {(3,): DriftKind.MISSING}


# --- recheck pass (lag safety-net #2) ----------------------------------------


class _ReconcilingConnector(MemoryConnector):
    """A MemoryConnector whose recheck fetch (``fetch_row_hashes_for_keys``) reads
    from a *second*, fresher copy of the tables. This simulates lag catching up: the
    segmentation pass sees the stale snapshot (a divergence), but by recheck time the
    fresh snapshot agrees, so the candidate reconciles and must be dropped."""

    def __init__(self, stale: Dict[str, List[Dict[str, Any]]], fresh: Dict[str, List[Dict[str, Any]]]):
        super().__init__(stale)
        self._fresh = {name: list(rows) for name, rows in fresh.items()}

    def fetch_row_hashes_for_keys(self, table, pk_cols, compare_cols, keys, watermark_column, cutoff, float_precision):
        wanted = set(keys)
        out: Dict[Any, int] = {}
        for row in self._fresh.get(table, []):
            if not self._passes_cutoff(row, watermark_column, cutoff):
                continue
            key = self._key_of(row, pk_cols)
            if key in wanted:
                out[key] = self._row_hash(row, pk_cols, compare_cols, float_precision)
        return out


def test_recheck_drops_reconciled_keeps_genuine():
    # STALE snapshot (what segmentation sees):
    #   id=2 changed (alice→BOBBY mismatch), id=3 missing from target.
    # FRESH snapshot (what recheck sees): id=2 has caught up (matches now) → drop it.
    #   id=3 is genuinely gone forever → it survives recheck as confirmed drift.
    src_stale = [
        {"id": 1, "name": "alice"},
        {"id": 2, "name": "bob"},
        {"id": 3, "name": "carol"},
    ]
    dst_stale = [
        {"id": 1, "name": "alice"},
        {"id": 2, "name": "LAGGED"},  # transient mismatch → candidate CHANGED
        # id=3 absent → candidate MISSING
    ]
    src_fresh = list(src_stale)
    dst_fresh = [
        {"id": 1, "name": "alice"},
        {"id": 2, "name": "bob"},  # reconciled: now matches source
        # id=3 still absent → genuine drift
    ]
    source = _ReconcilingConnector({"src": src_stale}, {"src": src_fresh})
    target = _ReconcilingConnector({"dst": dst_stale}, {"dst": dst_fresh})

    sleeps: List[float] = []
    cmp = _cmp(leaf_size=10, recheck=RecheckConfig(delay_seconds=30.0, rounds=1))
    report = compare(source, target, cmp, sleep=sleeps.append)

    # Two candidates found by segmentation; one reconciles, one survives.
    assert report.candidates_before_recheck == 2
    assert _drift_map(report) == {(3,): DriftKind.MISSING}
    assert report.in_sync is False
    # the recheck delay was honored exactly once (rounds=1, delay>0)
    assert sleeps == [30.0]


def test_recheck_zero_rounds_keeps_all_candidates():
    # rounds=0 → no recheck, every candidate is reported as-is (no sleep either).
    dst = [r for r in _rows() if r["id"] != 2]  # id=2 missing
    c = MemoryConnector({"src": _rows(), "dst": dst})
    cmp = _cmp(recheck=RecheckConfig(delay_seconds=60.0, rounds=0))
    report = compare(c, c, cmp, sleep=_no_sleep)  # _no_sleep would raise if called
    assert _drift_map(report) == {(2,): DriftKind.MISSING}
    assert report.candidates_before_recheck == 1


def test_recheck_all_reconcile_reports_in_sync():
    # Both candidates reconcile at recheck → confirmed drift is empty → in_sync True,
    # but candidates_before_recheck still records that segmentation found divergence.
    src_stale = [{"id": 1, "name": "alice"}, {"id": 2, "name": "bob"}]
    dst_stale = [{"id": 1, "name": "LAG1"}, {"id": 2, "name": "LAG2"}]
    fresh = [{"id": 1, "name": "alice"}, {"id": 2, "name": "bob"}]
    source = _ReconcilingConnector({"src": src_stale}, {"src": list(fresh)})
    target = _ReconcilingConnector({"dst": dst_stale}, {"dst": list(fresh)})
    cmp = _cmp(leaf_size=10, recheck=RecheckConfig(delay_seconds=0.0, rounds=2))
    report = compare(source, target, cmp, sleep=lambda _s: None)
    assert report.candidates_before_recheck == 2
    assert report.in_sync is True
    assert report.drift_keys == []


# --- operational errors propagate (never swallowed into in_sync) -------------


class _ExplodingConnector(MemoryConnector):
    """Raises on the very first checksum to simulate a DB/operational failure."""

    def checksum(self, *args, **kwargs):
        raise RuntimeError("connection reset by peer")


def test_operational_error_propagates():
    src = _ExplodingConnector({"src": _rows()})
    dst = MemoryConnector({"dst": _rows()})
    raised = False
    try:
        compare(src, dst, _cmp(), sleep=_no_sleep)
    except RuntimeError as e:
        raised = True
        assert "connection reset" in str(e)
    assert raised, "operational error must propagate, never become in_sync=True"


# --- report metadata ---------------------------------------------------------


def test_report_metadata_populated():
    c = MemoryConnector({"src": _rows(), "dst": _rows()})
    report = compare(c, c, _cmp(), sleep=_no_sleep)
    assert report.comparison == "t"
    assert report.started_at is not None
    assert report.finished_at is not None
    assert report.duration_seconds >= 0.0


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
