"""Reporter tests: exit-code mapping, human text rendering, machine JSON.

Runnable two ways, mirroring ``tests/test_foundation.py``: ``pytest`` (CI) or
``python3 tests/test_reporter.py`` (no deps), so the output layer can be validated on
a bare interpreter.
"""

import datetime as dt
import json
import os
import sys
from decimal import Decimal

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from driftwatch.models import DriftKey, DriftKind, DriftReport  # noqa: E402
from driftwatch.reporter import (  # noqa: E402
    exit_code,
    print_report,
    render_json,
    render_text,
)


# --- fixtures ------------------------------------------------------------------


def _in_sync_report():
    return DriftReport(
        comparison="users",
        in_sync=True,
        rows_compared=1000,
        segments_scanned=4,
        cutoff="2026-06-20T00:00:00",
        duration_seconds=1.2345,
    )


def _drift_report(n_missing=2, n_extra=1, n_changed=1):
    keys = []
    i = 0
    for _ in range(n_missing):
        i += 1
        keys.append(DriftKey(key=(i,), kind=DriftKind.MISSING))
    for _ in range(n_extra):
        i += 1
        keys.append(DriftKey(key=(i,), kind=DriftKind.EXTRA))
    for _ in range(n_changed):
        i += 1
        keys.append(DriftKey(key=(i,), kind=DriftKind.CHANGED))
    return DriftReport(
        comparison="orders",
        in_sync=False,
        drift_keys=keys,
        rows_compared=500,
        segments_scanned=8,
        candidates_before_recheck=6,
        cutoff="2026-06-20T00:00:00",
        duration_seconds=3.5,
    )


def _error_report():
    return DriftReport(
        comparison="events",
        in_sync=True,  # meaningless when error is set; reporter must ignore it
        error="connection refused: postgres:5432",
    )


# --- exit_code -----------------------------------------------------------------


def test_exit_code_in_sync():
    assert exit_code(_in_sync_report()) == 0


def test_exit_code_drift():
    assert exit_code(_drift_report()) == 1


def test_exit_code_error_wins_over_in_sync():
    # error set but in_sync=True -> still operational error, never report in-sync
    assert exit_code(_error_report()) == 2


def test_exit_code_error_with_drift_keys():
    r = _drift_report()
    r.error = "query timeout"
    assert exit_code(r) == 2


# --- render_text ---------------------------------------------------------------


def test_render_text_in_sync_headline():
    out = render_text(_in_sync_report())
    assert "IN SYNC" in out
    assert "users" in out
    assert "rows compared: 1000" in out


def test_render_text_drift_headline_and_counts():
    out = render_text(_drift_report(n_missing=2, n_extra=1, n_changed=1))
    assert "DRIFT" in out
    assert "orders" in out
    # counts by kind present
    assert "missing=2" in out
    assert "extra=1" in out
    assert "changed=1" in out
    assert "4 total" in out
    # run statistics surfaced
    assert "segments scanned: 8" in out
    assert "candidates before recheck: 6" in out
    assert "cutoff: 2026-06-20T00:00:00" in out


def test_render_text_error_headline():
    out = render_text(_error_report())
    assert "ERROR" in out
    assert "events" in out
    assert "connection refused" in out
    # an errored report must not claim a verdict
    assert "IN SYNC" not in out
    assert "DRIFT" not in out


def test_render_text_lists_keys():
    out = render_text(_drift_report(n_missing=1, n_extra=0, n_changed=0))
    assert "[missing]" in out
    assert "diverging keys:" in out


def test_render_text_samples_and_truncates():
    # 25 changed keys, sample=20 -> 20 shown, "... and 5 more"
    keys = [DriftKey(key=(i,), kind=DriftKind.CHANGED) for i in range(25)]
    r = DriftReport(comparison="big", in_sync=False, drift_keys=keys)
    out = render_text(r, sample=20)
    shown = [ln for ln in out.splitlines() if ln.strip().startswith("[changed]")]
    assert len(shown) == 20
    assert "... and 5 more" in out


def test_render_text_no_truncation_when_under_sample():
    out = render_text(_drift_report(n_missing=2, n_extra=1, n_changed=1), sample=20)
    assert "more" not in out  # 4 keys, no truncation line


def test_render_text_composite_key():
    keys = [DriftKey(key=(1, "a"), kind=DriftKind.MISSING)]
    r = DriftReport(comparison="ck", in_sync=False, drift_keys=keys)
    out = render_text(r)
    assert "(1, a)" in out


# --- render_json ---------------------------------------------------------------


def test_render_json_valid_and_roundtrips():
    out = render_json(_drift_report())
    parsed = json.loads(out)  # must not raise
    assert parsed["comparison"] == "orders"
    assert parsed["in_sync"] is False
    assert parsed["drift_total"] == 4
    assert parsed["drift_by_kind"] == {"missing": 2, "extra": 1, "changed": 1}


def test_render_json_includes_every_drift_key():
    r = _drift_report(n_missing=3, n_extra=2, n_changed=4)
    parsed = json.loads(render_json(r))
    assert len(parsed["drift_keys"]) == len(r.drift_keys) == 9
    # every key/kind pair round-trips
    expected = {(tuple(dk.key), dk.kind.value) for dk in r.drift_keys}
    got = {(tuple(item["key"]), item["kind"]) for item in parsed["drift_keys"]}
    assert got == expected


def test_render_json_sorted_keys_deterministic():
    out1 = render_json(_drift_report())
    out2 = render_json(_drift_report())
    assert out1 == out2  # deterministic
    # sort_keys -> top-level keys are alphabetically ordered
    keys = list(json.loads(out1).keys())
    assert keys == sorted(keys)


def test_render_json_error_report():
    parsed = json.loads(render_json(_error_report()))
    assert parsed["error"] == "connection refused: postgres:5432"


def test_render_json_non_native_key_types():
    # date, datetime, Decimal, bytes, composite -> must serialize without crashing
    keys = [
        DriftKey(key=(dt.date(2026, 6, 20),), kind=DriftKind.MISSING),
        DriftKey(key=(dt.datetime(2026, 6, 20, 12, 30, 0),), kind=DriftKind.CHANGED),
        DriftKey(key=(Decimal("1.2300"),), kind=DriftKind.EXTRA),
        DriftKey(key=(b"\x00\xff",), kind=DriftKind.CHANGED),
        DriftKey(key=(42, "alice", Decimal("9.99")), kind=DriftKind.MISSING),
    ]
    r = DriftReport(comparison="weird_keys", in_sync=False, drift_keys=keys)
    out = render_json(r)
    parsed = json.loads(out)  # must round-trip
    serialized = [item["key"] for item in parsed["drift_keys"]]
    assert serialized[0] == ["2026-06-20"]
    assert serialized[1] == ["2026-06-20T12:30:00"]
    assert serialized[2] == ["1.23"]  # Decimal trailing zeros trimmed
    assert serialized[3] == ["00ff"]  # bytes -> lowercase hex
    assert serialized[4] == [42, "alice", "9.99"]  # ints stay native, Decimal -> str


def test_render_text_non_native_key_types_no_crash():
    keys = [
        DriftKey(key=(dt.date(2026, 6, 20),), kind=DriftKind.MISSING),
        DriftKey(key=(Decimal("1.2300"),), kind=DriftKind.CHANGED),
    ]
    r = DriftReport(comparison="weird_keys", in_sync=False, drift_keys=keys)
    out = render_text(r)  # must not raise
    assert "2026-06-20" in out
    assert "1.23" in out


# --- print_report --------------------------------------------------------------


def test_print_report_text_returns_exit_code():
    import io

    buf = io.StringIO()
    code = print_report(_drift_report(), fmt="text", stream=buf)
    assert code == 1
    assert "DRIFT" in buf.getvalue()


def test_print_report_json_returns_exit_code():
    import io

    buf = io.StringIO()
    code = print_report(_in_sync_report(), fmt="json", stream=buf)
    assert code == 0
    json.loads(buf.getvalue())  # valid JSON written


def test_print_report_unknown_format_raises():
    import io

    try:
        print_report(_in_sync_report(), fmt="xml", stream=io.StringIO())
    except ValueError:
        pass
    else:
        raise AssertionError("expected ValueError for unknown format")


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
