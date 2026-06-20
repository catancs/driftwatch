"""The output layer: exit-code mapping + human-readable text + machine JSON.

The reporter is the only place that turns a :class:`~driftwatch.models.DriftReport`
into something a human or a CI runner consumes. It is deliberately dependency-free
(stdlib ``json`` only) and side-effect-free except for the thin ``print_report``
convenience the CLI uses.

Three exit codes are produced here (config errors are exit 3 and are the CLI's job,
not the reporter's):

* ``0`` - clean run, no error, in sync.
* ``1`` - clean run, no error, confirmed drift.
* ``2`` - operational error (``report.error`` is set); the verdict is meaningless.

We **never** report in-sync when an error is present: a set ``error`` always wins.
"""

from __future__ import annotations

import datetime as _dt
import json
import sys
from decimal import Decimal
from typing import IO, Any, List

from .models import DriftKind, DriftReport

# Order in which drift kinds are shown, so text/JSON output is stable regardless of
# the order keys happened to be appended to the report.
_KIND_ORDER = [DriftKind.MISSING.value, DriftKind.EXTRA.value, DriftKind.CHANGED.value]


# --- exit codes ----------------------------------------------------------------


def exit_code(report: DriftReport) -> int:
    """Map a report to a process exit code.

    ``2`` if an operational error is present (verdict is meaningless), else ``1``
    for confirmed drift, else ``0`` for in-sync. Config errors (exit ``3``) are
    handled by the CLI before a report ever exists.
    """
    if report.error is not None:
        return 2
    return 0 if report.in_sync else 1


# --- JSON-safe key scalar conversion -------------------------------------------


def _json_scalar(value: Any) -> Any:
    """Coerce one key-tuple scalar into a deterministic JSON-native value.

    JSON natively understands ``None``/``bool``/``int``/``float``/``str``; we pass
    those through unchanged so round-tripping is lossless where it can be. Everything
    else a primary key might contain - ``date``, ``datetime``, ``Decimal``, ``bytes`` -
    is rendered to a stable string (reusing the cross-dialect ``canonical`` forms where
    they exist) so ``json.dumps`` can never choke on a key.
    """
    # bool is a subclass of int but is JSON-native, so it needs no special handling
    # beyond being listed before the broader checks for documentation's sake.
    if value is None or isinstance(value, (bool, int, str)):
        return value
    if isinstance(value, float):
        # NaN / inf are not valid JSON; stringify them so the document stays portable.
        if value != value or value in (float("inf"), float("-inf")):
            return repr(value)
        return value
    if isinstance(value, Decimal):
        # Stringify (don't float-cast) to preserve precision deterministically.
        if value.is_nan() or value.is_infinite():
            return str(value)
        return format(value.normalize(), "f") if value != value.to_integral_value() else str(value.quantize(Decimal(1)))
    if isinstance(value, _dt.datetime):
        return value.isoformat()
    if isinstance(value, _dt.date):
        return value.isoformat()
    if isinstance(value, (bytes, bytearray)):
        return bytes(value).hex()
    return str(value)


def _key_to_json(key: Any) -> List[Any]:
    """Render a key tuple as a JSON-safe list of scalars."""
    return [_json_scalar(part) for part in key]


# --- machine JSON --------------------------------------------------------------


def render_json(report: DriftReport) -> str:
    """Deterministic JSON string with the full structured result.

    Contains every field from :meth:`DriftReport.summary` plus the complete list of
    drift keys, each as ``{"key": [...], "kind": "..."}``. Keys are sorted and the
    output is round-trippable through ``json.loads``.
    """
    payload = dict(report.summary())
    payload["drift_keys"] = [
        {"key": _key_to_json(dk.key), "kind": dk.kind.value} for dk in report.drift_keys
    ]
    return json.dumps(payload, sort_keys=True, ensure_ascii=False)


# --- human-readable text -------------------------------------------------------


def _headline(report: DriftReport) -> str:
    if report.error is not None:
        return "ERROR"
    return "IN SYNC" if report.in_sync else "DRIFT"


def _format_key(key: Any) -> str:
    """Render a key tuple for the text report (single-col keys shown bare)."""
    parts = [str(_json_scalar(p)) for p in key]
    if len(parts) == 1:
        return parts[0]
    return "(" + ", ".join(parts) + ")"


def render_text(report: DriftReport, sample: int = 20) -> str:
    """Concise, terminal/CI-friendly summary of a report.

    Shows the comparison name, an ``IN SYNC`` / ``DRIFT`` / ``ERROR`` headline,
    counts by kind, the run statistics, and up to ``sample`` diverging keys with a
    ``... and N more`` line when the list is truncated.
    """
    lines: List[str] = []
    lines.append("driftwatch: {comparison} - {headline}".format(
        comparison=report.comparison, headline=_headline(report)))

    if report.error is not None:
        lines.append("  error: {}".format(report.error))
        # An errored run has no trustworthy verdict, so stop after the error.
        return "\n".join(lines)

    counts = report.counts_by_kind()
    total = len(report.drift_keys)
    lines.append("  drift keys: {total} total ({by_kind})".format(
        total=total,
        by_kind=", ".join("{}={}".format(k, counts.get(k, 0)) for k in _KIND_ORDER),
    ))
    lines.append("  rows compared: {}".format(report.rows_compared))
    lines.append("  segments scanned: {}".format(report.segments_scanned))
    lines.append("  candidates before recheck: {}".format(report.candidates_before_recheck))
    lines.append("  cutoff: {}".format(report.cutoff if report.cutoff is not None else "-"))
    lines.append("  duration: {:.3f}s".format(report.duration_seconds))

    if total:
        lines.append("  diverging keys:")
        for dk in report.drift_keys[:sample]:
            lines.append("    [{kind}] {key}".format(kind=dk.kind.value, key=_format_key(dk.key)))
        if total > sample:
            lines.append("    ... and {} more".format(total - sample))

    return "\n".join(lines)


# --- CLI convenience -----------------------------------------------------------


def print_report(report: DriftReport, fmt: str = "text", stream: IO[str] = None) -> int:
    """Render ``report`` in ``fmt`` (``"text"`` or ``"json"``) to ``stream``.

    Thin helper for the CLI: writes the rendered report and returns the exit code so
    the caller can ``sys.exit(print_report(...))``. Defaults to ``sys.stdout``.
    """
    if stream is None:
        stream = sys.stdout
    if fmt == "json":
        rendered = render_json(report)
    elif fmt == "text":
        rendered = render_text(report)
    else:
        raise ValueError("unknown format: {!r} (expected 'text' or 'json')".format(fmt))
    stream.write(rendered + "\n")
    return exit_code(report)
