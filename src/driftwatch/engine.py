"""The recursive hash-segmentation diff engine + lag handling.

This is the pure-logic core of driftwatch. It talks to two :class:`Connector`
instances and nothing else: it never sees a SQL string, never knows a dialect
exists. Everything fragile (canonicalization, hashing, watermark predicates) lives
behind the connector + hashing contracts.

Algorithm (recursive hash-segmentation, the ``data-diff``/``reladiff`` approach):

1. Resolve the compare columns + the fixed hashed column order (pk then sorted cmp).
2. Capture a single watermark ``cutoff`` once, reused for every query → determinism.
3. Get pk bounds from both sides; handle empties; build the global key range.
4. Recurse over the range: checksum() both sides; prune equal segments; subdivide
   unequal segments by integer interpolation; at leaves fetch_row_hashes() and
   set-diff to classify MISSING / EXTRA / CHANGED.
5. The leaf divergences are *candidates*. Re-fetch them (recheck pass) against the
   freshest data; drop any that have since reconciled (lag, not drift).
6. Survivors are confirmed drift → build a :class:`DriftReport`.

Operational errors (a connector raising a DB exception) propagate out as exceptions;
they are NEVER swallowed into ``in_sync=True``. The CLI maps them to exit code 2.
"""

from __future__ import annotations

import time
from datetime import datetime, timedelta, timezone
from typing import Callable, Dict, List, Optional, Sequence, Tuple

from .config import ComparisonConfig
from .connector import Connector
from .models import Checksum, DriftKey, DriftKind, DriftReport, Key, KeyRange


class EngineError(Exception):
    """Wraps an operational failure encountered during a comparison.

    The engine does not *need* to raise this - re-raising the connector's own
    exception is equally valid - but it gives the CLI a single type to catch when
    it wants to attribute the failure to the engine layer. ``in_sync`` is never
    reported True when an error occurs.
    """


def compare(
    source: Connector,
    target: Connector,
    cmp: ComparisonConfig,
    now: Optional[datetime] = None,
    sleep: Callable[[float], None] = time.sleep,
) -> DriftReport:
    """Compare ``cmp.source_table`` on ``source`` against ``cmp.target_table`` on ``target``.

    :param now: injected clock for deterministic cutoff in tests; defaults to
        ``datetime.now(timezone.utc)`` when a watermark cutoff is needed.
    :param sleep: injected sleep for deterministic recheck delays in tests.
    :returns: a populated :class:`DriftReport`. Operational errors propagate.
    """
    started = datetime.now(timezone.utc)

    # --- 1. resolve compare columns + the fixed hashed column order ------------
    pk_cols: List[str] = list(cmp.primary_key)
    compare_cols = _resolve_compare_columns(source, target, cmp, pk_cols)
    fp = cmp.float_precision

    # --- 2. capture the watermark cutoff ONCE ---------------------------------
    if cmp.watermark_column:
        base = now if now is not None else datetime.now(timezone.utc)
        cutoff = base - timedelta(seconds=cmp.grace_seconds)
        cutoff_iso: Optional[str] = cutoff.isoformat()
    else:
        cutoff = None
        cutoff_iso = None

    wm = cmp.watermark_column

    # --- 3. pk bounds from both sides; handle empties -------------------------
    src_bounds = source.pk_bounds(cmp.source_table, pk_cols, wm, cutoff)
    tgt_bounds = target.pk_bounds(cmp.target_table, pk_cols, wm, cutoff)

    rows_compared = 0
    segments_scanned = 0
    candidates: List[DriftKey] = []

    # --- 2b. fetch the in-flight key set ONCE (symmetric watermark) -----------
    # When a watermark is configured, a source row that was just updated carries a
    # fresh watermark (> cutoff) and is excluded from the SOURCE by the cutoff. Its
    # stale copy on the TARGET still passes the cutoff, so without correction it
    # would be wrongly reported as EXTRA. We fetch the keys that are too fresh to
    # have synced and drop them from the TARGET side at every leaf so an
    # updated-but-not-yet-propagated row is never reported as drift.
    # No watermark → empty set → behaviour unchanged.
    inflight: set = set()
    if wm is not None:
        inflight = set(
            source.keys_above_watermark(
                cmp.source_table, pk_cols, KeyRange(), wm, cutoff
            )
        )

    if src_bounds is None and tgt_bounds is None:
        # both empty within cutoff → trivially in sync
        pass
    elif src_bounds is None or tgt_bounds is None:
        # exactly one side is empty → every key on the populated side is drift.
        # MISSING if the row exists only in source; EXTRA if only in target.
        full = KeyRange()  # whole table (unbounded both ends)
        if tgt_bounds is None:
            src_hashes = source.fetch_row_hashes(
                cmp.source_table, pk_cols, compare_cols, full, wm, cutoff, fp
            )
            rows_compared += len(src_hashes)
            candidates = [DriftKey(key=k, kind=DriftKind.MISSING) for k in src_hashes]
        else:
            tgt_hashes = target.fetch_row_hashes(
                cmp.target_table, pk_cols, compare_cols, full, wm, cutoff, fp
            )
            rows_compared += len(tgt_hashes)
            # symmetric watermark: a target row whose source copy is in flight (fresh
            # watermark > cutoff) is not yet synced, not stale → not EXTRA.
            candidates = [
                DriftKey(key=k, kind=DriftKind.EXTRA)
                for k in tgt_hashes
                if k not in inflight
            ]
        segments_scanned += 1
    else:
        # both sides populated → build the global range and walk it recursively.
        # pk_bounds is INCLUSIVE [lo, hi]; the half-open walk would EXCLUDE a key
        # equal to hi, so we must extend the upper bound to include the global max.
        #   - single-column integer PK: hi = (max + 1,) → half-open [lo, max+1)
        #     includes max AND stays numerically splittable (the whole point).
        #   - anything else: hi = None (unbounded high) so max is included; such a
        #     top segment is not numerically splittable and resolves as one leaf.
        global_lo = min(src_bounds.lo, tgt_bounds.lo)
        global_hi_key = max(src_bounds.hi, tgt_bounds.hi)  # inclusive max key tuple
        if len(pk_cols) == 1 and _is_int(global_hi_key[0]):
            global_hi: Optional[Key] = (global_hi_key[0] + 1,)
        else:
            global_hi = None
        global_range = KeyRange(lo=global_lo, hi=global_hi)

        result = _walk(
            source,
            target,
            cmp,
            pk_cols,
            compare_cols,
            fp,
            wm,
            cutoff,
            global_range,
            inflight,
        )
        candidates = result.drift_keys
        rows_compared = result.rows_compared
        segments_scanned = result.segments_scanned

    candidates_before_recheck = len(candidates)

    # --- 5. recheck pass: drop candidates that have since reconciled ----------
    confirmed = _recheck(
        source,
        target,
        cmp,
        pk_cols,
        compare_cols,
        fp,
        candidates,
        sleep,
    )

    # --- 6. build the report --------------------------------------------------
    finished = datetime.now(timezone.utc)
    report = DriftReport(
        comparison=cmp.name,
        in_sync=(len(confirmed) == 0),
        drift_keys=confirmed,
        rows_compared=rows_compared,
        segments_scanned=segments_scanned,
        candidates_before_recheck=candidates_before_recheck,
        cutoff=cutoff_iso,
        started_at=started.isoformat(),
        finished_at=finished.isoformat(),
        duration_seconds=(finished - started).total_seconds(),
    )
    return report


# --- column resolution --------------------------------------------------------


def _resolve_compare_columns(
    source: Connector,
    target: Connector,
    cmp: ComparisonConfig,
    pk_cols: Sequence[str],
) -> List[str]:
    """Return the compare columns in their FIXED hashed order (always sorted).

    If ``cmp.compare_columns`` is given it is used verbatim but still sorted (the
    hashed order is always pk-cols-in-pk-order then sorted-compare-cols, so both
    sides hash the same columns in the same order). Otherwise it is the sorted
    intersection of both sides' columns minus the pk and any excluded columns.
    """
    if cmp.compare_columns is not None:
        cols = list(cmp.compare_columns)
    else:
        src_cols = set(source.columns(cmp.source_table))
        tgt_cols = set(target.columns(cmp.target_table))
        cols = list(src_cols & tgt_cols)

    excluded = set(pk_cols) | set(cmp.exclude_columns)
    return sorted(c for c in cols if c not in excluded)


# --- the recursive segment walk ----------------------------------------------


class _WalkResult:
    """Mutable accumulator threaded through the recursion (cheaper than tuples)."""

    __slots__ = ("drift_keys", "rows_compared", "segments_scanned")

    def __init__(self) -> None:
        self.drift_keys: List[DriftKey] = []
        self.rows_compared: int = 0
        self.segments_scanned: int = 0


def _walk(
    source: Connector,
    target: Connector,
    cmp: ComparisonConfig,
    pk_cols: Sequence[str],
    compare_cols: Sequence[str],
    fp: int,
    wm: Optional[str],
    cutoff: object,
    key_range: KeyRange,
    inflight: set,
) -> _WalkResult:
    """Iterative driver for the recursive segment walk (avoids deep call stacks on
    very large/sparse tables). Maintains an explicit work-stack of ranges."""
    result = _WalkResult()
    stack: List[KeyRange] = [key_range]

    while stack:
        rng = stack.pop()
        _process_segment(
            source,
            target,
            cmp,
            pk_cols,
            compare_cols,
            fp,
            wm,
            cutoff,
            rng,
            result,
            stack,
            inflight,
        )
    return result


def _process_segment(
    source: Connector,
    target: Connector,
    cmp: ComparisonConfig,
    pk_cols: Sequence[str],
    compare_cols: Sequence[str],
    fp: int,
    wm: Optional[str],
    cutoff: object,
    rng: KeyRange,
    result: _WalkResult,
    stack: List[KeyRange],
    inflight: set,
) -> None:
    """Handle one segment: prune, recurse, or fall to a leaf set-diff."""
    src_ck: Checksum = source.checksum(
        cmp.source_table, pk_cols, compare_cols, rng, wm, cutoff, fp
    )
    tgt_ck: Checksum = target.checksum(
        cmp.target_table, pk_cols, compare_cols, rng, wm, cutoff, fp
    )

    # (b) counts AND checksums equal → this whole segment matches; prune it.
    if src_ck.count == tgt_ck.count and src_ck.checksum == tgt_ck.checksum:
        result.segments_scanned += 1
        return

    if max(src_ck.count, tgt_ck.count) > cmp.leaf_size:
        sub_ranges = _split_segment(source, cmp, pk_cols, wm, cutoff, rng)
    else:
        sub_ranges = None

    # (c) small enough OR not splittable → exact leaf set-diff.
    if sub_ranges is None:
        result.segments_scanned += 1
        _leaf_diff(
            source,
            target,
            cmp,
            pk_cols,
            compare_cols,
            fp,
            wm,
            cutoff,
            rng,
            result,
            inflight,
        )
        return

    # (d) too big and splittable → subdivide and recurse.
    result.segments_scanned += 1
    stack.extend(sub_ranges)


def _leaf_diff(
    source: Connector,
    target: Connector,
    cmp: ComparisonConfig,
    pk_cols: Sequence[str],
    compare_cols: Sequence[str],
    fp: int,
    wm: Optional[str],
    cutoff: object,
    rng: KeyRange,
    result: _WalkResult,
    inflight: set,
) -> None:
    """Fetch every row hash on both sides over ``rng`` and classify each key."""
    src_hashes: Dict[Key, int] = source.fetch_row_hashes(
        cmp.source_table, pk_cols, compare_cols, rng, wm, cutoff, fp
    )
    tgt_hashes: Dict[Key, int] = target.fetch_row_hashes(
        cmp.target_table, pk_cols, compare_cols, rng, wm, cutoff, fp
    )
    # symmetric watermark: drop in-flight keys from BOTH sides before the set-diff.
    # They are already absent from the source (excluded by the cutoff); removing
    # them from the target stops an updated-but-not-yet-synced row from being
    # reported as EXTRA/CHANGED. inflight is empty when no watermark is configured.
    if inflight:
        for k in inflight:
            src_hashes.pop(k, None)
            tgt_hashes.pop(k, None)
    # rows_compared is the number of distinct rows examined at this leaf.
    result.rows_compared += len(set(src_hashes) | set(tgt_hashes))

    for key, sh in src_hashes.items():
        th = tgt_hashes.get(key)
        if th is None:
            result.drift_keys.append(DriftKey(key=key, kind=DriftKind.MISSING))
        elif sh != th:
            result.drift_keys.append(DriftKey(key=key, kind=DriftKind.CHANGED))
    for key in tgt_hashes:
        if key not in src_hashes:
            result.drift_keys.append(DriftKey(key=key, kind=DriftKind.EXTRA))


# --- range splitting ----------------------------------------------------------


def _split_segment(
    source: Connector,
    cmp: ComparisonConfig,
    pk_cols: Sequence[str],
    wm: Optional[str],
    cutoff: object,
    rng: KeyRange,
) -> Optional[List[KeyRange]]:
    """Decide how to subdivide an oversized segment.

    Fast path (unchanged from v1): a single-column integer PK with concrete,
    distinct bounds is split by pure integer interpolation - NO database scan, so
    the common surrogate-id case stays cheap. We must NOT call ``split_points`` for
    it (that would add a needless scan).

    Fallback (FIX 1): composite / string / UUID keys - or an integer segment whose
    bounds are not interpolatable (e.g. an unbounded end) - now ask the SOURCE
    connector for percentile-style boundary keys via ``split_points``. If it returns
    a non-empty, strictly-increasing list ``b1<...<bk`` strictly inside ``(lo, hi)``
    we form half-open sub-ranges ``[lo,b1),[b1,b2),...,[bk,hi)`` that partition
    ``[lo,hi)`` exactly (no gaps, no overlap). If it returns None, the caller keeps
    the old whole-range leaf behaviour.
    """
    # 1) integer fast path - try the pure-arithmetic split first, no scan.
    integer_split = _split(rng, pk_cols, cmp.segment_fanout)
    if integer_split is not None:
        return integer_split

    # A single-column integer PK with concrete distinct bounds was handled above.
    # Reaching here means either a non-integer/composite key, or an integer segment
    # whose bounds can't be interpolated (unbounded/adjacent). Ask the source to
    # split by row percentiles instead of reading the whole range as one leaf.
    boundaries = source.split_points(
        cmp.source_table, pk_cols, rng, wm, cutoff, cmp.segment_fanout
    )
    if not boundaries:
        return None  # not splittable → caller treats rng as a single leaf

    # Build [lo, b1), [b1, b2), ..., [bk, hi). The first sub-range inherits rng.lo
    # and the last inherits rng.hi exactly, so the union equals the parent range.
    sub_ranges: List[KeyRange] = []
    prev: Optional[Key] = rng.lo
    for b in boundaries:
        sub_ranges.append(KeyRange(lo=prev, hi=b))
        prev = b
    sub_ranges.append(KeyRange(lo=prev, hi=rng.hi))
    return sub_ranges


def _split(
    rng: KeyRange, pk_cols: Sequence[str], fanout: int
) -> Optional[List[KeyRange]]:
    """Split ``rng`` into up to ``fanout`` half-open sub-ranges, or None if the
    range is not numerically splittable.

    "Numerically splittable" (v1) = a single-column integer PK with concrete,
    distinct integer bounds. We interpolate ``fanout`` evenly spaced boundaries
    across ``[lo, hi)`` and emit contiguous half-open sub-ranges. The first
    sub-range inherits ``rng.lo`` and the last inherits ``rng.hi`` exactly so the
    union of the children is identical to the parent (no key gained or lost).

    # ponytail: composite or non-integer keys fall back to a single leaf fetch over
    # the whole range (return None → caller treats it as a leaf). Percentile-based
    # splitting for arbitrary key types is the v2 upgrade; for v1 the integer fast
    # path covers the surrogate-id case that dominates real CDC pipelines, and the
    # whole-range leaf fetch is still correct (just less selective) for the rest.
    """
    if len(pk_cols) != 1:
        return None

    lo_key = rng.lo
    hi_key = rng.hi

    # We can only interpolate when BOTH ends are concrete integers. An unbounded end
    # (None) carries no numeric value to interpolate against → not splittable here
    # (the caller will treat such a segment as a single leaf). The top global segment
    # for an integer PK is given a concrete hi=(max+1) by ``compare`` precisely so it
    # stays splittable; only non-integer PK top segments ever reach here with hi=None.
    lo_val = lo_key[0] if lo_key is not None else None
    hi_val = hi_key[0] if hi_key is not None else None

    if not _is_int(lo_val) or not _is_int(hi_val):
        return None

    lo_int = int(lo_val)
    hi_int = int(hi_val)
    if hi_int - lo_int < 1:
        # nothing to split (adjacent or inverted) → treat as a leaf
        return None

    n = max(2, fanout)
    # Build n+1 evenly spaced integer boundaries from lo_int to hi_int inclusive.
    boundaries: List[int] = []
    for i in range(n + 1):
        b = lo_int + (hi_int - lo_int) * i // n
        boundaries.append(b)
    # Deduplicate while preserving order (small ranges with large fanout collapse).
    uniq: List[int] = []
    for b in boundaries:
        if not uniq or uniq[-1] != b:
            uniq.append(b)
    if len(uniq) < 2:
        return None

    sub_ranges: List[KeyRange] = []
    for i in range(len(uniq) - 1):
        seg_lo: Optional[Key] = (uniq[i],)
        seg_hi: Optional[Key] = (uniq[i + 1],)
        if i == 0:
            seg_lo = rng.lo  # preserve the parent's exact lower bound
        if i == len(uniq) - 2:
            seg_hi = rng.hi  # preserve the parent's exact upper bound (may be None)
        sub_ranges.append(KeyRange(lo=seg_lo, hi=seg_hi))
    return sub_ranges


def _is_int(value: object) -> bool:
    """True for genuine integers (not bools, which are int subclasses in Python)."""
    return isinstance(value, int) and not isinstance(value, bool)


# --- recheck pass -------------------------------------------------------------


def _recheck(
    source: Connector,
    target: Connector,
    cmp: ComparisonConfig,
    pk_cols: Sequence[str],
    compare_cols: Sequence[str],
    fp: int,
    candidates: List[DriftKey],
    sleep: Callable[[float], None],
) -> List[DriftKey]:
    """Re-confirm candidate divergences against the freshest data.

    For each of ``cmp.recheck.rounds`` rounds (when there are still candidates):
    sleep the configured delay, then re-fetch the candidate keys on both sides with
    ``cutoff=None`` (freshest data) and reclassify. Keys that have reconciled (the
    lag has caught up) are dropped; only keys that STILL diverge survive. After the
    rounds, survivors are the confirmed drift.
    """
    survivors = list(candidates)
    rounds = cmp.recheck.rounds
    delay = cmp.recheck.delay_seconds

    if rounds <= 0 or not survivors:
        return survivors

    for _round in range(rounds):
        if not survivors:
            break
        if delay > 0:
            sleep(delay)

        keys = [dk.key for dk in survivors]
        # cutoff=None → read the freshest data on both sides for confirmation.
        src_hashes = source.fetch_row_hashes_for_keys(
            cmp.source_table, pk_cols, compare_cols, keys, cmp.watermark_column, None, fp
        )
        tgt_hashes = target.fetch_row_hashes_for_keys(
            cmp.target_table, pk_cols, compare_cols, keys, cmp.watermark_column, None, fp
        )

        still: List[DriftKey] = []
        for key in keys:
            kind = _classify(key, src_hashes, tgt_hashes)
            if kind is not None:
                still.append(DriftKey(key=key, kind=kind))
        survivors = still

    return survivors


def _classify(
    key: Key, src_hashes: Dict[Key, int], tgt_hashes: Dict[Key, int]
) -> Optional[DriftKind]:
    """Re-classify a single key from freshly fetched hashes, or None if it now matches."""
    sh = src_hashes.get(key)
    th = tgt_hashes.get(key)
    if sh is None and th is None:
        # gone from both sides → no longer a divergence
        return None
    if th is None:
        return DriftKind.MISSING  # in source, not target
    if sh is None:
        return DriftKind.EXTRA  # in target, not source
    if sh != th:
        return DriftKind.CHANGED
    return None  # both present and equal → reconciled
