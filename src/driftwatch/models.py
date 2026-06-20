"""Core data models shared across the engine, connectors, and reporter.

These are plain, dependency-free dataclasses. They are a frozen contract: parallel
modules (engine, connectors, reporter) all speak in these types and nothing else.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, List, Optional, Tuple

# A primary-key value is a tuple even for single-column keys, so the whole codebase
# can treat single and composite keys uniformly.
Key = Tuple[Any, ...]


class DriftKind(str, Enum):
    """How a key diverges between source and target."""

    MISSING = "missing"  # present in source, absent in target (target is behind / dropped)
    EXTRA = "extra"      # present in target, absent in source (target has stale/phantom rows)
    CHANGED = "changed"  # present in both, but row content differs


@dataclass(frozen=True)
class KeyRange:
    """A half-open primary-key range ``[lo, hi)`` over the (single or composite) key.

    ``lo``/``hi`` are key tuples. ``lo=None`` means unbounded-low, ``hi=None`` means
    unbounded-high (used for the initial whole-table range and the last segment).
    Connectors translate this into a dialect-specific ``WHERE`` predicate.
    """

    lo: Optional[Key] = None
    hi: Optional[Key] = None


@dataclass(frozen=True)
class Checksum:
    """Result of summarizing a key range on one side: row count + aggregate checksum."""

    count: int
    checksum: int


@dataclass(frozen=True)
class DriftKey:
    """A single confirmed (or candidate) divergence."""

    key: Key
    kind: DriftKind


@dataclass
class DriftReport:
    """The outcome of one comparison run."""

    comparison: str
    in_sync: bool
    drift_keys: List[DriftKey] = field(default_factory=list)
    rows_compared: int = 0
    segments_scanned: int = 0
    candidates_before_recheck: int = 0
    cutoff: Optional[str] = None  # ISO-8601 string of the watermark cutoff, if any
    started_at: Optional[str] = None
    finished_at: Optional[str] = None
    duration_seconds: float = 0.0
    error: Optional[str] = None  # set on operational error; in_sync is meaningless if set

    def counts_by_kind(self) -> Dict[str, int]:
        out = {k.value: 0 for k in DriftKind}
        for dk in self.drift_keys:
            out[dk.kind.value] += 1
        return out

    def summary(self) -> Dict[str, Any]:
        return {
            "comparison": self.comparison,
            "in_sync": self.in_sync,
            "drift_total": len(self.drift_keys),
            "drift_by_kind": self.counts_by_kind(),
            "rows_compared": self.rows_compared,
            "segments_scanned": self.segments_scanned,
            "candidates_before_recheck": self.candidates_before_recheck,
            "cutoff": self.cutoff,
            "duration_seconds": round(self.duration_seconds, 3),
            "error": self.error,
        }
