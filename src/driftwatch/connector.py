"""The Connector interface and the driver registry.

A Connector is the *only* thing the engine talks to. It exposes a tiny, dialect-free
surface; each implementation translates these calls into native SQL and applies the
hashing contract from ``driftwatch.hashing``. The engine never sees a SQL string.

Resolution: ``get_connector(driver)`` looks the driver up in the ``driftwatch.connectors``
entry-point group (built-ins ship there; third-party packages can register their own).
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional, Sequence

from .models import Checksum, Key, KeyRange


class Connector(ABC):
    """Read-only access to one table on one backend.

    Implementations MUST be strictly read-only and MUST apply ``cutoff`` (when given)
    as ``watermark_column <= cutoff`` so the engine can exclude not-yet-propagated rows.
    All row hashing MUST follow ``driftwatch.hashing`` so digests match across backends.
    """

    #: dialect name, e.g. "postgres" - set by subclasses, informational.
    driver: str = ""

    # Range semantics: ``pk_bounds`` returns an INCLUSIVE [min, max] range (hi is the
    # actual max key). Every other method receives HALF-OPEN segment ranges ``[lo, hi)``
    # built by the engine: include a row iff ``(lo is None or key >= lo) and
    # (hi is None or key < hi)``. The engine controls column order by passing explicit
    # ``pk_cols`` + ``compare_cols`` lists; connectors MUST hash in exactly that order.

    @abstractmethod
    def columns(self, table: str) -> List[str]:
        """Return the table's column names in a canonical, lowercased form. Used by the
        engine to resolve ``compare_columns: "*"`` into an explicit, sorted, intersected
        list so both sides hash the same columns in the same order."""

    @abstractmethod
    def pk_bounds(
        self,
        table: str,
        pk_cols: Sequence[str],
        watermark_column: Optional[str],
        cutoff: Optional[Any],
    ) -> Optional[KeyRange]:
        """Return min/max primary key (as a closed range), or None if the table is empty
        within the cutoff. ``hi`` here is the actual max key (inclusive)."""

    @abstractmethod
    def checksum(
        self,
        table: str,
        pk_cols: Sequence[str],
        compare_cols: Sequence[str],
        key_range: KeyRange,
        watermark_column: Optional[str],
        cutoff: Optional[Any],
        float_precision: int,
    ) -> Checksum:
        """Aggregate (row count, SUM-of-row-hash mod 2**63) over rows whose key is in
        ``key_range`` and whose watermark is ``<= cutoff``. Computed natively in-engine."""

    @abstractmethod
    def fetch_row_hashes(
        self,
        table: str,
        pk_cols: Sequence[str],
        compare_cols: Sequence[str],
        key_range: KeyRange,
        watermark_column: Optional[str],
        cutoff: Optional[Any],
        float_precision: int,
    ) -> Dict[Key, int]:
        """Return ``{key_tuple: row_hash}`` for every row in ``key_range`` within cutoff.
        Used at leaf segments for the exact set-diff that classifies missing/extra/changed."""

    @abstractmethod
    def fetch_row_hashes_for_keys(
        self,
        table: str,
        pk_cols: Sequence[str],
        compare_cols: Sequence[str],
        keys: Sequence[Key],
        watermark_column: Optional[str],
        cutoff: Optional[Any],
        float_precision: int,
    ) -> Dict[Key, int]:
        """Like ``fetch_row_hashes`` but restricted to an explicit set of keys. Used by the
        recheck pass; ``cutoff`` may be None to read the freshest data for confirmation."""

    def close(self) -> None:  # pragma: no cover - optional cleanup hook
        """Release any underlying connection. Default no-op."""


# --- registry ------------------------------------------------------------------

# Built-in fallback map so the package works even if entry-point metadata is missing
# (e.g. running from a source checkout that wasn't `pip install`-ed).
_BUILTIN: Dict[str, str] = {
    "postgres": "driftwatch.connectors.postgres:PostgresConnector",
    "duckdb": "driftwatch.connectors.duckdb:DuckDBConnector",
    "snowflake": "driftwatch.connectors.snowflake:SnowflakeConnector",
    "memory": "driftwatch.connectors.memory:MemoryConnector",
}


def _load_target(target: str) -> type:
    module_name, _, attr = target.partition(":")
    import importlib

    module = importlib.import_module(module_name)
    return getattr(module, attr)


def available_drivers() -> List[str]:
    drivers = set(_BUILTIN)
    try:
        from importlib.metadata import entry_points

        eps = entry_points()
        group = eps.select(group="driftwatch.connectors") if hasattr(eps, "select") \
            else eps.get("driftwatch.connectors", [])  # type: ignore[attr-defined]
        for ep in group:
            drivers.add(ep.name)
    except Exception:
        pass
    return sorted(drivers)


def get_connector_class(driver: str) -> type:
    """Resolve a driver name to its Connector class via entry points, then built-ins."""
    try:
        from importlib.metadata import entry_points

        eps = entry_points()
        group = eps.select(group="driftwatch.connectors") if hasattr(eps, "select") \
            else eps.get("driftwatch.connectors", [])  # type: ignore[attr-defined]
        for ep in group:
            if ep.name == driver:
                return ep.load()
    except Exception:
        pass
    if driver in _BUILTIN:
        return _load_target(_BUILTIN[driver])
    raise KeyError(
        "unknown connector driver %r; available: %s" % (driver, ", ".join(available_drivers()))
    )
