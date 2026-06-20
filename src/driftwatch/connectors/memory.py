"""In-memory reference connector.

Holds tables as lists of dict rows and implements the Connector contract purely in
Python using the ``driftwatch.hashing`` reference. It is the canonical implementation:
the engine's unit tests run against it (no database, fully deterministic), and it is
the behaviour every SQL connector must match in the conformance test.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional, Sequence

from ..connector import Connector
from ..hashing import row_hash, segment_checksum
from ..models import Checksum, Key, KeyRange


class MemoryConnector(Connector):
    driver = "memory"

    def __init__(self, tables: Dict[str, List[Dict[str, Any]]]):
        # shallow-copy the row lists so external mutation doesn't change a live connector
        self._tables = {name: list(rows) for name, rows in tables.items()}

    # --- helpers ---------------------------------------------------------------

    def _rows(self, table: str) -> List[Dict[str, Any]]:
        if table not in self._tables:
            raise KeyError("memory connector has no table %r" % table)
        return self._tables[table]

    @staticmethod
    def _key_of(row: Dict[str, Any], pk_cols: Sequence[str]) -> Key:
        return tuple(row.get(c) for c in pk_cols)

    @staticmethod
    def _passes_cutoff(row: Dict[str, Any], watermark_column: Optional[str], cutoff: Optional[Any]) -> bool:
        if watermark_column is None or cutoff is None:
            return True
        wm = row.get(watermark_column)
        # mirrors SQL `watermark <= cutoff`: NULL/unknown is excluded
        return wm is not None and wm <= cutoff

    @staticmethod
    def _in_range(key: Key, key_range: KeyRange) -> bool:
        if key_range.lo is not None and key < key_range.lo:
            return False
        if key_range.hi is not None and key >= key_range.hi:
            return False
        return True

    def _selected(
        self,
        table: str,
        pk_cols: Sequence[str],
        key_range: Optional[KeyRange],
        watermark_column: Optional[str],
        cutoff: Optional[Any],
    ):
        for row in self._rows(table):
            if not self._passes_cutoff(row, watermark_column, cutoff):
                continue
            key = self._key_of(row, pk_cols)
            if key_range is not None and not self._in_range(key, key_range):
                continue
            yield key, row

    @staticmethod
    def _row_hash(row: Dict[str, Any], pk_cols: Sequence[str], compare_cols: Sequence[str], fp: int) -> int:
        values = [row.get(c) for c in pk_cols] + [row.get(c) for c in compare_cols]
        return row_hash(values, fp)

    # --- Connector interface ---------------------------------------------------

    def columns(self, table: str) -> List[str]:
        names = set()
        for row in self._rows(table):
            names.update(str(c).lower() for c in row.keys())
        return sorted(names)

    def pk_bounds(self, table, pk_cols, watermark_column, cutoff) -> Optional[KeyRange]:
        keys = [k for k, _ in self._selected(table, pk_cols, None, watermark_column, cutoff)]
        if not keys:
            return None
        return KeyRange(lo=min(keys), hi=max(keys))  # inclusive bounds

    def checksum(self, table, pk_cols, compare_cols, key_range, watermark_column, cutoff, float_precision) -> Checksum:
        count = 0
        hashes = []
        for _key, row in self._selected(table, pk_cols, key_range, watermark_column, cutoff):
            count += 1
            hashes.append(self._row_hash(row, pk_cols, compare_cols, float_precision))
        return Checksum(count=count, checksum=segment_checksum(hashes))

    def fetch_row_hashes(self, table, pk_cols, compare_cols, key_range, watermark_column, cutoff, float_precision) -> Dict[Key, int]:
        out: Dict[Key, int] = {}
        for key, row in self._selected(table, pk_cols, key_range, watermark_column, cutoff):
            out[key] = self._row_hash(row, pk_cols, compare_cols, float_precision)
        return out

    def fetch_row_hashes_for_keys(self, table, pk_cols, compare_cols, keys, watermark_column, cutoff, float_precision) -> Dict[Key, int]:
        wanted = set(keys)
        out: Dict[Key, int] = {}
        for key, row in self._selected(table, pk_cols, None, watermark_column, cutoff):
            if key in wanted:
                out[key] = self._row_hash(row, pk_cols, compare_cols, float_precision)
        return out
