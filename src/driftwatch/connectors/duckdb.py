"""DuckDB connector - the local test/e2e warehouse stand-in.

This connector reproduces the :mod:`driftwatch.hashing` contract **in SQL** so that a
DuckDB table yields byte-for-byte identical row hashes and segment checksums to the
Python reference (:class:`~driftwatch.connectors.memory.MemoryConnector`). Because it
runs entirely in-process with no external service, it is the warehouse stand-in for the
engine's conformance / e2e tests; if DuckDB and the Python reference agree here, the
contract is sound and the heavier Postgres / Snowflake connectors can be trusted to
follow the same SQL recipe.

Contract reproduction notes (see ``tests/test_duckdb_connector.py`` for the proof):

* Per-field canonical text is produced by :meth:`_canon_sql`, one ``CASE``/cast
  expression per column type, mirroring :func:`driftwatch.hashing.canonical`:

  - NULL -> ``'\\N'`` via an outer ``COALESCE`` (NOT via ``concat_ws`` NULL-skipping,
    which would silently drop the separator - see below).
  - bool -> ``'1'`` / ``'0'``
  - integers -> base-10 text (the default ``::VARCHAR`` cast)
  - DECIMAL/NUMERIC -> fixed text with trailing zeros (and a bare ``.``) trimmed
  - DOUBLE/FLOAT/REAL -> ``printf('%.<p>g', v)`` which is the same C ``%g`` formatter
    Python's ``format(v, '.<p>g')`` uses, so significant-digit output matches exactly
  - TIMESTAMP -> ``strftime(v, '%Y-%m-%d %H:%M:%S.%f')`` (microseconds, space sep)
  - TIMESTAMPTZ -> converted to UTC then formatted the same way (no tz suffix)
  - DATE -> ``%Y-%m-%d``
  - BLOB/bytes -> ``lower(hex(v))``
  - everything else -> ``::VARCHAR`` (text passes through as UTF-8)

* The row payload is ``concat_ws(chr(31), COALESCE(canon, '\\N'), ...)``. ``concat_ws``
  *skips NULL arguments entirely*, which would corrupt the separator layout, so every
  field is wrapped in ``COALESCE(..., '\\N')`` BEFORE it reaches ``concat_ws`` -
  guaranteeing no argument is ever NULL.

* row hash = ``('0x' || substr(md5(payload), 1, 15))::UBIGINT`` - the integer value of
  the first 15 hex chars (60 bits) of the MD5 digest, identical to Python's
  ``int(md5_hex[:15], 16)``. UBIGINT (64-bit unsigned) holds the full 60-bit value.

* segment checksum = ``SUM(row_hash::HUGEINT) % 9223372036854775808`` - DuckDB promotes
  the SUM to HUGEINT (128-bit) so it cannot overflow, and the modulo matches Python's
  ``segment_checksum`` (``SUM mod 2**63``).

Half-open ranges ``[lo, hi)`` over single or composite keys are expanded into a
lexicographic predicate (DuckDB has no usable row-value ``(a,b) >= (x,y)`` operator).
All literals (range bounds, key tuples, cutoff) are parameter-bound, never interpolated.
The connection is opened ``read_only`` against a real file and never issues DML/DDL.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional, Sequence

from ..connector import Connector
from ..hashing import CHECKSUM_MOD, FIELD_SEP, NULL_SENTINEL
from ..models import Checksum, Key, KeyRange

# chr(31) Unit Separator, as a SQL-safe integer for chr(); avoids embedding a control
# character literal in generated SQL.
_FIELD_SEP_ORD = ord(FIELD_SEP)  # 31


class DuckDBConnector(Connector):
    """Read-only DuckDB connector that reproduces the hashing contract in SQL."""

    driver = "duckdb"

    def __init__(self, path: str = ":memory:", read_only: bool = False, **kwargs: Any):
        import duckdb  # imported lazily so the package works without the extra installed

        self._path = path
        # An on-disk database can be opened strictly read-only; ``:memory:`` cannot be
        # (there is nothing to read), and the test harness needs to populate it, so we
        # only pass read_only for real files. Either way this connector issues no writes.
        connect_kwargs = dict(kwargs)
        if read_only and path != ":memory:":
            connect_kwargs["read_only"] = True
        self._con = duckdb.connect(path, **connect_kwargs)

    # --- identifier / table helpers -------------------------------------------

    @staticmethod
    def _quote_ident(name: str) -> str:
        """Quote a single identifier for DuckDB (double quotes, doubled internally)."""
        return '"' + str(name).replace('"', '""') + '"'

    @classmethod
    def _quote_table(cls, table: str) -> str:
        """Quote a possibly schema-qualified table name (``schema.name`` or ``name``).

        Splits on the first dot only; a dotted *unqualified* name is unusual but we keep
        the conservative behaviour of treating the first dot as the schema separator,
        which matches how the engine passes ``main.orders`` style names.
        """
        if "." in table:
            schema, _, rest = table.partition(".")
            return cls._quote_ident(schema) + "." + cls._quote_ident(rest)
        return cls._quote_ident(table)

    # --- canonicalization SQL --------------------------------------------------

    def _column_types(self, table: str) -> Dict[str, str]:
        """Return ``{lowercased_column_name: duckdb_data_type}`` for a table."""
        # Resolve schema/name for information_schema lookup.
        if "." in table:
            schema, _, name = table.partition(".")
            rows = self._con.execute(
                "SELECT column_name, data_type FROM information_schema.columns "
                "WHERE table_schema = ? AND table_name = ?",
                [schema, name],
            ).fetchall()
        else:
            rows = self._con.execute(
                "SELECT column_name, data_type FROM information_schema.columns "
                "WHERE table_name = ?",
                [table],
            ).fetchall()
        return {str(c).lower(): str(t) for c, t in rows}

    @staticmethod
    def _canon_sql(col_ident: str, data_type: str, float_precision: int) -> str:
        """Return a SQL expression giving the canonical text of one column value.

        ``col_ident`` is the already-quoted column reference; ``data_type`` is the
        DuckDB ``information_schema`` type string. The result may be NULL when the
        underlying value is NULL - the caller is responsible for the outer COALESCE to
        the NULL sentinel.
        """
        t = data_type.upper()

        # BOOLEAN -> '1'/'0' (matches Python: bool before int). A NULL boolean must stay
        # NULL so the caller's COALESCE maps it to the sentinel - a bare CASE WHEN/ELSE
        # would collapse NULL to '0', so guard with an explicit IS NULL -> NULL branch.
        if t.startswith("BOOLEAN"):
            return (
                f"CASE WHEN {col_ident} IS NULL THEN NULL "
                f"WHEN {col_ident} THEN '1' ELSE '0' END"
            )

        # DECIMAL/NUMERIC -> fixed text, trailing zeros (and trailing '.') trimmed.
        if t.startswith("DECIMAL") or t.startswith("NUMERIC"):
            s = f"({col_ident})::VARCHAR"
            return (
                f"CASE WHEN strpos({s}, '.') > 0 "
                f"THEN rtrim(rtrim({s}, '0'), '.') ELSE {s} END"
            )

        # Floating point -> Python's format(v, '.<p>g') == C printf %.<p>g.
        if t in ("DOUBLE", "FLOAT", "REAL") or t.startswith("DOUBLE") or t.startswith("FLOAT"):
            return f"printf('%.{int(float_precision)}g', ({col_ident})::DOUBLE)"

        # Timestamp with time zone -> normalize to UTC, then format with no tz suffix.
        if "TIMESTAMP" in t and "TIME ZONE" in t:
            return (
                f"strftime(timezone('UTC', {col_ident}), '%Y-%m-%d %H:%M:%S.%f')"
            )
        # Plain timestamp (already naive / treated as UTC by the contract).
        if t.startswith("TIMESTAMP"):
            return f"strftime({col_ident}, '%Y-%m-%d %H:%M:%S.%f')"

        # DATE -> ISO date.
        if t.startswith("DATE"):
            return f"strftime({col_ident}, '%Y-%m-%d')"

        # BLOB / bytes -> lowercase hex.
        if t.startswith("BLOB") or t.startswith("BYTEA") or t.startswith("VARBINARY"):
            return f"lower(hex({col_ident}))"

        # Integers and everything else -> plain text. Integer ::VARCHAR is base-10,
        # text passes through unchanged.
        return f"({col_ident})::VARCHAR"

    def _row_hash_sql(
        self,
        col_types: Dict[str, str],
        pk_cols: Sequence[str],
        compare_cols: Sequence[str],
        float_precision: int,
    ) -> str:
        """Build the SQL expression for one row's 60-bit hash (UBIGINT)."""
        order = list(pk_cols) + list(compare_cols)
        fields = []
        for col in order:
            ident = self._quote_ident(col)
            dtype = col_types.get(str(col).lower(), "VARCHAR")
            canon = self._canon_sql(ident, dtype, float_precision)
            # concat_ws SKIPS NULL args, so COALESCE to the sentinel happens here, per
            # field, guaranteeing every concat_ws argument is non-NULL.
            fields.append(f"COALESCE({canon}, '{NULL_SENTINEL}')")
        payload = f"concat_ws(chr({_FIELD_SEP_ORD}), {', '.join(fields)})"
        # First 15 hex chars of md5 -> 60-bit unsigned int.
        return f"('0x' || substr(md5({payload}), 1, 15))::UBIGINT"

    # --- predicate builders ----------------------------------------------------

    def _cutoff_predicate(
        self, watermark_column: Optional[str], cutoff: Optional[Any], params: List[Any]
    ) -> Optional[str]:
        """SQL for ``watermark <= cutoff`` (NULL watermark excluded), or None.

        Appends the cutoff value to ``params``. ``IS NOT NULL`` is explicit so the
        semantics match the Memory reference (NULL/unknown watermark rows are dropped).
        """
        if watermark_column is None or cutoff is None:
            return None
        wm = self._quote_ident(watermark_column)
        params.append(cutoff)
        return f"({wm} IS NOT NULL AND {wm} <= ?)"

    def _range_predicate(
        self, pk_cols: Sequence[str], key_range: Optional[KeyRange], params: List[Any]
    ) -> Optional[str]:
        """SQL for the half-open ``[lo, hi)`` key range, or None for unbounded.

        Builds a lexicographic comparison expanded into AND/OR because DuckDB has no
        usable row-value tuple comparison. All bound values are parameter-bound.
        """
        if key_range is None:
            return None
        clauses = []
        if key_range.lo is not None:
            clauses.append(self._lex_compare(pk_cols, key_range.lo, ">=", params))
        if key_range.hi is not None:
            clauses.append(self._lex_compare(pk_cols, key_range.hi, "<", params))
        clauses = [c for c in clauses if c]
        if not clauses:
            return None
        return " AND ".join(clauses)

    def _lex_compare(
        self, pk_cols: Sequence[str], bound: Key, final_op: str, params: List[Any]
    ) -> str:
        """Lexicographic tuple comparison ``(pk_cols) <final_op> bound``.

        ``final_op`` is the operator applied at the deepest (last) column: ``>=`` for the
        inclusive low bound, ``<`` for the exclusive high bound. Earlier columns use
        strict ``>`` / ``<`` with equality fall-through, e.g. for two columns and ``>=``::

            (c1 > ?) OR (c1 = ? AND c2 >= ?)
        """
        cols = list(pk_cols)
        # bound may be a tuple/Key; align by position with cols.
        bound_vals = list(bound)
        if len(bound_vals) != len(cols):
            # Defensive: a shorter bound compares only its provided prefix.
            cols = cols[: len(bound_vals)]
        strict_op = ">" if final_op == ">=" else "<"

        def build(i: int) -> str:
            ident = self._quote_ident(cols[i])
            if i == len(cols) - 1:
                params.append(bound_vals[i])
                return f"{ident} {final_op} ?"
            params.append(bound_vals[i])  # for the strict comparison
            strict = f"{ident} {strict_op} ?"
            params.append(bound_vals[i])  # for the equality check
            eq = f"{ident} = ?"
            inner = build(i + 1)
            return f"({strict} OR ({eq} AND {inner}))"

        if not cols:
            return ""
        return build(0)

    def _where(self, *predicates: Optional[str]) -> str:
        """Combine optional predicates into a WHERE clause (or empty string)."""
        parts = [p for p in predicates if p]
        if not parts:
            return ""
        return " WHERE " + " AND ".join(parts)

    # --- Connector interface ---------------------------------------------------

    def columns(self, table: str) -> List[str]:
        return sorted(self._column_types(table).keys())

    def pk_bounds(
        self,
        table: str,
        pk_cols: Sequence[str],
        watermark_column: Optional[str],
        cutoff: Optional[Any],
    ) -> Optional[KeyRange]:
        qtable = self._quote_table(table)
        idents = [self._quote_ident(c) for c in pk_cols]
        pk_idents = ", ".join(idents)
        params: List[Any] = []
        where = self._where(self._cutoff_predicate(watermark_column, cutoff, params))
        # MIN/MAX over a *tuple* isn't directly expressible; fetch the lexicographically
        # first/last key. The direction must be applied to EVERY key column - a bare
        # "ORDER BY a, b DESC" sorts only b descending, which gives the wrong max for a
        # composite key. Build per-column "<col> ASC|DESC".
        order_asc = ", ".join(f"{i} ASC" for i in idents)
        order_desc = ", ".join(f"{i} DESC" for i in idents)
        asc = self._con.execute(
            f"SELECT {pk_idents} FROM {qtable}{where} ORDER BY {order_asc} LIMIT 1",
            params,
        ).fetchone()
        if asc is None:
            return None
        desc = self._con.execute(
            f"SELECT {pk_idents} FROM {qtable}{where} ORDER BY {order_desc} LIMIT 1",
            list(params),
        ).fetchone()
        return KeyRange(lo=tuple(asc), hi=tuple(desc))  # inclusive bounds

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
        qtable = self._quote_table(table)
        col_types = self._column_types(table)
        row_hash_sql = self._row_hash_sql(col_types, pk_cols, compare_cols, float_precision)
        params: List[Any] = []
        where = self._where(
            self._range_predicate(pk_cols, key_range, params),
            self._cutoff_predicate(watermark_column, cutoff, params),
        )
        # SUM(UBIGINT) promotes to HUGEINT (128-bit) so it can't overflow; apply the
        # contract modulo in SQL. COUNT and SUM in one pass.
        sql = (
            f"SELECT COUNT(*), "
            f"COALESCE(SUM(({row_hash_sql})::HUGEINT) % {CHECKSUM_MOD}, 0) "
            f"FROM {qtable}{where}"
        )
        count, checksum = self._con.execute(sql, params).fetchone()
        # Empty segment: Python's segment_checksum([]) == 0.
        return Checksum(count=int(count), checksum=int(checksum or 0))

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
        qtable = self._quote_table(table)
        col_types = self._column_types(table)
        row_hash_sql = self._row_hash_sql(col_types, pk_cols, compare_cols, float_precision)
        pk_idents = ", ".join(self._quote_ident(c) for c in pk_cols)
        params: List[Any] = []
        where = self._where(
            self._range_predicate(pk_cols, key_range, params),
            self._cutoff_predicate(watermark_column, cutoff, params),
        )
        sql = f"SELECT {pk_idents}, {row_hash_sql} FROM {qtable}{where}"
        n_pk = len(list(pk_cols))
        out: Dict[Key, int] = {}
        for row in self._con.execute(sql, params).fetchall():
            key = tuple(row[:n_pk])
            out[key] = int(row[n_pk])
        return out

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
        if not keys:
            return {}
        qtable = self._quote_table(table)
        col_types = self._column_types(table)
        row_hash_sql = self._row_hash_sql(col_types, pk_cols, compare_cols, float_precision)
        pk_list = list(pk_cols)
        pk_idents = ", ".join(self._quote_ident(c) for c in pk_list)
        params: List[Any] = []

        # Build an OR of per-key equality predicates: (c1=? AND c2=?) OR (...).
        key_clauses = []
        for key in keys:
            kv = list(key)
            terms = []
            for col, val in zip(pk_list, kv):
                terms.append(f"{self._quote_ident(col)} = ?")
                params.append(val)
            key_clauses.append("(" + " AND ".join(terms) + ")")
        keys_predicate = "(" + " OR ".join(key_clauses) + ")"

        where = self._where(
            keys_predicate,
            self._cutoff_predicate(watermark_column, cutoff, params),
        )
        sql = f"SELECT {pk_idents}, {row_hash_sql} FROM {qtable}{where}"
        wanted = set(tuple(k) for k in keys)
        n_pk = len(pk_list)
        out: Dict[Key, int] = {}
        for row in self._con.execute(sql, params).fetchall():
            key = tuple(row[:n_pk])
            if key in wanted:
                out[key] = int(row[n_pk])
        return out

    # --- pruning / lag helpers -------------------------------------------------

    def split_points(
        self,
        table: str,
        pk_cols: Sequence[str],
        key_range: KeyRange,
        watermark_column: Optional[str],
        cutoff: Optional[Any],
        n: int,
    ) -> Optional[List[Key]]:
        """Up to ``n - 1`` interior boundary keys splitting ``key_range`` into ~``n``
        equal-count buckets.

        Strategy (works identically for single-column int/text and composite keys):
        number the watermark-filtered, in-range rows with
        ``ROW_NUMBER() OVER (ORDER BY pk...)`` (1-based ``rn``) alongside
        ``COUNT(*) OVER ()`` (``total``), then pick the rows whose ``rn`` lands on
        ``round(total * i / n)`` for ``i`` in ``1..n-1``. Each picked row's full pk
        tuple is a boundary. The engine forms half-open sub-ranges
        ``[lo, b1), [b1, b2), ..., [bk, hi)`` from them, so boundaries must be strictly
        increasing and strictly inside ``(lo, hi)``: we de-dup, drop any boundary equal
        to the segment's low key (its first row), and return None if there are <= 1 rows
        or no valid interior boundary survives.
        """
        if n is None or n < 2:
            return None
        qtable = self._quote_table(table)
        pk_list = list(pk_cols)
        if not pk_list:
            return None
        idents = [self._quote_ident(c) for c in pk_list]
        pk_idents = ", ".join(idents)
        order_by = ", ".join(f"{i} ASC" for i in idents)

        params: List[Any] = []
        where = self._where(
            self._range_predicate(pk_list, key_range, params),
            self._cutoff_predicate(watermark_column, cutoff, params),
        )
        # Window-numbered selection of every in-range row, ordered by the full key.
        # ROW_NUMBER gives the 1-based position; COUNT(*) OVER () repeats the segment
        # total on every row so the outer query can pick target positions without a
        # second scan.
        numbered = (
            f"SELECT {pk_idents}, "
            f"ROW_NUMBER() OVER (ORDER BY {order_by}) AS __rn, "
            f"COUNT(*) OVER () AS __total "
            f"FROM {qtable}{where}"
        )
        # Target ranks: round(total * i / n) for i in 1..n-1. Compute in SQL so it stays
        # correct for any total. DuckDB's ROUND() is round-half-up, matching the spec's
        # round(total*i/n); positions are integers so ties are vanishingly rare and either
        # choice yields a valid split. We also require the picked row to NOT be the first
        # row (__rn > 1) so a boundary can never equal the low key.
        offsets = ", ".join(str(i) for i in range(1, n))
        sql = (
            f"WITH numbered AS ({numbered}) "
            f"SELECT DISTINCT {pk_idents} FROM numbered "
            f"WHERE __total > 1 AND __rn > 1 AND __rn IN ("
            f"  SELECT CAST(ROUND(__total * i / {n}.0) AS BIGINT) "
            f"  FROM numbered, (SELECT unnest([{offsets}]) AS i) "
            f") ORDER BY {order_by}"
        )
        rows = self._con.execute(sql, params).fetchall()
        if not rows:
            return None
        n_pk = len(pk_list)
        bounds: List[Key] = []
        for row in rows:
            key = tuple(row[:n_pk])
            # DISTINCT + ORDER BY already make these unique and increasing; the guard is
            # belt-and-suspenders for any backend quirk.
            if not bounds or key > bounds[-1]:
                bounds.append(key)
        return bounds or None

    def keys_above_watermark(
        self,
        table: str,
        pk_cols: Sequence[str],
        key_range: KeyRange,
        watermark_column: Optional[str],
        cutoff: Optional[Any],
    ) -> List[Key]:
        """Keys in ``key_range`` whose watermark is STRICTLY greater than ``cutoff``.

        These are the in-flight rows (too fresh to have synced); the engine drops them
        from the target side. Returns [] when there is no watermark column or no cutoff,
        mirroring the Memory reference. NULL watermarks are excluded (``> ?`` is never
        true for NULL).
        """
        if watermark_column is None or cutoff is None:
            return []
        qtable = self._quote_table(table)
        pk_list = list(pk_cols)
        pk_idents = ", ".join(self._quote_ident(c) for c in pk_list)
        wm = self._quote_ident(watermark_column)

        params: List[Any] = []
        range_pred = self._range_predicate(pk_list, key_range, params)
        params.append(cutoff)
        above_pred = f"({wm} IS NOT NULL AND {wm} > ?)"
        where = self._where(range_pred, above_pred)
        sql = f"SELECT {pk_idents} FROM {qtable}{where}"
        return [tuple(row) for row in self._con.execute(sql, params).fetchall()]

    def close(self) -> None:
        try:
            self._con.close()
        except Exception:  # pragma: no cover - best-effort cleanup
            pass
