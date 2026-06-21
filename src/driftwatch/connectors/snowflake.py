"""Snowflake connector - the derived/warehouse side of a comparison.

Translates the dialect-free :class:`~driftwatch.connector.Connector` surface into
native Snowflake SQL, reproducing the :mod:`driftwatch.hashing` contract entirely
in-engine so digests match the Python reference (and the Postgres / DuckDB sides).

Strictly **read-only**: every statement is a ``SELECT``. No DDL/DML is ever issued.

Snowflake identifier folding
----------------------------
Unquoted identifiers fold to UPPERCASE in Snowflake. ``columns()`` therefore returns
lowercased names (matching the engine's lowercased ``compare_columns: "*"`` resolution),
and every column reference in generated SQL is emitted as a double-quoted *uppercased*
identifier. That makes column resolution deterministic regardless of how the table was
created - as long as the table itself was created with default (unquoted/uppercase)
identifiers, which is the overwhelmingly common case. Mixed-case quoted DDL is a known
limitation, documented at ``columns()``.

The hex->int step of the contract
---------------------------------
The Python contract is ``int(md5_hex[:15], 16)`` - the integer value of the first 15
hex characters of the MD5 digest (60 bits). Snowflake reproduces this *exactly* with
``TO_DECIMAL(LEFT(MD5_HEX(payload), 15), 'XXXXXXXXXXXXXXX')``: the ``'X'`` format model
parses a hex string into a fixed-point ``NUMBER``. 15 hex nibbles max out at
``0xFFFFFFFFFFFFFFF`` = 1152921504606846975 (19 decimal digits), well within
``NUMBER(38,0)``, so there is no overflow and no precision loss. This is a true,
bit-for-bit match of the contract - *no* contract adjustment is required for the hash.
(See module-level notes / the task report for the one genuine best-effort edge: floats.)
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional, Sequence

from ..connector import Connector
from ..hashing import CHECKSUM_MOD, FIELD_SEP, HASH_HEX_CHARS, NULL_SENTINEL
from ..models import Checksum, Key, KeyRange

# Snowflake CHR() code point for the field separator (ASCII Unit Separator, 0x1F).
_SEP_CODE = ord(FIELD_SEP)  # 31

# Format model with exactly HASH_HEX_CHARS 'X' elements, e.g. 'XXXXXXXXXXXXXXX' for 15.
_HEX_FORMAT = "X" * HASH_HEX_CHARS


class SnowflakeConnector(Connector):
    """Read-only :class:`Connector` over a Snowflake table."""

    driver = "snowflake"

    def __init__(
        self,
        account: Optional[str] = None,
        user: Optional[str] = None,
        password: Optional[str] = None,
        warehouse: Optional[str] = None,
        database: Optional[str] = None,
        schema: Optional[str] = None,
        role: Optional[str] = None,
        **kwargs: Any,
    ) -> None:
        import snowflake.connector  # imported lazily so the core has no hard dep

        params: Dict[str, Any] = dict(kwargs)
        # Only forward explicitly-provided connection params; let the driver apply its
        # own defaults / connections.toml resolution for anything left as None.
        for name, value in (
            ("account", account),
            ("user", user),
            ("password", password),
            ("warehouse", warehouse),
            ("database", database),
            ("schema", schema),
            ("role", role),
        ):
            if value is not None:
                params[name] = value

        self._conn = snowflake.connector.connect(**params)
        # Keep all bound timestamps interpreted as UTC and avoid any session-format
        # surprises affecting our explicit TO_CHAR masks. Read-only session settings.
        self._database = database
        self._schema = schema

    # --- identifier / SQL helpers ---------------------------------------------

    @staticmethod
    def _split_table(table: str) -> List[str]:
        """Split a possibly schema/db-qualified table name into its identifier parts.

        ``"t"`` -> ``["t"]``; ``"sch.t"`` -> ``["sch", "t"]``; ``"db.sch.t"`` -> 3 parts.
        Quoting in the input (``"Sch"."T"``) is tolerated by stripping surrounding quotes
        and *preserving* the inner case for those parts.
        """
        parts: List[str] = []
        for raw in table.split("."):
            raw = raw.strip()
            if len(raw) >= 2 and raw[0] == '"' and raw[-1] == '"':
                parts.append(raw[1:-1])  # explicitly quoted: keep case verbatim
            else:
                parts.append(raw)
        return parts

    @classmethod
    def _qualified_table_sql(cls, table: str) -> str:
        """Render a (possibly qualified) table name as quoted, upper-cased identifiers.

        Unquoted parts are upper-cased (Snowflake's own folding); already-quoted parts
        keep their verbatim case. Each part is emitted double-quoted so reserved words
        and the qualification dots are handled safely.
        """
        out = []
        for raw in table.split("."):
            raw = raw.strip()
            if len(raw) >= 2 and raw[0] == '"' and raw[-1] == '"':
                inner = raw[1:-1].replace('"', '""')
                out.append('"' + inner + '"')
            else:
                out.append('"' + raw.upper().replace('"', '""') + '"')
        return ".".join(out)

    @staticmethod
    def _quote_col(name: str) -> str:
        """A column reference. ``name`` arrives lowercased (engine convention); emit it
        as a quoted UPPERCASE identifier to match Snowflake's default folding."""
        return '"' + name.upper().replace('"', '""') + '"'

    # --- canonicalization in SQL ----------------------------------------------

    def _canonical_sql(self, col: str, float_precision: int) -> str:
        """SQL expression producing ``canonical(col)`` for one column.

        We do not know the static type of ``col`` here, so we lean on Snowflake's
        runtime type dispatch where possible. The contract's per-type rules map to:

          * NULL            -> COALESCE(..., '\\N')        (NULL_SENTINEL)
          * BOOLEAN         -> '1' / '0'
          * INTEGER         -> base-10 via TO_VARCHAR
          * fixed NUMBER    -> trailing-zeros trimmed
          * FLOAT/DOUBLE    -> %.<p>g  (BEST EFFORT - see report)
          * TIMESTAMP*      -> UTC 'YYYY-MM-DD HH24:MI:SS.FF6'
          * DATE            -> 'YYYY-MM-DD'
          * BINARY          -> lowercase hex
          * TEXT/other      -> as-is

        Because a single column has exactly one Snowflake type, the engine resolves the
        relevant branch at compile time; the multi-branch CASE below is written so that
        only the branch matching the column's actual type is ever taken. ``TYPEOF`` is
        used on the value to dispatch safely without raising on the non-matching casts.
        """
        c = self._quote_col(col)
        ref = c

        # Per-type renderings. Each must equal the Python `canonical()` output.
        bool_sql = "(CASE WHEN {r} THEN '1' ELSE '0' END)".format(r=ref)

        # Integers: base-10, no thousands separators, no decimal point.
        int_sql = "TO_VARCHAR({r})".format(r=ref)

        # Fixed-point NUMBER(p,s): trailing zeros trimmed, no trailing '.'.
        # TO_VARCHAR on a NUMBER yields plain decimal; strip trailing zeros then a bare
        # trailing dot. Integral decimals (e.g. 100) keep no fractional part.
        num_sql = (
            "(CASE WHEN CONTAINS(TO_VARCHAR({r}), '.') "
            "THEN RTRIM(RTRIM(TO_VARCHAR({r}), '0'), '.') "
            "ELSE TO_VARCHAR({r}) END)"
        ).format(r=ref)

        # Floats: best-effort %.<p>g. Snowflake has no exact %g; see _float_sql.
        float_sql = self._float_sql(ref, float_precision)

        # Timestamps -> normalize to UTC wall clock, microsecond precision, space sep.
        ts_sql = (
            "TO_CHAR(CONVERT_TIMEZONE('UTC', {r}), 'YYYY-MM-DD HH24:MI:SS.FF6')"
        ).format(r=ref)

        # Dates -> ISO date.
        date_sql = "TO_CHAR({r}, 'YYYY-MM-DD')".format(r=ref)

        # Binary -> lowercase hex (HEX_ENCODE(x, 0) = lowercase).
        bin_sql = "HEX_ENCODE({r}, 0)".format(r=ref)

        # Text / fallback -> as-is string form.
        text_sql = "TO_VARCHAR({r})".format(r=ref)

        # Dispatch on the runtime type so every column resolves to exactly one rule.
        # TYPEOF returns the data-type family name for a value (e.g. 'BOOLEAN',
        # 'INTEGER', 'DECIMAL', 'DOUBLE', 'TIMESTAMP_NTZ', 'DATE', 'BINARY', 'TEXT').
        inner = (
            "CASE TYPEOF({r}) "
            "WHEN 'BOOLEAN' THEN {b} "
            "WHEN 'INTEGER' THEN {i} "
            "WHEN 'DECIMAL' THEN {n} "
            "WHEN 'DOUBLE' THEN {f} "
            "WHEN 'REAL' THEN {f} "
            "WHEN 'FLOAT' THEN {f} "
            "WHEN 'DATE' THEN {d} "
            "WHEN 'TIME' THEN {txt} "
            "WHEN 'TIMESTAMP_NTZ' THEN {ts} "
            "WHEN 'TIMESTAMP_LTZ' THEN {ts} "
            "WHEN 'TIMESTAMP_TZ' THEN {ts} "
            "WHEN 'BINARY' THEN {bin} "
            "ELSE {txt} END"
        ).format(
            r=ref, b=bool_sql, i=int_sql, n=num_sql, f=float_sql,
            d=date_sql, ts=ts_sql, bin=bin_sql, txt=text_sql,
        )
        # NULL handling wraps the whole thing: a NULL value -> NULL_SENTINEL.
        return "COALESCE({inner}, '{sentinel}')".format(
            inner=inner, sentinel=NULL_SENTINEL.replace("'", "''")
        )

    @staticmethod
    def _float_sql(ref: str, float_precision: int) -> str:
        """Best-effort reproduction of Python ``format(x, '.<p>g')`` for a FLOAT column.

        Python's ``%g`` keeps ``float_precision`` *significant* digits, strips trailing
        zeros, and switches to ``e`` notation outside roughly ``[1e-4, 1e<p>)``. Snowflake
        has no direct ``%g``. We approximate with ``TO_CHAR(x, 'TM9')`` which produces a
        minimal-width decimal (no trailing zeros). This agrees with the contract for the
        common, exactly-representable magnitudes but is NOT guaranteed bit-identical for
        every double (rounding at the precision boundary and the decimal<->scientific
        cutover differ from CPython's ``repr``/``%g``). Floats are a documented sharp edge
        of the contract; comparisons on float columns are best-effort across engines.
        """
        # TM9: text-minimal, trims trailing zeros, no spaces. Closest single-function
        # analogue to %g available without a UDF.
        return "TO_CHAR({r}, 'TM9')".format(r=ref)

    def _row_hash_sql(self, pk_cols: Sequence[str], compare_cols: Sequence[str], fp: int) -> str:
        """SQL expression for the 60-bit row hash of one row.

        payload = CHR(31)-joined canonical fields, in engine order (pk then compare).
        hash    = integer value of first HASH_HEX_CHARS hex chars of MD5_HEX(payload).
        """
        fields = [self._canonical_sql(c, fp) for c in list(pk_cols) + list(compare_cols)]
        # CONCAT_WS(sep, ...) joins with the separator between fields. None of our field
        # expressions can be NULL (COALESCE guarantees a string), so CONCAT_WS does not
        # skip any field, preserving exact FIELD_SEP.join semantics.
        payload = "CONCAT_WS(CHR({sep}), {fields})".format(
            sep=_SEP_CODE, fields=", ".join(fields)
        )
        return "TO_DECIMAL(LEFT(MD5_HEX({payload}), {n}), '{fmt}')".format(
            payload=payload, n=HASH_HEX_CHARS, fmt=_HEX_FORMAT
        )

    # --- predicate builders ----------------------------------------------------

    def _cutoff_predicate(
        self, watermark_column: Optional[str], cutoff: Optional[Any], binds: List[Any]
    ) -> Optional[str]:
        """`watermark <= %s` with NULL watermark excluded; appends the bind value."""
        if watermark_column is None or cutoff is None:
            return None
        wm = self._quote_col(watermark_column)
        binds.append(cutoff)
        # `wm <= %s` already excludes NULL watermark (NULL <= x is unknown -> filtered),
        # but we make the exclusion explicit for clarity and parity with the reference.
        return "({wm} IS NOT NULL AND {wm} <= %s)".format(wm=wm)

    def _range_predicate(
        self, pk_cols: Sequence[str], key_range: Optional[KeyRange], binds: List[Any]
    ) -> List[str]:
        """Half-open ``[lo, hi)`` over the (possibly composite) key via row-value compare.

        Row-value comparison ``(a, b) >= (?, ?)`` gives the correct lexicographic tuple
        ordering that matches Python tuple comparison used by the MemoryConnector oracle.
        lo is inclusive (``>=``), hi is exclusive (``<``); either may be None (unbounded).
        """
        preds: List[str] = []
        if key_range is None:
            return preds
        cols_sql = ", ".join(self._quote_col(c) for c in pk_cols)
        if key_range.lo is not None:
            placeholders = ", ".join(["%s"] * len(pk_cols))
            preds.append("({cols}) >= ({ph})".format(cols=cols_sql, ph=placeholders))
            binds.extend(key_range.lo)
        if key_range.hi is not None:
            placeholders = ", ".join(["%s"] * len(pk_cols))
            preds.append("({cols}) < ({ph})".format(cols=cols_sql, ph=placeholders))
            binds.extend(key_range.hi)
        return preds

    def _where(
        self,
        pk_cols: Sequence[str],
        key_range: Optional[KeyRange],
        watermark_column: Optional[str],
        cutoff: Optional[Any],
        binds: List[Any],
    ) -> str:
        preds: List[str] = []
        preds.extend(self._range_predicate(pk_cols, key_range, binds))
        cp = self._cutoff_predicate(watermark_column, cutoff, binds)
        if cp is not None:
            preds.append(cp)
        return (" WHERE " + " AND ".join(preds)) if preds else ""

    # --- execution -------------------------------------------------------------

    def _query(self, sql: str, binds: Sequence[Any]) -> List[tuple]:
        cur = self._conn.cursor()
        try:
            cur.execute(sql, tuple(binds) if binds else None)
            return cur.fetchall()
        finally:
            cur.close()

    # --- Connector interface ---------------------------------------------------

    def columns(self, table: str) -> List[str]:
        """Column names, lowercased and sorted.

        Resolved from ``INFORMATION_SCHEMA.COLUMNS`` so we get the real schema (not just
        columns present in sampled rows). Names are lowercased to match the engine's
        ``compare_columns: "*"`` resolution. Snowflake stores unquoted-DDL identifiers
        upper-cased; lowercasing them yields the canonical form. NOTE: tables created with
        case-sensitive *quoted* mixed-case identifiers are a documented limitation - their
        names would still be lowercased here and may not round-trip to the same physical
        column when re-quoted-upper in SQL.
        """
        parts = self._split_table(table)
        if len(parts) == 1:
            tbl = parts[0]
            sch = self._schema
            db = self._database
        elif len(parts) == 2:
            sch, tbl = parts
            db = self._database
        else:
            db, sch, tbl = parts[-3], parts[-2], parts[-1]

        # Identifiers in INFORMATION_SCHEMA are stored upper-cased for unquoted DDL.
        tbl_u = tbl.upper()
        binds: List[Any] = [tbl_u]
        where = "TABLE_NAME = %s"
        if sch is not None:
            where += " AND TABLE_SCHEMA = %s"
            binds.append(sch.upper())
        # Prefer the database-scoped INFORMATION_SCHEMA when we know the database.
        info_schema = (
            '"{db}".INFORMATION_SCHEMA.COLUMNS'.format(db=db.upper())
            if db is not None
            else "INFORMATION_SCHEMA.COLUMNS"
        )
        sql = (
            "SELECT COLUMN_NAME FROM {isc} WHERE {where} ORDER BY ORDINAL_POSITION"
        ).format(isc=info_schema, where=where)
        rows = self._query(sql, binds)
        return sorted({str(r[0]).lower() for r in rows})

    def pk_bounds(self, table, pk_cols, watermark_column, cutoff) -> Optional[KeyRange]:
        """Inclusive [min, max] of the primary key within cutoff, or None if empty.

        Returns the true *lexicographic* (row-value) min and max key tuples, matching the
        reference oracle's ``min(keys)`` / ``max(keys)`` (Python tuple comparison) for both
        single and composite keys. A per-column MIN/MAX box would be wrong for composite
        keys: e.g. for ``{(1,9),(2,1)}`` the lexicographic max is ``(2,1)`` but the box
        max is ``(2,9)``, which would mis-seed the engine's half-open segmenter. We get the
        exact tuples with two ``ORDER BY``-ed single-row reads, which Snowflake serves from
        sort/limit cheaply.
        """
        order_cols = ", ".join(self._quote_col(c) for c in pk_cols)
        select_cols = order_cols  # same list, in key order

        def _one(direction: str) -> Optional[tuple]:
            binds: List[Any] = []
            where = self._where(pk_cols, None, watermark_column, cutoff, binds)
            sql = "SELECT {cols} FROM {tbl}{where} ORDER BY {order} {dir} LIMIT 1".format(
                cols=select_cols,
                tbl=self._qualified_table_sql(table),
                where=where,
                order=order_cols,
                dir=direction,
            )
            rows = self._query(sql, binds)
            return rows[0] if rows else None

        lo_row = _one("ASC")
        if lo_row is None:
            return None  # empty within cutoff
        hi_row = _one("DESC")
        lo = tuple(lo_row)
        hi = tuple(hi_row) if hi_row is not None else lo
        return KeyRange(lo=lo, hi=hi)

    def checksum(
        self, table, pk_cols, compare_cols, key_range, watermark_column, cutoff, float_precision
    ) -> Checksum:
        """Native (count, SUM(row_hash) mod 2**63) over the selected rows."""
        binds: List[Any] = []
        where = self._where(pk_cols, key_range, watermark_column, cutoff, binds)
        rh = self._row_hash_sql(pk_cols, compare_cols, float_precision)
        # SUM over NUMBER(38,0) holds the running total without overflow; apply the
        # contract modulus in-engine. MOD on a non-negative sum yields a non-negative
        # remainder, matching Python's `% CHECKSUM_MOD` for non-negative operands.
        sql = (
            "SELECT COUNT(*), MOD(COALESCE(SUM({rh}), 0), {mod}) FROM {tbl}{where}"
        ).format(rh=rh, mod=CHECKSUM_MOD, tbl=self._qualified_table_sql(table), where=where)
        rows = self._query(sql, binds)
        if not rows:
            return Checksum(count=0, checksum=0)
        count, chk = rows[0]
        return Checksum(count=int(count or 0), checksum=int(chk or 0))

    def fetch_row_hashes(
        self, table, pk_cols, compare_cols, key_range, watermark_column, cutoff, float_precision
    ) -> Dict[Key, int]:
        """{key_tuple: row_hash} for every selected row."""
        binds: List[Any] = []
        where = self._where(pk_cols, key_range, watermark_column, cutoff, binds)
        return self._fetch_pairs(table, pk_cols, compare_cols, where, binds, float_precision)

    def fetch_row_hashes_for_keys(
        self, table, pk_cols, compare_cols, keys, watermark_column, cutoff, float_precision
    ) -> Dict[Key, int]:
        """Like ``fetch_row_hashes`` but restricted to an explicit set of keys.

        ``cutoff`` may be None here (recheck reads the freshest data). Empty ``keys`` is a
        fast no-op. Keys are matched with an ``IN`` over the row-value of the PK columns.
        """
        keys = list(keys)
        if not keys:
            return {}
        binds: List[Any] = []
        preds: List[str] = []
        cols_sql = ", ".join(self._quote_col(c) for c in pk_cols)
        placeholder_tuple = "(" + ", ".join(["%s"] * len(pk_cols)) + ")"
        in_list = ", ".join([placeholder_tuple] * len(keys))
        preds.append("({cols}) IN ({vals})".format(cols=cols_sql, vals=in_list))
        for k in keys:
            binds.extend(k)
        cp = self._cutoff_predicate(watermark_column, cutoff, binds)
        if cp is not None:
            preds.append(cp)
        where = " WHERE " + " AND ".join(preds)
        return self._fetch_pairs(table, pk_cols, compare_cols, where, binds, float_precision)

    def _fetch_pairs(
        self,
        table: str,
        pk_cols: Sequence[str],
        compare_cols: Sequence[str],
        where: str,
        binds: List[Any],
        float_precision: int,
    ) -> Dict[Key, int]:
        rh = self._row_hash_sql(pk_cols, compare_cols, float_precision)
        pk_sql = ", ".join(self._quote_col(c) for c in pk_cols)
        sql = "SELECT {pk}, {rh} FROM {tbl}{where}".format(
            pk=pk_sql, rh=rh, tbl=self._qualified_table_sql(table), where=where
        )
        rows = self._query(sql, binds)
        n = len(list(pk_cols))
        out: Dict[Key, int] = {}
        for row in rows:
            key = tuple(row[:n])
            out[key] = int(row[n])
        return out

    # --- optional pruning / lag helpers ---------------------------------------

    def split_points(self, table, pk_cols, key_range, watermark_column, cutoff, n):
        """Up to ``n - 1`` boundary keys splitting the watermark-filtered rows of
        ``key_range`` into ~``n`` equal-count buckets.

        Matches the :class:`MemoryConnector` oracle exactly:

          * order the selected rows by the full PK tuple (row-value / lexicographic
            order, identical to ``_range_predicate``'s ``(a, b) >= (?, ?)`` semantics
            and to Python tuple comparison),
          * take the key at 0-based position ``(total * i) // n`` for ``i`` in
            ``1 .. n - 1`` (floor division - the same integer arithmetic the reference
            uses, NOT ``round``),
          * keep boundaries strictly inside ``(lo, hi)`` by dropping any that equals the
            first selected key, and de-duplicate while keeping them strictly increasing.

        Returns ``None`` when the range holds <= 1 row (or ``n < 2``) so the engine
        treats the range as a leaf.

        SQL strategy: a single pass with window functions over the filtered rows -
        ``ROW_NUMBER() OVER (ORDER BY <pk...>)`` for the position and ``COUNT(*) OVER ()``
        for the total - then a row-number filter computed entirely in SQL so only the
        boundary rows (at most ``n - 1`` of them) cross the wire::

            SELECT <pk...>
            FROM (
              SELECT <pk...>,
                     ROW_NUMBER() OVER (ORDER BY <pk...>) AS rn,
                     COUNT(*) OVER () AS total
              FROM <table><where>
            )
            WHERE total > 1
              AND rn - 1 IN (FLOOR(total * 1 / n), ..., FLOOR(total * (n-1) / n))
              AND rn - 1 > 0            -- strictly after the first (low-bound) key
            ORDER BY <pk...>

        ``ROW_NUMBER`` is 1-based, so ``rn - 1`` is the 0-based index the reference
        indexes with. ``FLOOR(total * i / n)`` reproduces Python's ``(total * i) // n``
        for the non-negative integers involved. The de-dup of equal indices and the
        ``> 0`` guard are done in SQL; Python only assembles tuples and enforces strict
        monotonicity as a final belt-and-braces pass.
        """
        if n < 2:
            return None

        order_cols = ", ".join(self._quote_col(c) for c in pk_cols)
        select_cols = order_cols  # same list, in key order

        binds: List[Any] = []
        where = self._where(pk_cols, key_range, watermark_column, cutoff, binds)

        # Distinct 0-based target indices (drop the 0 index: that is the low-bound key,
        # which must be strictly excluded). FLOOR(total*i/n) is expressed in SQL so the
        # arithmetic happens against the *actual* row count, exactly like the reference.
        idx_terms = ", ".join(
            "FLOOR(total * {i} / {n})".format(i=i, n=n) for i in range(1, n)
        )

        sql = (
            "SELECT {cols} FROM ("
            "SELECT {cols}, "
            "ROW_NUMBER() OVER (ORDER BY {order}) AS rn, "
            "COUNT(*) OVER () AS total "
            "FROM {tbl}{where}"
            ") "
            "WHERE total > 1 AND (rn - 1) > 0 AND (rn - 1) IN ({idx}) "
            "ORDER BY {order}"
        ).format(
            cols=select_cols,
            order=order_cols,
            tbl=self._qualified_table_sql(table),
            where=where,
            idx=idx_terms,
        )

        rows = self._query(sql, binds)
        if not rows:
            return None

        bounds: List[Key] = []
        for row in rows:
            b = tuple(row)
            # SQL already guarantees ordering + distinct indices, but enforce strict
            # monotonicity here too so a degenerate engine/driver can't slip a dup or a
            # boundary equal to the previous one through.
            if not bounds or b > bounds[-1]:
                bounds.append(b)
        return bounds or None

    def keys_above_watermark(self, table, pk_cols, key_range, watermark_column, cutoff):
        """PK tuples in ``key_range`` whose watermark is STRICTLY greater than ``cutoff``.

        These are the in-flight rows (updated after the cutoff, not yet propagated). The
        engine drops them from the target side so a fresh-but-unsynced update is not
        misreported as drift.

        Returns ``[]`` when there is no watermark column or no cutoff (nothing can be
        "above" an absent cutoff), mirroring the reference. The watermark predicate is
        ``wm IS NOT NULL AND wm > %s`` (the strict complement of ``_cutoff_predicate``'s
        ``wm <= %s``); ``cutoff`` is bound as a parameter using the same paramstyle.
        """
        if watermark_column is None or cutoff is None:
            return []

        binds: List[Any] = []
        preds: List[str] = []
        preds.extend(self._range_predicate(pk_cols, key_range, binds))
        wm = self._quote_col(watermark_column)
        binds.append(cutoff)
        preds.append("({wm} IS NOT NULL AND {wm} > %s)".format(wm=wm))

        where = (" WHERE " + " AND ".join(preds)) if preds else ""
        pk_sql = ", ".join(self._quote_col(c) for c in pk_cols)
        sql = "SELECT {pk} FROM {tbl}{where}".format(
            pk=pk_sql, tbl=self._qualified_table_sql(table), where=where
        )
        rows = self._query(sql, binds)
        return [tuple(row) for row in rows]

    def close(self) -> None:
        try:
            self._conn.close()
        except Exception:  # pragma: no cover - best-effort cleanup
            pass
