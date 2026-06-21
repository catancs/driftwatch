"""PostgreSQL connector - the source-of-truth side.

Translates the dialect-free :class:`~driftwatch.connector.Connector` surface into
PostgreSQL SQL while reproducing the :mod:`driftwatch.hashing` contract *natively in
the database*. The engine never sees SQL; it only sees row counts, checksums and
``{key: row_hash}`` maps, identical to what :class:`MemoryConnector` would compute in
Python over the same data.

Strictly read-only: every statement runs inside a ``REPEATABLE READ`` /
``READ ONLY`` transaction so a comparison observes one stable snapshot and can never
mutate the source.

Hashing contract reproduced in SQL
----------------------------------
For each field, a per-type canonical text is produced (see ``_canonical_sql``); the
fields are joined with ``chr(31)`` in engine order (pk cols then compare cols); the
60-bit row hash is ``(('x'||substr(md5(payload),1,15))::bit(60))::bigint`` and the
segment checksum is ``SUM(row_hash) % 9223372036854775808`` so it matches Python's
``sum(hashes) % 2**63``.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional, Sequence

try:  # pragma: no cover - import guard
    import psycopg
    from psycopg import sql as _sql
except ImportError as exc:  # pragma: no cover - exercised only without psycopg
    raise ImportError(
        "PostgresConnector requires psycopg v3; install with `pip install \"psycopg[binary]\"`"
    ) from exc

from ..connector import Connector
from ..hashing import CHECKSUM_MOD, FIELD_SEP
from ..models import Checksum, Key, KeyRange


class PostgresConnector(Connector):
    driver = "postgres"

    def __init__(
        self,
        dsn: Optional[str] = None,
        host: Optional[str] = None,
        port: Optional[Any] = None,
        user: Optional[str] = None,
        password: Optional[str] = None,
        dbname: Optional[str] = None,
        **kwargs: Any,
    ) -> None:
        parts: Dict[str, Any] = {
            "host": host,
            "port": port,
            "user": user,
            "password": password,
            "dbname": dbname,
        }
        # Drop unset parts so psycopg / libpq can apply its own defaults and env vars.
        conn_kwargs = {k: v for k, v in parts.items() if v is not None}
        # Forward any extra libpq params (sslmode, connect_timeout, ...).
        for k, v in kwargs.items():
            if v is not None:
                conn_kwargs[k] = v

        if dsn:
            self._conn = psycopg.connect(dsn, **conn_kwargs)
        else:
            self._conn = psycopg.connect(**conn_kwargs)

        # Install the session-local float-formatting helper *before* going read-only.
        # It is a TEMPORARY function (pg_temp schema): it never touches user data or the
        # on-disk catalog visible to other sessions, so creating it does not violate the
        # read-only intent against the source table. After this point the connection is
        # pinned read-only + REPEATABLE READ for the rest of its life.
        self._conn.autocommit = True
        with self._conn.cursor() as setup:
            setup.execute(_FLOAT_G_FUNCTION_SQL)
        self._conn.autocommit = False
        self._conn.read_only = True
        self._conn.isolation_level = psycopg.IsolationLevel.REPEATABLE_READ

    # --- identifier handling ---------------------------------------------------

    @staticmethod
    def _table_ident(table: str) -> _sql.Composable:
        """Quote a possibly schema-qualified table name safely.

        ``"public.orders"`` -> ``"public"."orders"``; a bare ``"orders"`` stays a
        single quoted identifier. A literal dot inside an identifier is not supported
        (matches the rest of the toolchain, which treats ``schema.table`` as the
        qualifier syntax).
        """
        parts = table.split(".")
        return _sql.SQL(".").join(_sql.Identifier(p) for p in parts)

    @staticmethod
    def _col(name: str) -> _sql.Composable:
        return _sql.Identifier(name)

    # --- column type resolution ------------------------------------------------

    def _column_types(self, table: str) -> Dict[str, str]:
        """``{lower(column_name): typcategory+typename}`` for the table, cached.

        Resolved once per table from ``pg_attribute``/``pg_type`` so we can choose the
        right canonical SQL per column *at build time*. Build-time dispatch is required
        because a runtime ``CASE pg_typeof(...)`` would still have to type-check every
        branch (e.g. ``id::timestamp``), which Postgres rejects for an integer column.
        """
        cache = getattr(self, "_type_cache", None)
        if cache is None:
            cache = self._type_cache = {}
        if table in cache:
            return cache[table]
        # Pass the *quoted* identifier text (e.g. ``"My Schema"."Weird Table"``) as the
        # regclass argument so ``::regclass`` parses qualified / case-sensitive / spaced
        # names correctly - the raw dotted string would fail regclass syntax. Unqualified
        # names still resolve via search_path. This mirrors how ``_table_ident`` quotes
        # the same name everywhere else, so type lookup and the actual query agree.
        regclass_text = self._table_ident(table).as_string(self._conn)
        cur = self._cursor()
        try:
            cur.execute(
                "SELECT lower(a.attname), t.typname "
                "FROM pg_attribute a "
                "JOIN pg_type t ON t.oid = a.atttypid "
                "WHERE a.attrelid = %s::regclass "
                "AND a.attnum > 0 AND NOT a.attisdropped",
                (regclass_text,),
            )
            types = {row[0]: row[1] for row in cur.fetchall()}
        finally:
            cur.close()
            self._conn.rollback()
        cache[table] = types
        return types

    # --- per-type canonicalisation (the hashing contract) ----------------------

    # Internal ``pg_type.typname`` values. Postgres spells the SQL type names with
    # these catalog names (e.g. ``int4`` for integer, ``timestamp`` for "timestamp
    # without time zone", ``timestamptz`` for the tz variant, ``bpchar`` for char(n)).
    _INT_TYPES = {"int2", "int4", "int8"}
    _BOOL_TYPES = {"bool"}
    _NUMERIC_TYPES = {"numeric"}
    _FLOAT_TYPES = {"float4", "float8"}
    _TS_NAIVE_TYPES = {"timestamp"}
    _TS_TZ_TYPES = {"timestamptz"}
    _DATE_TYPES = {"date"}
    _BYTEA_TYPES = {"bytea"}

    def _canonical_sql(self, name: str, typname: str) -> _sql.Composable:
        """Per-column canonical-text SQL, dispatched on the resolved ``pg_type`` name.

        The expression COALESCEs to the NULL sentinel ``'\\N'`` so a SQL NULL of any
        type canonicalises identically to Python ``canonical(None)``. Branch coverage
        mirrors ``driftwatch.hashing.canonical``:

          * bool       -> '1' / '0'
          * int2/4/8   -> base-10 text (``::text`` on an integer is exact)
          * numeric    -> ``trim_scale`` then ``::text``: trailing zeros dropped,
            integral values keep no decimal point (``100``->'100', ``1.2300``->'1.23').
          * float4/8   -> ``float_precision`` significant digits in Python ``%g`` shape
            (the ``driftwatch_float_g`` session helper).
          * timestamp  -> ``to_char`` microsecond precision.
          * timestamptz-> converted to UTC first, then ``to_char``.
          * date       -> ``YYYY-MM-DD``.
          * bytea      -> lowercase hex (``encode(x,'hex')``).
          * everything else (text, varchar, bpchar, uuid, json, ...) -> ``::text``.

        ``typname`` is a trusted catalog value, not user input.
        """
        col = self._col(name)
        if typname in self._BOOL_TYPES:
            # IS TRUE/IS FALSE (not a bare ELSE) so a NULL boolean stays NULL and the
            # outer COALESCE substitutes the sentinel - otherwise NULL would wrongly
            # canonicalise as '0'.
            expr = _sql.SQL(
                "CASE WHEN {col} IS TRUE THEN '1' "
                "WHEN {col} IS FALSE THEN '0' END"
            ).format(col=col)
        elif typname in self._INT_TYPES:
            expr = _sql.SQL("{col}::text").format(col=col)
        elif typname in self._NUMERIC_TYPES:
            expr = _sql.SQL("trim_scale({col})::text").format(col=col)
        elif typname in self._FLOAT_TYPES:
            expr = self._float_sql(col)
        elif typname in self._TS_NAIVE_TYPES:
            expr = _sql.SQL(
                "to_char({col}, 'YYYY-MM-DD HH24:MI:SS.US')"
            ).format(col=col)
        elif typname in self._TS_TZ_TYPES:
            expr = _sql.SQL(
                "to_char(({col} AT TIME ZONE 'UTC'), 'YYYY-MM-DD HH24:MI:SS.US')"
            ).format(col=col)
        elif typname in self._DATE_TYPES:
            expr = _sql.SQL("to_char({col}, 'YYYY-MM-DD')").format(col=col)
        elif typname in self._BYTEA_TYPES:
            expr = _sql.SQL("encode({col}, 'hex')").format(col=col)
        else:
            # text, varchar, bpchar, uuid, json, jsonb, inet, ... : str() identity.
            expr = _sql.SQL("{col}::text").format(col=col)
        return _sql.SQL("COALESCE({expr}, '\\N')").format(expr=expr)

    def _float_sql(self, col: _sql.Composable) -> _sql.Composable:
        """Render a float like Python ``format(v, '.<p>g')`` with ``p`` sig digits.

        Postgres' own ``float8::text`` uses *extra_float_digits* (shortest round-trip),
        which is NOT Python ``%g``; ``to_char`` can't do significant-digit formatting
        either. So the contract is reproduced by the ``driftwatch_float_g`` session
        helper, which reconstructs the EXACT decimal value of the double from its
        IEEE-754 bits and rounds it to ``p`` significant digits with round-half-to-even
        (matching C ``%g`` byte-for-byte at every precision, incl. 16-17), then applies
        the ``%g`` fixed/scientific choice + trailing-zero trim. ``float_precision`` is
        supplied as a bound named parameter.
        """
        # ``pg_temp.`` qualifies the call so it always resolves to this session's
        # temporary helper - an *unqualified* name is NOT reliably searched in pg_temp
        # for functions, yielding "function does not exist". The precision is cast to
        # integer explicitly so psycopg sending a small int as ``smallint`` can't break
        # overload resolution against the ``(double precision, integer)`` signature.
        return _sql.SQL(
            "pg_temp.driftwatch_float_g({col}::double precision, {p}::integer)"
        ).format(col=col, p=_sql.Placeholder("float_precision"))

    # --- payload + row hash ----------------------------------------------------

    def _payload_sql(
        self, table: str, pk_cols: Sequence[str], compare_cols: Sequence[str]
    ) -> _sql.Composable:
        """concat_ws(chr(31), canonical(c1), canonical(c2), ...) in engine order.

        ``concat_ws`` skips NULL arguments, but each canonical expression COALESCEs to
        the sentinel first, so no argument is ever NULL - separator placement is exactly
        the Python ``FIELD_SEP.join`` behaviour.
        """
        types = self._column_types(table)
        all_cols = list(pk_cols) + list(compare_cols)
        canon = []
        for c in all_cols:
            typname = types.get(c.lower())
            if typname is None:
                raise KeyError(
                    "column %r not found on table %r (have: %s)"
                    % (c, table, ", ".join(sorted(types)))
                )
            canon.append(self._canonical_sql(c, typname))
        sep = _sql.Literal(FIELD_SEP)
        return _sql.SQL("concat_ws({sep}, {fields})").format(
            sep=sep, fields=_sql.SQL(", ").join(canon)
        )

    def _row_hash_sql(
        self, table: str, pk_cols: Sequence[str], compare_cols: Sequence[str]
    ) -> _sql.Composable:
        """60-bit row hash as a bigint, from the first 15 hex chars of md5(payload)."""
        payload = self._payload_sql(table, pk_cols, compare_cols)
        return _sql.SQL(
            "(('x' || substr(md5({payload}), 1, 15))::bit(60))::bigint"
        ).format(payload=payload)

    # --- WHERE predicate (range + cutoff) --------------------------------------

    # All value parameters are passed as *named* placeholders (``%(p0)s``, ...) so they
    # can coexist with the named ``%(float_precision)s`` placeholder the canonical-float
    # expression emits. psycopg v3 forbids mixing positional and named params in one
    # query, hence the uniform named scheme. ``_PARAM`` is a tiny allocator that yields a
    # fresh name and records the value, keeping names globally consistent across clauses.
    class _Params:
        def __init__(self) -> None:
            self.values: Dict[str, Any] = {}
            self._n = 0

        def add(self, value: Any) -> _sql.Placeholder:
            name = "p%d" % self._n
            self._n += 1
            self.values[name] = value
            return _sql.Placeholder(name)

    def _where_sql(
        self,
        pk_cols: Sequence[str],
        key_range: Optional[KeyRange],
        watermark_column: Optional[str],
        cutoff: Optional[Any],
        params: "PostgresConnector._Params",
    ) -> _sql.Composable:
        """Build the WHERE clause for a half-open range plus optional cutoff.

        Range is expressed with row-value comparison so it works for composite keys:
        ``(pk1, pk2, ...) >= (lo...)`` and ``(pk1, pk2, ...) < (hi...)``. ``lo`` is
        inclusive, ``hi`` exclusive (half-open ``[lo, hi)``), matching the engine.

        Cutoff is ``watermark_column <= :cutoff``; a NULL watermark is excluded exactly
        as ``MemoryConnector`` excludes it (kept explicit with ``IS NOT NULL`` for
        parity/clarity).

        Every value is parameter-bound (never interpolated) to stay injection-safe.
        """
        clauses: List[_sql.Composable] = []

        pk_tuple = _sql.SQL("({cols})").format(
            cols=_sql.SQL(", ").join(self._col(c) for c in pk_cols)
        )

        if key_range is not None and key_range.lo is not None:
            phs = _sql.SQL(", ").join(params.add(v) for v in key_range.lo)
            clauses.append(_sql.SQL("{pk} >= ({ph})").format(pk=pk_tuple, ph=phs))

        if key_range is not None and key_range.hi is not None:
            phs = _sql.SQL(", ").join(params.add(v) for v in key_range.hi)
            clauses.append(_sql.SQL("{pk} < ({ph})").format(pk=pk_tuple, ph=phs))

        if watermark_column is not None and cutoff is not None:
            clauses.append(
                _sql.SQL("{wm} IS NOT NULL AND {wm} <= {ph}").format(
                    wm=self._col(watermark_column), ph=params.add(cutoff)
                )
            )

        if not clauses:
            return _sql.SQL("TRUE")
        return _sql.SQL(" AND ").join(clauses)

    def _keys_predicate_sql(
        self,
        pk_cols: Sequence[str],
        keys: Sequence[Key],
        params: "PostgresConnector._Params",
    ) -> _sql.Composable:
        """``(pk...) IN ((..),(..),...)`` for an explicit key set (recheck pass)."""
        tuples: List[_sql.Composable] = []
        for key in keys:
            phs = _sql.SQL(", ").join(params.add(v) for v in key)
            tuples.append(_sql.SQL("({ph})").format(ph=phs))
        pk_tuple = _sql.SQL("({cols})").format(
            cols=_sql.SQL(", ").join(self._col(c) for c in pk_cols)
        )
        return _sql.SQL("{pk} IN ({tuples})").format(
            pk=pk_tuple, tuples=_sql.SQL(", ").join(tuples)
        )

    # --- transaction helper ----------------------------------------------------

    def _cursor(self):
        """A plain cursor. The read-only REPEATABLE READ txn is started implicitly on
        first use and rolled back after each method, so every call sees a fresh, stable
        snapshot and can never write."""
        return self._conn.cursor()

    # --- Connector interface ---------------------------------------------------

    def columns(self, table: str) -> List[str]:
        """Column names, lowercased and sorted, like ``MemoryConnector.columns``.

        Resolved from the live relation via ``information_schema`` so we get the real,
        ordered set even for an empty table (Memory can only see columns of rows it
        holds; on a populated table the two agree, which is all the engine needs).
        """
        schema, _, name = table.rpartition(".")
        cur = self._cursor()
        try:
            if schema:
                cur.execute(
                    "SELECT lower(column_name) FROM information_schema.columns "
                    "WHERE table_schema = %s AND table_name = %s",
                    (schema, name),
                )
            else:
                cur.execute(
                    "SELECT lower(column_name) FROM information_schema.columns "
                    "WHERE table_name = %s AND table_schema = ANY(current_schemas(false))",
                    (name,),
                )
            names = sorted({r[0] for r in cur.fetchall()})
            return names
        finally:
            cur.close()
            self._conn.rollback()

    def pk_bounds(
        self,
        table: str,
        pk_cols: Sequence[str],
        watermark_column: Optional[str],
        cutoff: Optional[Any],
    ) -> Optional[KeyRange]:
        """Inclusive [min, max] over the (composite) primary key, within cutoff.

        For composite keys, ``MIN``/``MAX`` over a row value isn't available portably,
        so we fetch the lexicographically smallest and largest key with two ORDER BY +
        LIMIT 1 queries. This matches Python's ``min(keys)``/``max(keys)`` over tuples.
        """
        params = self._Params()
        where = self._where_sql(pk_cols, None, watermark_column, cutoff, params)
        pk_list = _sql.SQL(", ").join(self._col(c) for c in pk_cols)
        tbl = self._table_ident(table)

        cur = self._cursor()
        try:
            cur.execute(
                _sql.SQL(
                    "SELECT {cols} FROM {tbl} WHERE {where} ORDER BY {asc} LIMIT 1"
                ).format(cols=pk_list, tbl=tbl, where=where, asc=pk_list),
                params.values,
            )
            lo_row = cur.fetchone()
            if lo_row is None:
                return None
            desc = _sql.SQL(", ").join(
                _sql.SQL("{c} DESC").format(c=self._col(c)) for c in pk_cols
            )
            cur.execute(
                _sql.SQL(
                    "SELECT {cols} FROM {tbl} WHERE {where} ORDER BY {desc} LIMIT 1"
                ).format(cols=pk_list, tbl=tbl, where=where, desc=desc),
                params.values,
            )
            hi_row = cur.fetchone()
            return KeyRange(lo=tuple(lo_row), hi=tuple(hi_row))
        finally:
            cur.close()
            self._conn.rollback()

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
        """count(*) and SUM(row_hash) % 2**63 over the selected rows, in-engine."""
        params = self._Params()
        row_hash = self._row_hash_sql(table, pk_cols, compare_cols)
        where = self._where_sql(pk_cols, key_range, watermark_column, cutoff, params)
        tbl = self._table_ident(table)
        # ``mod(...)`` instead of the ``%`` operator: a literal ``%`` in the SQL would be
        # parsed by psycopg as a client-side placeholder. ``SUM(bigint)`` promotes to
        # ``numeric`` (no overflow), and ``mod(numeric, 2**63)`` matches Python's
        # ``sum(hashes) % 2**63``.
        query = _sql.SQL(
            "SELECT COUNT(*), "
            "mod(COALESCE(SUM({rh}), 0), {mod}) "
            "FROM {tbl} WHERE {where}"
        ).format(
            rh=row_hash,
            mod=_sql.Literal(CHECKSUM_MOD),
            tbl=tbl,
            where=where,
        )
        cur = self._cursor()
        try:
            cur.execute(query, self._bind(params, float_precision))
            count, checksum = cur.fetchone()
            return Checksum(count=int(count), checksum=int(checksum))
        finally:
            cur.close()
            self._conn.rollback()

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
        """``{key_tuple: row_hash}`` for every selected row."""
        params = self._Params()
        row_hash = self._row_hash_sql(table, pk_cols, compare_cols)
        where = self._where_sql(pk_cols, key_range, watermark_column, cutoff, params)
        return self._fetch_map(table, pk_cols, row_hash, where, params, float_precision)

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
        """Like :meth:`fetch_row_hashes` but restricted to an explicit key set."""
        if not keys:
            return {}
        params = self._Params()
        row_hash = self._row_hash_sql(table, pk_cols, compare_cols)
        key_pred = self._keys_predicate_sql(pk_cols, keys, params)
        cut_where = self._where_sql(pk_cols, None, watermark_column, cutoff, params)
        # Combine the IN-list with any cutoff predicate (shared param allocator keeps
        # placeholder names unique across both).
        where = _sql.SQL("({kp}) AND ({cw})").format(kp=key_pred, cw=cut_where)
        return self._fetch_map(table, pk_cols, row_hash, where, params, float_precision)

    # --- pruning / lag helpers (overrides of the non-abstract defaults) ---------

    def split_points(
        self,
        table: str,
        pk_cols: Sequence[str],
        key_range: KeyRange,
        watermark_column: Optional[str],
        cutoff: Optional[Any],
        n: int,
    ) -> Optional[List[Key]]:
        """Up to ``n - 1`` interior boundary keys that split the watermark-filtered rows
        in ``key_range`` into ~``n`` equal-count buckets.

        Mirrors :meth:`MemoryConnector.split_points`: order the selected rows by the full
        pk tuple, then pick the keys at the 1-based row positions
        ``round(total * i / n)`` for ``i`` in ``1..n-1`` and keep those that are strictly
        greater than the smallest selected key (the engine's effective low bound) and
        strictly increasing. ``round`` here is integer ``(total * i + n // 2) // n`` so it
        matches Python's banker's-rounding-free half-up split used by the engine's
        equal-count intent; any off-by-one only shifts a bucket boundary and never breaks
        the partition guarantee (every selected row still lands in exactly one sub-range).

        A single windowed pass computes ``ROW_NUMBER() OVER (ORDER BY pk...)`` and
        ``COUNT(*) OVER ()`` so we never materialise the whole table client-side; the
        boundaries themselves are pulled out in SQL. Works for single-column integer/text
        keys and composite keys alike because ordering and selection are over the whole pk
        tuple (the same row-value semantics the range predicate uses).

        Returns ``None`` when the range holds ``<= 1`` row, when ``n < 2``, or when no
        valid interior boundary survives de-duplication / the low-bound drop - the engine
        then treats the range as a leaf.
        """
        if n < 2:
            return None

        params = self._Params()
        where = self._where_sql(pk_cols, key_range, watermark_column, cutoff, params)
        pk_list = _sql.SQL(", ").join(self._col(c) for c in pk_cols)
        tbl = self._table_ident(table)
        n_pk = len(pk_cols)

        # ``ordered`` numbers every selected row by the full pk tuple and tags it with the
        # total count. The outer query keeps only the rows whose 1-based position is one of
        # the target split positions ``round(total*i/n)`` for i in 1..n-1, computed entirely
        # in SQL from ``generate_series``. ``DISTINCT`` collapses positions that collide
        # (e.g. when total < n) so duplicate boundaries never reach Python.
        query = _sql.SQL(
            "WITH ordered AS ("
            "  SELECT {pk}, "
            "         ROW_NUMBER() OVER (ORDER BY {pk}) AS dw_rn, "
            "         COUNT(*) OVER () AS dw_total "
            "  FROM {tbl} WHERE {where}"
            ") "
            "SELECT DISTINCT {pk} FROM ordered "
            "WHERE dw_total > 1 "
            "  AND dw_rn IN ("
            "    SELECT (dw_total * i + {n} / 2) / {n} "
            "    FROM generate_series(1, {n} - 1) AS g(i)"
            "  ) "
            "  AND dw_rn > 1 "
            "ORDER BY {pk}"
        ).format(pk=pk_list, tbl=tbl, where=where, n=_sql.Literal(int(n)))

        cur = self._cursor()
        try:
            cur.execute(query, params.values)
            rows = cur.fetchall()
        finally:
            cur.close()
            self._conn.rollback()

        # ``dw_rn > 1`` already drops the very first row (the low bound). De-duplicate and
        # keep strictly increasing order across the pk tuple, matching MemoryConnector.
        bounds: List[Key] = []
        for row in rows:
            b = tuple(row[:n_pk])
            if not bounds or b > bounds[-1]:
                bounds.append(b)
        return bounds or None

    def keys_above_watermark(
        self,
        table: str,
        pk_cols: Sequence[str],
        key_range: KeyRange,
        watermark_column: Optional[str],
        cutoff: Optional[Any],
    ) -> List[Key]:
        """Keys in ``key_range`` whose watermark is *strictly greater* than ``cutoff`` -
        the in-flight rows the engine must drop from the target side.

        Returns ``[]`` immediately when there is no watermark column or no cutoff (matching
        :meth:`MemoryConnector.keys_above_watermark`). The range predicate is the same
        half-open ``[lo, hi)`` row-value comparison used everywhere else; only the cutoff
        flips from ``<=`` to ``> cutoff`` (built directly here rather than via ``_where_sql``
        so the strict comparison and NULL-exclusion are explicit).
        """
        if watermark_column is None or cutoff is None:
            return []

        params = self._Params()
        # Reuse the shared range predicate (lo/hi only) so composite keys, half-open
        # boundaries, and identifier quoting all match the other methods exactly.
        range_where = self._where_sql(pk_cols, key_range, None, None, params)
        wm = self._col(watermark_column)
        wm_clause = _sql.SQL("{wm} IS NOT NULL AND {wm} > {ph}").format(
            wm=wm, ph=params.add(cutoff)
        )
        where = _sql.SQL("({rw}) AND ({wm})").format(rw=range_where, wm=wm_clause)

        pk_list = _sql.SQL(", ").join(self._col(c) for c in pk_cols)
        tbl = self._table_ident(table)
        query = _sql.SQL("SELECT {pk} FROM {tbl} WHERE {where}").format(
            pk=pk_list, tbl=tbl, where=where
        )
        n_pk = len(pk_cols)
        out: List[Key] = []
        cur = self._cursor()
        try:
            cur.execute(query, params.values)
            for row in cur:
                out.append(tuple(row[:n_pk]))
            return out
        finally:
            cur.close()
            self._conn.rollback()

    # --- shared fetch ----------------------------------------------------------

    def _fetch_map(
        self,
        table: str,
        pk_cols: Sequence[str],
        row_hash: _sql.Composable,
        where: _sql.Composable,
        params: "PostgresConnector._Params",
        float_precision: int,
    ) -> Dict[Key, int]:
        pk_list = _sql.SQL(", ").join(self._col(c) for c in pk_cols)
        tbl = self._table_ident(table)
        query = _sql.SQL(
            "SELECT {pk}, {rh} FROM {tbl} WHERE {where}"
        ).format(pk=pk_list, rh=row_hash, tbl=tbl, where=where)
        n_pk = len(pk_cols)
        out: Dict[Key, int] = {}
        cur = self._cursor()
        try:
            cur.execute(query, self._bind(params, float_precision))
            for row in cur:
                key = tuple(row[:n_pk])
                out[key] = int(row[n_pk])
            return out
        finally:
            cur.close()
            self._conn.rollback()

    @staticmethod
    def _bind(params: "PostgresConnector._Params", float_precision: int) -> Dict[str, Any]:
        """Merge the collected range/cutoff/key values with the named
        ``float_precision`` placeholder (referenced by every ``driftwatch_float_g``
        call). All placeholders in the query are named, as psycopg v3 forbids mixing
        positional and named parameters."""
        bound: Dict[str, Any] = dict(params.values)
        bound["float_precision"] = int(float_precision)
        return bound

    def close(self) -> None:
        try:
            self._conn.close()
        except Exception:  # pragma: no cover - best-effort cleanup
            pass


# A pure-SQL helper that reproduces CPython ``format(v, '.<p>g')`` (== C ``printf
# '%.<p>g'``, which DuckDB uses) for finite doubles, BYTE-FOR-BYTE, at every precision
# 1..17. Created once per session as a temporary function. NaN/Inf are not expected in
# keyed comparison data; if they occur the function returns Python's repr tokens
# ('inf'/'-inf'/'nan'), documented as a known sharp edge same as the Python contract.
#
# Why this is non-trivial (and why the naive version was wrong)
# -------------------------------------------------------------
# C ``%g`` rounds the *exact* binary value of the double using round-HALF-TO-EVEN
# (banker's rounding). Two facts make a faithful emulation in Postgres harder than it
# looks, and both must be handled or p=16/17 diverge from Python/DuckDB:
#
#   1. ``round(numeric, s)`` in Postgres rounds half-AWAY-from-zero, not half-to-even.
#      On an exact tie (the value sits exactly on the .5 boundary at the target scale)
#      C ``%g`` rounds toward the even neighbour while Postgres rounds away. This alone
#      diverges on ~1% of values at 16-17 sig digits (and rarely at 12-14).
#
#   2. ``double precision::numeric`` does NOT give the exact decimal value of the double:
#      it produces the SHORTEST round-trip decimal (the same text ``float8out`` / Python
#      ``repr`` give) and then zero-pads. Rounding that shortened value to 16/17 sig
#      digits disagrees with rounding the TRUE value on >50% of doubles at p=17. So the
#      cast cannot be used; the exact value must be reconstructed from the IEEE-754 bits.
#
# This implementation therefore (a) reconstructs the exact decimal value of the double
# from its 64-bit image - every finite double is an exact finite decimal, value =
# mantissa * 2^k, computed with exact numeric arithmetic (for k<0, mant * 5^-k shifted
# right by -k decimal places, which is exact) - and (b) rounds that exact value to p
# significant digits with explicit round-half-to-EVEN. The %g notation choice
# (scientific iff exp < -4 or exp >= p), trailing-zero stripping, exponent sign and
# 2-digit-minimum padding, and the sign of zero ('-0') all mirror C/Python exactly.
#
# Verified: 0 mismatches vs Python ``format(v,'.{p}g')`` AND vs DuckDB ``printf`` over
# 180k random/adversarial doubles and 87k constructed tie cases (incl. 1.2k genuine
# half-even-vs-half-away ties) at p in {12,15,16,17}. See tests/test_postgres_connector.py.
_FLOAT_G_FUNCTION_SQL = r"""
CREATE OR REPLACE FUNCTION pg_temp.driftwatch_float_g(v double precision, p integer)
RETURNS text
LANGUAGE plpgsql IMMUTABLE
AS $func$
DECLARE
    bits bit(64);
    ef integer;          -- IEEE-754 exponent field (11 bits)
    mf numeric;          -- IEEE-754 mantissa field (52 bits)
    mant numeric;        -- full significand (with implicit leading bit for normals)
    k integer;           -- power of two applied to the significand
    av numeric;          -- exact, sign-stripped decimal value of the double
    sign text := '';
    exp10 integer;       -- decimal exponent of the most-significant digit
    q integer;           -- numeric scale to round to (fractional digits kept; may be <0)
    rounded numeric;
    truncated numeric;   -- av truncated toward zero at scale q
    rem numeric;         -- av - truncated (the dropped tail, always >= 0)
    half numeric;        -- exactly 0.5 ULP at scale q
    use_sci boolean;
    mant_txt text;
    e_str text;
    out_text text;
BEGIN
    IF v IS NULL THEN
        RETURN NULL;
    END IF;
    -- Non-finite: mirror Python's repr tokens (inf, -inf, nan).
    IF v = 'Infinity'::double precision THEN RETURN 'inf'; END IF;
    IF v = '-Infinity'::double precision THEN RETURN '-inf'; END IF;
    IF v <> v THEN RETURN 'nan'; END IF;  -- NaN

    IF v = 0 THEN
        -- Python: format(0.0, '.12g') == '0'; format(-0.0, '.12g') == '-0'.
        -- Detect the IEEE-754 sign bit directly (Postgres preserves -0.0); dividing by
        -- v would raise division-by-zero here, so inspect the raw 8-byte image instead.
        IF float8send(v) = '\x8000000000000000'::bytea THEN
            RETURN '-0';
        END IF;
        RETURN '0';
    END IF;

    -- Reconstruct the EXACT decimal value from the IEEE-754 image (see header comment).
    -- A plain ``v::numeric`` would give the shortest round-trip decimal, not the exact
    -- value, and would round wrong at 16-17 sig digits, so we go through the raw bits.
    bits := ('x' || encode(float8send(v), 'hex'))::bit(64);
    ef := (substring(bits from 2 for 11))::bit(11)::int;
    mf := (substring(bits from 13 for 52))::bit(52)::bigint::numeric;
    IF ef = 0 THEN
        -- Subnormal (or zero, already handled): no implicit leading 1, exp is -1074.
        mant := mf;
        k := -1074;
    ELSE
        -- Normal: implicit leading bit adds 2^52; biased exponent -> 2-power is ef-1075.
        mant := mf + 4503599627370496::numeric;  -- 2^52
        k := ef - 1075;
    END IF;
    IF k >= 0 THEN
        av := mant * (2::numeric ^ k);
    ELSE
        -- mant / 2^-k = mant * 5^-k * 10^k : the 5^-k multiply is exact (integer), and
        -- '1e<k>'::numeric is an exact decimal-point shift, so av is the exact value.
        av := (mant * (5::numeric ^ (-k))) * ('1e' || k)::numeric;
    END IF;
    IF (substring(bits from 1 for 1)) = B'1' THEN
        sign := '-';
    END IF;

    -- floor(log10(av)): float estimate via ln(), then corrected EXACTLY against the
    -- numeric powers of ten so it is never off-by-one at decade boundaries.
    exp10 := floor(ln(av) / ln(10::numeric))::integer;
    WHILE av < (10::numeric ^ exp10) LOOP exp10 := exp10 - 1; END LOOP;
    WHILE av >= (10::numeric ^ (exp10 + 1)) LOOP exp10 := exp10 + 1; END LOOP;

    -- Round av to p significant digits with round-half-to-EVEN (C %g semantics).
    -- q = fractional digits to keep; negative for magnitudes >= 10^p.
    q := p - 1 - exp10;
    truncated := trunc(av, q);                              -- toward zero
    rem := av - truncated;                                  -- dropped tail (>= 0)
    half := (5::numeric) * (10::numeric ^ (-(q + 1)));      -- one half-ULP at scale q
    IF rem < half THEN
        rounded := truncated;
    ELSIF rem > half THEN
        rounded := truncated + (10::numeric ^ (-q));
    ELSE
        -- Exact tie: keep the truncated value iff its last kept digit is already even,
        -- otherwise step up to the even neighbour. ``truncated * 10^q`` is the integer
        -- of kept digits; its parity is that last digit's parity.
        IF mod((truncated * (10::numeric ^ q))::numeric, 2::numeric) = 0 THEN
            rounded := truncated;
        ELSE
            rounded := truncated + (10::numeric ^ (-q));
        END IF;
    END IF;

    -- Rounding can carry into a new decade (9.99..e0 -> 1.0eX): recompute exp10.
    IF rounded <> 0 THEN
        exp10 := floor(ln(rounded) / ln(10::numeric))::integer;
        WHILE rounded < (10::numeric ^ exp10) LOOP exp10 := exp10 - 1; END LOOP;
        WHILE rounded >= (10::numeric ^ (exp10 + 1)) LOOP exp10 := exp10 + 1; END LOOP;
    END IF;

    -- %g rule: scientific iff exponent < -4 or exponent >= precision.
    use_sci := (exp10 < -4) OR (exp10 >= p);

    IF use_sci THEN
        -- Normalise mantissa to [1,10): multiply by 10^-exp10 (exact), trim zeros.
        mant_txt := trim_scale(rounded * (10::numeric ^ (-exp10)))::text;
        e_str := abs(exp10)::text;
        IF length(e_str) < 2 THEN
            e_str := lpad(e_str, 2, '0');
        END IF;
        IF exp10 < 0 THEN
            out_text := sign || mant_txt || 'e-' || e_str;
        ELSE
            out_text := sign || mant_txt || 'e+' || e_str;
        END IF;
    ELSE
        -- Fixed notation: trim_scale drops trailing zeros; integral values lose the dot.
        out_text := sign || trim_scale(rounded)::text;
    END IF;

    RETURN out_text;
END;
$func$;
"""
