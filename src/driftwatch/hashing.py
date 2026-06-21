"""The cross-dialect hashing contract.

This module is the **source of truth** for how a row is canonicalized and hashed.
Every connector must emit SQL that reproduces these exact digests; the conformance
test asserts that Postgres / DuckDB / Snowflake agree with this Python reference over
the same data. If you change anything here, you change the contract for all connectors.

Pipeline:
  1. ``canonical(value)`` -> a canonical UTF-8 string per type (the fragile part).
  2. row hash = first 60 bits of ``md5(FIELD_SEP.join(canonical(v) for v in row))``.
     The primary key is always part of the row, so every row hash is distinct, which
     makes the order-independent SUM aggregate collision-safe.
  3. segment checksum = ``SUM(row_hash) mod 2**63`` over the rows in the segment.
     SUM is order-independent, so neither side has to sort - each engine computes it
     natively and only the integer digest crosses the wire.
"""

from __future__ import annotations

import datetime as _dt
import hashlib
from decimal import Decimal
from typing import Iterable, Sequence

# --- contract constants (connectors must use identical values) -----------------

FIELD_SEP = "\x1f"          # ASCII Unit Separator: between fields of a row
NULL_SENTINEL = "\\N"       # canonical form of SQL NULL
DEFAULT_FLOAT_PRECISION = 17  # significant digits for floats. 17 round-trips an IEEE-754 double
# exactly, so two doubles compare equal iff they are the same value (no precision is lost). All
# three engines agree byte-for-byte at 17: the Postgres renderer reconstructs the exact decimal
# from the double's bits and rounds half-to-even like C %g (verified over ~180k random and
# adversarial doubles, 0 mismatches at 12/15/16/17). Lower this if a float column is recomputed
# (not copied) in the target and you want to tolerate last-bit differences from different math.
HASH_HEX_CHARS = 15          # first 15 hex chars of md5 = 60 bits
CHECKSUM_MOD = 2 ** 63       # SUM aggregate is taken modulo this


def canonical(value: object, float_precision: int = DEFAULT_FLOAT_PRECISION) -> str:
    """Canonical string form of a single SQL value.

    Rules (must be reproducible in every connector's dialect):
      NULL -> NULL_SENTINEL; bool -> '0'/'1'; int -> base-10; Decimal -> fixed,
      trailing zeros trimmed; float -> '%.<p>g'; datetime -> UTC ISO at microsecond
      precision; date -> ISO date; bytes -> lowercase hex; everything else -> str().
    """
    if value is None:
        return NULL_SENTINEL
    # bool must precede int (bool is a subclass of int in Python)
    if isinstance(value, bool):
        return "1" if value else "0"
    if isinstance(value, int):
        return str(value)
    if isinstance(value, Decimal):
        return _canonical_decimal(value)
    if isinstance(value, float):
        return format(value, "." + str(float_precision) + "g")
    if isinstance(value, _dt.datetime):
        return _canonical_datetime(value)
    if isinstance(value, _dt.date):
        return value.isoformat()
    if isinstance(value, (bytes, bytearray)):
        return bytes(value).hex()
    return str(value)


def _canonical_decimal(value: Decimal) -> str:
    if value == value.to_integral_value():
        # normalize() can yield exponent notation for integral decimals (e.g. 1E+2);
        # force plain integer form.
        return str(value.quantize(Decimal(1)))
    normalized = value.normalize()
    return format(normalized, "f")


def _canonical_datetime(value: _dt.datetime) -> str:
    if value.tzinfo is not None:
        value = value.astimezone(_dt.timezone.utc).replace(tzinfo=None)
    # microsecond precision, space separator, no timezone suffix
    return value.strftime("%Y-%m-%d %H:%M:%S.%f")


def row_hash(values: Sequence[object], float_precision: int = DEFAULT_FLOAT_PRECISION) -> int:
    """60-bit integer hash of one row (key columns + compared columns, in order)."""
    joined = FIELD_SEP.join(canonical(v, float_precision) for v in values)
    digest = hashlib.md5(joined.encode("utf-8")).hexdigest()
    return int(digest[:HASH_HEX_CHARS], 16)


def segment_checksum(row_hashes: Iterable[int]) -> int:
    """Order-independent aggregate of row hashes for a segment."""
    total = 0
    for h in row_hashes:
        total += h
    return total % CHECKSUM_MOD
