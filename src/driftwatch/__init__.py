"""driftwatch - continuous, cross-engine reconciliation of derived data.

Trust, but verify: check that a derived dataset (warehouse mirror, search index,
materialized view, read replica) still matches its source of truth, tolerating
replication lag, and report drift.
"""

from importlib.metadata import PackageNotFoundError, version as _version

try:
    __version__ = _version("driftwatch")
except PackageNotFoundError:  # running from a source tree that is not installed
    __version__ = "0.1.0"
