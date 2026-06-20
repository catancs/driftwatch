"""driftwatch — continuous, cross-engine reconciliation of derived data.

Trust, but verify: check that a derived dataset (warehouse mirror, search index,
materialized view, read replica) still matches its source of truth, tolerating
replication lag, and report drift.
"""

__version__ = "0.1.0.dev0"
