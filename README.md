# driftwatch

**Continuous, cross-engine reconciliation of derived data against its source of truth.**
Trust, but verify your CDC / warehouse / materialized-view pipelines: driftwatch checks that a
derived dataset (a warehouse mirror, search index, read replica, materialized view) still
matches its source of truth — tolerating replication lag — and tells you exactly which rows
have drifted.

> Status: alpha, under active construction. See `docs/superpowers/specs/` for the design.

## Why

Derived data drifts silently — a dropped CDC event, an off-by-one in incremental view
maintenance, a half-run backfill. Today that's caught by a customer complaint or an auditor,
not by tooling. Every CDC guide says "write your own reconciliation job." driftwatch is that
job, done once, properly: a recursive **hash-segmentation** diff (only moves data where drift
exists) with **lag-aware** comparison (watermark cutoff + recheck) so it doesn't cry wolf on
rows the warehouse simply hasn't caught up on yet.

## Quickstart (local, no cloud)

```bash
pip install "driftwatch[postgres,duckdb]"
driftwatch init > driftwatch.yaml   # scaffold a config
driftwatch run -c driftwatch.yaml   # exit 0 = in sync, 1 = drift, 2 = error, 3 = bad config
```

## Connectors

Built-in: `postgres`, `snowflake`, `duckdb` (local stand-in), `memory` (tests). Connectors are
optional extras (`pip install driftwatch[snowflake]`) and resolve via the
`driftwatch.connectors` entry-point group — publish your own as a separate package, no change to
core.

## License

Apache-2.0.
