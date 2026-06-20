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

### Example output

```text
driftwatch: orders — DRIFT
  drift keys: 3 total (missing=1, extra=1, changed=1)
  rows compared: 51
  segments scanned: 1
  candidates before recheck: 3
  cutoff: -
  duration: 0.019s
  diverging keys:
    [changed] 10
    [missing] 20
    [extra] 999
```

`--format json` emits the full machine-readable report (every diverging key) for CI/alerting.

## Connectors

Built-in: `postgres`, `snowflake`, `duckdb` (local stand-in), `memory` (tests). Connectors are
optional extras (`pip install driftwatch[snowflake]`) and resolve via the
`driftwatch.connectors` entry-point group — publish your own as a separate package, no change to
core.

### Adding a connector

Implement the `driftwatch.connector.Connector` interface (six methods: `columns`, `pk_bounds`,
`checksum`, `fetch_row_hashes`, `fetch_row_hashes_for_keys`, `close`), reproduce the hashing
contract in `driftwatch/hashing.py` in your dialect's SQL, and register an entry point:

```toml
[project.entry-points."driftwatch.connectors"]
clickhouse = "driftwatch_clickhouse:ClickHouseConnector"
```

The conformance pattern in `tests/test_duckdb_connector.py` (compare your connector's digests
against `MemoryConnector` over the same data) is the gate every connector must pass.

## Development

```bash
pip install ".[dev,duckdb]" && pytest -q
```

A GitHub Actions workflow is provided at `docs/ci-workflow.yml` (matrix py3.9–3.12 with a
Postgres service). Move it to `.github/workflows/ci.yml` to enable CI — that path requires a
token with the `workflow` scope.

## License

Apache-2.0.
