<div align="center">

# driftwatch

**A reconciliation gate for derived data** — continuously check that a warehouse mirror,
search index, read replica, or materialized view still matches its source of truth, and
catch silent drift *before* it reaches a dashboard or an auditor.

[![License: Apache-2.0](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](LICENSE)
[![Python](https://img.shields.io/badge/python-3.9%2B-blue.svg)](https://www.python.org)
[![tests](https://img.shields.io/badge/tests-62%20passing-brightgreen.svg)](#development)
[![status](https://img.shields.io/badge/status-alpha-orange.svg)](#status)

</div>

**Derived data drifts silently** — a dropped CDC event, an off-by-one in incremental view
maintenance, a half-run backfill, an unhandled schema change. Today that's caught by a
customer complaint or an auditor, not by tooling, and every CDC guide just tells you to
"write your own reconciliation job." `driftwatch` *is* that job, done once and properly: it
diffs a derived table against its source of truth, **tolerates replication lag so it never
cries wolf**, and tells you exactly which rows drifted.

Think `data-diff`, but **continuous, lag-aware, and cross-engine** (Postgres → Snowflake) —
and it never drags every row over the wire to find out.

Drop it in CI or cron — the exit code decides whether your mirror is trustworthy:

```console
$ driftwatch run -c driftwatch.yaml   # exit 0 = in sync · 1 = drift · 2 = error · 3 = bad config
driftwatch: orders — DRIFT
  drift keys: 3 total (missing=1, extra=1, changed=1)
  rows compared: 51   segments scanned: 1   duration: 0.019s
  diverging keys:
    [changed] 10
    [missing] 20
    [extra]   999
```

---

## Install

### From source

The path that works today (the package isn't on PyPI yet):

```bash
git clone https://github.com/catancs/driftwatch && cd driftwatch
pip install ".[postgres,snowflake,duckdb]"   # pick only the connectors you need
driftwatch --help
```

Connectors are **optional extras** — the core stays tiny. Install just what you use:
`driftwatch[postgres]`, `driftwatch[snowflake]`, `driftwatch[duckdb]` (the local stand-in).

### PyPI & Homebrew

Planned for the first tagged release:

```bash
pip install "driftwatch[postgres,snowflake]"   # coming soon
```

## Use — one verb, a config file

```bash
driftwatch init > driftwatch.yaml                 # scaffold a config
driftwatch run -c driftwatch.yaml                 # reconcile; exit 0/1/2/3
driftwatch run -c driftwatch.yaml --format json   # machine-readable report (for CI / alerting)
driftwatch run -c driftwatch.yaml --only orders   # run a single named comparison
```

The whole tool is driven by one declarative config. Secrets come from the environment via
`${VAR}` — never put credentials in the file:

```yaml
connections:
  source: { driver: postgres,  dsn: ${PG_DSN} }
  target:
    driver: snowflake
    account: ${SNOWFLAKE_ACCOUNT}
    user: ${SNOWFLAKE_USER}
    password: ${SNOWFLAKE_PASSWORD}
    database: ANALYTICS

comparisons:
  - name: orders
    source_table: public.orders
    target_table: ANALYTICS.PUBLIC.ORDERS
    primary_key: [id]
    watermark_column: updated_at   # only compare rows older than the grace window…
    grace: 15m                      # …so warehouse lag is never reported as drift
    compare_columns: "*"            # or an explicit list; exclude_columns also supported
    recheck: { delay: 60s, rounds: 1 }
```

Want a zero-cloud trial? Point both `source` and `target` at local DuckDB files
(`driver: duckdb`) and you can see the whole pipeline work in under a minute.

## How it works

Two ideas do the heavy lifting:

- **Recursive hash-segmentation.** Both sides hash key-ranges natively and compare only the
  digests; matching segments are *pruned* without moving a single row. Drift is found by
  descending only into the ranges that disagree — so a clean billion-row table costs almost
  nothing, and a sparse drift is located in `O(log n)` round-trips. (In the test suite, a
  6,000-row table with 3 drifted rows touches just **4.6%** of rows.)
- **Lag-aware comparison.** A naive diff flags every row the warehouse hasn't caught up on
  yet — useless noise. `driftwatch` captures a **watermark cutoff** once at run start and
  only compares rows old enough to have propagated, then runs a bounded **recheck pass** on
  the survivors to drop anything that reconciled in the meantime. Two independent safety nets,
  so it stays quiet until something is *actually* wrong.

Read-only on both sides; a run is deterministic and idempotent.
Full design: [`docs/superpowers/specs/2026-06-20-driftwatch-design.md`](docs/superpowers/specs/2026-06-20-driftwatch-design.md).

## What it catches

| Failure mode | How it shows up |
|---|---|
| Dropped / lost CDC event | `missing` key (in source, absent in target) |
| Phantom or un-deleted rows | `extra` key (in target, absent in source) |
| Off-by-one / wrong value in a view | `changed` key (present both, different content) |
| Half-run backfill | a cluster of `missing` keys in one range |
| Silent schema / encoding drift | `changed` keys across the board |

## Connectors

Built-in: `postgres`, `snowflake`, `duckdb` (local stand-in), `memory` (tests). They resolve
via the `driftwatch.connectors` entry-point group, so **you can publish a new connector as its
own package** — no change to core.

### Adding a connector

Implement the six-method `driftwatch.connector.Connector` interface, reproduce the hashing
contract (`driftwatch/hashing.py`) in your dialect's SQL, and register an entry point:

```toml
[project.entry-points."driftwatch.connectors"]
clickhouse = "driftwatch_clickhouse:ClickHouseConnector"
```

The conformance test in `tests/test_duckdb_connector.py` — assert your connector's digests
match `MemoryConnector` over the same data — is the gate every connector must pass.

## How it compares

| | continuous | cross-engine | lag-aware | open source |
|---|:---:|:---:|:---:|:---:|
| **driftwatch** | ✅ | ✅ | ✅ | ✅ |
| `data-diff` / `reladiff` | ❌ one-shot | ✅ | ❌ | ⚠️ archived / fork |
| `dbt` tests / audit-helper | ❌ batch | ❌ in-warehouse | ❌ | ✅ |
| Great Expectations | ❌ | ❌ single dataset | ❌ | ✅ |
| Monte Carlo comparisons | ⏱ scheduled | ⚠️ warehouse-only | ❌ | ❌ proprietary |
| `pt-table-checksum` | ✅ | ❌ MySQL-only | ✅ | ✅ |

The empty cell everyone else leaves — *continuous, cross-engine, lag-aware reconciliation as
an open-source library* — is the one `driftwatch` fills.

## Development

```bash
pip install ".[dev,duckdb]" && pytest -q     # 62 passing, 3 skipped (gated live DBs)
```

The test suite runs fully offline against DuckDB; Postgres conformance runs against a service
container, and Snowflake tests are gated on credentials. A GitHub Actions workflow ships at
[`docs/ci-workflow.yml`](docs/ci-workflow.yml) — move it to `.github/workflows/ci.yml` to
enable CI (that path needs a token with the `workflow` scope).

## Status

**Alpha.** The engine, connectors, and CLI are tested and working; the always-on daemon, a
Prometheus exporter, and PyPI/Homebrew releases are next. Issues and connector contributions
welcome.

## License

[Apache-2.0](LICENSE).
