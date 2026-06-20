<div align="center">

# driftwatch

**A reconciliation gate for derived data** — continuously check that a warehouse mirror,
search index, read replica, or materialized view still matches its source of truth, and
catch silent drift *before* it reaches a dashboard or an auditor.

[![License: Apache-2.0](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](LICENSE)
[![Python](https://img.shields.io/badge/python-3.9%2B-blue.svg?logo=python&logoColor=white)](https://www.python.org)
[![tests](https://img.shields.io/badge/tests-62%20passing-brightgreen.svg)](#development)
[![status](https://img.shields.io/badge/status-alpha-orange.svg)](#status)

<br/>

**Works with**

[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-source%20of%20truth-336791?logo=postgresql&logoColor=white)](#connectors)
[![Snowflake](https://img.shields.io/badge/Snowflake-derived-29B5E8?logo=snowflake&logoColor=white)](#connectors)
[![DuckDB](https://img.shields.io/badge/DuckDB-local%20stand--in-FFF000?logo=duckdb&logoColor=black)](#connectors)
[![+ your warehouse](https://img.shields.io/badge/%2B-your%20warehouse-lightgrey)](#adding-a-connector)

</div>

---

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

## Quickstart

Five steps from nothing to a reconciliation check wired into CI.

**1. Install** it (pick only the connectors you need — the core stays tiny):

```bash
git clone https://github.com/catancs/driftwatch && cd driftwatch
pip install ".[postgres,snowflake,duckdb]"      # PyPI release coming soon
```

**2. Scaffold a config:**

```bash
driftwatch init > driftwatch.yaml
```

**3. Point it at your databases.** Open `driftwatch.yaml` and set the `source` (your system of
record) and `target` (the derived copy), the `primary_key`, and a `watermark_column` so lag
isn't mistaken for drift. Credentials come from the environment via `${VAR}` — never hardcode them.

**4. Run it:**

```bash
driftwatch run -c driftwatch.yaml
```

Read the exit code — it's the whole contract: **`0`** in sync · **`1`** drift (the diverging
keys are printed) · **`2`** operational error · **`3`** bad config.

**5. Wire it into CI or cron** so drift fails a build instead of surprising a user:

```yaml
# .github/workflows/reconcile.yml (or any scheduler)
- run: driftwatch run -c driftwatch.yaml   # non-zero exit blocks the pipeline
```

> [!TIP]
> **No cloud handy?** Set both `source` and `target` to `driver: duckdb` with local file
> paths and watch the whole pipeline work offline in under a minute.

## Configuration

One declarative file drives everything. Secrets are interpolated from the environment:

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

Run one comparison at a time with `--only orders`, or get a machine-readable report for
alerting with `--format json`.

## How it works

Two ideas do the heavy lifting:

- **Recursive hash-segmentation.** Both sides hash key-ranges natively and compare only the
  digests; matching segments are *pruned* without moving a single row. Drift is found by
  descending only into the ranges that disagree — so a clean billion-row table costs almost
  nothing, and a sparse drift is located in `O(log n)` round-trips. (In the test suite, a
  6,000-row table with 3 drifted rows touches just **4.6%** of rows.)
- **Lag-aware comparison.** A naive diff flags every row the warehouse hasn't caught up on
  yet — useless noise. `driftwatch` captures a **watermark cutoff** once at run start and only
  compares rows old enough to have propagated, then runs a bounded **recheck pass** on the
  survivors to drop anything that reconciled meanwhile. Two independent safety nets, so it
  stays quiet until something is *actually* wrong.

Read-only on both sides; every run is deterministic and idempotent.
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

## Credit — built on Kleppmann's "Trust, but Verify"

driftwatch is a concrete implementation of a future-work idea from **Martin Kleppmann**'s
[*Designing Data-Intensive Applications*](https://dataintensive.net) (2nd ed., 2026, with
**Chris Riccomini**). In the final chapter on **Aiming for Correctness**, he argues that
mature data systems should stop *assuming* derived data is correct and instead **continually
verify their own integrity** — his "**trust, but verify**" principle — making reconciliation
and auditability first-class concerns rather than afterthoughts.

That idea never had a good open-source home for the CDC/warehouse era; everyone hand-rolls a
one-off reconciliation job. driftwatch is the missing piece, built directly on his suggestion:
**continuous, cross-engine, lag-aware verification of derived data against its source of truth.**

> 📖 If you find this useful, read the book — the "why" behind driftwatch is Chapter 12 / 13
> of DDIA. The credit for the idea is Kleppmann & Riccomini's; the implementation is ours.

## Development

```bash
pip install ".[dev,duckdb]" && pytest -q     # 62 passing, 3 skipped (gated live DBs)
```

The suite runs fully offline against DuckDB; Postgres conformance runs against a service
container, and Snowflake tests are gated on credentials. A GitHub Actions workflow ships at
[`docs/ci-workflow.yml`](docs/ci-workflow.yml) — move it to `.github/workflows/ci.yml` to
enable CI (that path needs a token with the `workflow` scope).

## Status

**Alpha.** The engine, connectors, and CLI are tested and working; the always-on daemon, a
Prometheus exporter, and PyPI/Homebrew releases are next. Issues and connector contributions
welcome.

## License

[Apache-2.0](LICENSE).
