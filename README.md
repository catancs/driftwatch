<div align="center">

# driftwatch

**A reconciliation gate for derived data.**

Check that a warehouse mirror, search index, read replica, or materialized view still matches
its source of truth, and catch drift before it reaches a dashboard or an auditor.

[![License: Apache-2.0](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](LICENSE)
[![Python](https://img.shields.io/badge/python-3.9%2B-blue.svg?logo=python&logoColor=white)](https://www.python.org)
[![tests](https://img.shields.io/badge/tests-62%20passing-brightgreen.svg)](#development)
[![status](https://img.shields.io/badge/status-alpha-orange.svg)](#status)

<br/>

**Works with**

[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-source%20of%20truth-336791?logo=postgresql&logoColor=white)](#connectors)
[![Snowflake](https://img.shields.io/badge/Snowflake-derived-29B5E8?logo=snowflake&logoColor=white)](#connectors)
[![DuckDB](https://img.shields.io/badge/DuckDB-local-FFF000?logo=duckdb&logoColor=black)](#connectors)
[![plus your warehouse](https://img.shields.io/badge/plus-your%20warehouse-lightgrey)](#adding-a-connector)

</div>

---

Derived data drifts. A dropped CDC event, an off-by-one in a view, a half-run backfill, a
schema change that nobody caught. Usually a customer or an auditor finds it first. driftwatch
finds it for you. It compares a derived table against its source of truth, ignores rows that
are only lagging behind, and reports exactly which rows differ.

```console
$ driftwatch run -c driftwatch.yaml   # exit 0 = in sync, 1 = drift, 2 = error, 3 = bad config
driftwatch: orders - DRIFT
  drift keys: 3 total (missing=1, extra=1, changed=1)
  rows compared: 51   segments scanned: 1   duration: 0.019s
  diverging keys:
    [changed] 10
    [missing] 20
    [extra]   999
```

## Where it fits

driftwatch reads both ends of a pipeline, read-only, and returns an exit code. You put it
wherever you already run jobs.

<p align="center"><img src="docs/img/architecture.svg" width="780" alt="Where driftwatch sits in a data system: it reads Postgres (source of truth) and the derived copies (Snowflake, search index) read-only, then returns an exit code to CI, cron, or alerting."></p>

## Use it in your stack

driftwatch is a single command with a clear exit code, so it drops into the places you already
have. Pick one.

**GitHub Actions.** A non-zero exit fails the job, so drift blocks the workflow.

```yaml
- uses: catancs/driftwatch@v0
  with:
    config: driftwatch.yaml
  env:
    PG_DSN: ${{ secrets.PG_DSN }}
    SNOWFLAKE_ACCOUNT: ${{ secrets.SNOWFLAKE_ACCOUNT }}
    SNOWFLAKE_USER: ${{ secrets.SNOWFLAKE_USER }}
    SNOWFLAKE_PASSWORD: ${{ secrets.SNOWFLAKE_PASSWORD }}
```

**Cron.** Run it on a schedule and page someone when it fails.

```cron
*/30 * * * * driftwatch run -c /etc/driftwatch.yaml || /usr/local/bin/page-oncall "orders drift"
```

**Airflow or dbt.** Run it right after the model build so a bad table never goes unverified.

```python
BashOperator(task_id="reconcile_orders",
             bash_command="driftwatch run -c /opt/driftwatch.yaml")
# the task fails when driftwatch exits non-zero, so the DAG surfaces the drift
```

**Alerting.** Ask for a JSON report and forward it.

```bash
driftwatch run -c driftwatch.yaml --format json | your-alerter
```

**Promotion gate.** Run it before you swap a staging table into production or serve a rebuilt
index. A non-zero exit stops the promotion.

## Quickstart

Five steps from nothing to a reconciliation check wired into CI.

**1. Install** it (pick only the connectors you need, the core stays small):

```bash
git clone https://github.com/catancs/driftwatch && cd driftwatch
pip install ".[postgres,snowflake,duckdb]"
```

**2. Scaffold a config:**

```bash
driftwatch init > driftwatch.yaml
```

**3. Point it at your databases.** Open `driftwatch.yaml`, set the `source` (your system of
record) and `target` (the derived copy), the `primary_key`, and a `watermark_column` so lag is
not mistaken for drift. Credentials come from the environment through `${VAR}`.

**4. Run it:**

```bash
driftwatch run -c driftwatch.yaml
```

Read the exit code, that is the whole contract: `0` in sync, `1` drift (with the diverging keys
printed), `2` operational error, `3` bad config.

**5. Wire it into CI or cron** so drift fails a build instead of surprising a user (see
[Use it in your stack](#use-it-in-your-stack)).

> [!TIP]
> No cloud handy? Set both `source` and `target` to `driver: duckdb` with local file paths and
> watch the whole pipeline work offline in under a minute.

## Install and run

| You want | Do this |
|---|---|
| A Python install | `pip install ".[postgres,snowflake,duckdb]"` (PyPI release planned) |
| A container | `docker build -t driftwatch . && docker run --rm -v "$PWD/driftwatch.yaml:/app/driftwatch.yaml" driftwatch run -c driftwatch.yaml` |
| A GitHub Action | `uses: catancs/driftwatch@v0` (see above) |
| Make targets | `make install`, `make test`, `make run`, `make docker` |

## Configuration

One declarative file drives everything. Secrets are read from the environment.

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
    watermark_column: updated_at   # only compare rows older than the grace window,
    grace: 15m                      # so warehouse lag is never reported as drift
    compare_columns: "*"            # or an explicit list; exclude_columns also supported
    recheck: { delay: 60s, rounds: 1 }
```

Run one comparison with `--only orders`, or get a machine-readable report with `--format json`.

## How it works

Two ideas do the work.

**Recursive hash-segmentation.** Both sides hash key-ranges in their own SQL and compare only
the digests. Matching ranges are skipped without reading their rows. driftwatch descends only
into the ranges that disagree, so a clean table is cheap to verify and a sparse drift is found
in a few round-trips.

**Lag-aware comparison.** A naive diff reports every row the warehouse has not caught up on yet,
which is noise. driftwatch captures a watermark cutoff once at the start of a run and compares
only rows old enough to have propagated, then runs a bounded recheck pass on the candidates and
drops anything that reconciled in the meantime. Two independent checks, so it stays quiet until
something is actually wrong.

Every run is read-only on both sides, deterministic, and idempotent. Full design in
[`docs/superpowers/specs/2026-06-20-driftwatch-design.md`](docs/superpowers/specs/2026-06-20-driftwatch-design.md).

## Performance

Because matching ranges are pruned instead of scanned, the work tracks the amount of drift, not
the size of the table. Verifying a 6,000-row table with 3 drifted rows reads 275 rows, about
4.6 percent of the table.

<p align="center"><img src="docs/img/perf-rows.svg" width="720" alt="Rows read to verify a 6,000-row table with 3 drifted rows: a full table scan reads 6,000 rows, driftwatch reads 275."></p>

For sparse drift the cost stays roughly flat as the table grows, while a full scan grows with
the table.

<p align="center"><img src="docs/img/perf-scaling.svg" width="640" alt="Log-log chart: rows read by a full table scan grow linearly with table size, while driftwatch stays flat for sparse drift, about 1000x fewer rows at 100 million."></p>

## What it catches

| Failure mode | How it shows up |
|---|---|
| Dropped or lost CDC event | `missing` key (in source, absent in target) |
| Phantom or un-deleted rows | `extra` key (in target, absent in source) |
| Off-by-one or wrong value in a view | `changed` key (present in both, different content) |
| Half-run backfill | a cluster of `missing` keys in one range |
| Silent schema or encoding drift | `changed` keys across the board |

## How it compares

<p align="center"><img src="docs/img/compare-matrix.svg" width="760" alt="Capability matrix. driftwatch covers continuous, cross-engine, lag-aware, and open source. data-diff and reladiff are cross-engine but one-shot and archived. dbt tests and Great Expectations are open source but batch and single-store. Monte Carlo is scheduled, warehouse-only, and proprietary. pt-table-checksum is continuous and open source but MySQL-only."></p>

What is new here:

- **Lag awareness.** Other diff tools report every not-yet-synced row as a difference.
  driftwatch separates lag from drift, so it does not page you for rows that are simply in
  transit.
- **One hashing contract across engines.** Postgres, Snowflake, and DuckDB each compute the
  same row digest in their own SQL, so a Postgres row and its Snowflake copy compare equal by
  value, not by raw bytes.
- **Pruning.** Matching ranges are skipped without reading their rows, so a clean table is cheap.
- **Pluggable connectors.** New databases ship as separate packages, not core changes.

The gap it fills is real. The closest open tool, `data-diff`, is archived and runs one-shot.
The continuous option, `pt-table-checksum`, is MySQL-only. The cross-engine continuous option,
Monte Carlo, is proprietary and scheduled. None of them is lag-aware.

## Connectors

Built-in: `postgres`, `snowflake`, `duckdb` (local stand-in), `memory` (tests). They resolve
through the `driftwatch.connectors` entry-point group, so you can publish a new connector as its
own package with no change to core.

### Adding a connector

Implement the six-method `driftwatch.connector.Connector` interface, reproduce the hashing
contract (`driftwatch/hashing.py`) in your dialect's SQL, and register an entry point:

```toml
[project.entry-points."driftwatch.connectors"]
clickhouse = "driftwatch_clickhouse:ClickHouseConnector"
```

The conformance test in `tests/test_duckdb_connector.py`, which asserts your connector's digests
match `MemoryConnector` over the same data, is the gate every connector must pass.

## Credit: built on Kleppmann's "trust, but verify"

driftwatch implements a future-work idea from Martin Kleppmann's
[*Designing Data-Intensive Applications*](https://dataintensive.net) (2nd edition, 2026, written
with Chris Riccomini). The final chapter, "Aiming for Correctness," argues that mature data
systems should stop assuming derived data is correct and instead keep verifying their own
integrity. Kleppmann calls this "trust, but verify," and treats reconciliation and auditability
as core concerns rather than afterthoughts.

That idea did not have a good open-source home for the CDC and warehouse era, so teams hand-roll
a one-off reconciliation job. driftwatch is that piece, built on his suggestion: continuous,
cross-engine, lag-aware verification of derived data against its source of truth. The credit for
the idea is Kleppmann and Riccomini's. The implementation is ours.

## Development

```bash
pip install ".[dev,duckdb]" && pytest -q     # 62 passing, 3 skipped (gated live databases)
```

The suite runs fully offline against DuckDB. Postgres conformance runs against a service
container, and Snowflake tests are gated on credentials. A GitHub Actions workflow ships at
[`docs/ci-workflow.yml`](docs/ci-workflow.yml); move it to `.github/workflows/ci.yml` to enable
CI (that path needs a token with the `workflow` scope). The README figures are generated by
[`docs/render_figures.py`](docs/render_figures.py) (run `make figures`).

## Status

Alpha. The engine, connectors, and CLI are tested and working. The always-on daemon, a
Prometheus exporter, and PyPI and Homebrew releases are next. Issues and connector contributions
are welcome.

## License

[Apache-2.0](LICENSE).
