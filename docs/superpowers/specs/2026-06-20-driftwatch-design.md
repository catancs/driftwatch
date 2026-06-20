# driftwatch - design spec

**Status:** approved (brainstorm, 2026-06-20). Working name `driftwatch` is a placeholder; renameable.

## Problem

Every "modern data stack" maintains *derived* copies of a source of truth - a warehouse
mirror, a search index, a cache, a read replica, a materialized view - fed by CDC or ETL.
These copies **drift silently**: a dropped event, an off-by-one in incremental view
maintenance, a half-run backfill, a schema change. Today drift is caught by a customer
complaint or an auditor, not by tooling. The open-source category for "is my data correct?"
(data observability) has no good, self-hostable tool that does **continuous, cross-engine
reconciliation of a derived dataset against its source of truth** while correctly tolerating
replication lag. Every CDC guide literally says "write your own reconciliation job."

This maps onto DDIA (2nd ed.) Ch. 13 "Aiming for Correctness" → **"Trust, but Verify."**

## Scope (v1)

- **Pair:** Postgres (source of truth) → data warehouse (derived). First real warehouse
  adapter: **Snowflake**. Local dev/test stand-in: **DuckDB** (free, embeddable, SQL,
  columnar - behaves like a mini-warehouse, zero cloud creds).
- **Runtime:** a **CLI / CI check** wrapping a reusable **engine core**. One command runs one
  comparison, prints a report, exits non-zero on confirmed drift. The always-on **daemon** is
  an explicit later wrapper around the same engine (out of scope for v1).
- **Algorithm:** **recursive hash-segmentation** (the `data-diff`/`reladiff` approach) - the
  one engine also covers full-scan (`fanout=1`) and sampling (early stop) as settings.
- **Lag handling:** **watermark cutoff + recheck confirmation** (two independent safety nets).
- **Packaging:** `pip install driftwatch`, console script, connectors as optional extras and
  entry-point plugins so the community can publish new connectors as separate packages.

Non-goals for v1: the daemon, Prometheus exporter, BigQuery/Redshift/ClickHouse connectors
(community can add via the plugin seam), web UI, auto-repair of drift.

## Architecture

One engine over a single `Connector` abstraction. The engine never knows a SQL dialect exists;
each backend is one implementation of `Connector`. Fragile dialect/hashing code is quarantined
inside connectors + the hashing contract.

```
config.yaml → Config (validate, fail fast)
                 │
                 ▼
              Engine (segmentation, pure logic) ⇄ Connector (interface)
                 │                                   ├ PostgresConnector
        ┌────────┼─────────┐                         ├ SnowflakeConnector
        ▼        ▼         ▼                          ├ DuckDBConnector
   LagHandler  Hashing   Reporter                     └ MemoryConnector (tests)
  (watermark+  contract  (human +
   recheck)              JSON)
                 │
                 ▼
                CLI (thin; daemon later)
```

### Units (one job each)

| Unit | Responsibility | Depends on | Owner |
|---|---|---|---|
| `Connector` (ABC + registry) | `pk_bounds`, `checksum(range,cutoff)→(count,checksum)`, `fetch_row_hashes(range,cutoff)`, `fetch_row_hashes_for_keys(keys)` - native dialect | DB driver | **foundation (hand-written)** |
| Hashing contract | canonical per-type string → md5 → int; order-independent segment aggregate; identical across dialects | - | **foundation** |
| `models` | `DriftKind`, `DriftKey`, `KeyRange`, `Segment`, `DriftReport` | - | **foundation** |
| `Config` | parse+validate YAML; env-interpolated secrets | pyyaml | **foundation** |
| `MemoryConnector` | in-memory reference impl of `Connector` over dicts, using the Python hashing reference | hashing, models | **foundation** |
| Engine + LagHandler | recursive segmentation; watermark cutoff threaded into every query; recheck pass | Connector iface | agent |
| `PostgresConnector` | `Connector` over Postgres (psycopg3) | postgres extra | agent |
| `DuckDBConnector` | `Connector` over DuckDB | duckdb extra | agent |
| `SnowflakeConnector` | `Connector` over Snowflake | snowflake extra | agent |
| `Reporter` | human summary + machine JSON; classify drift | models | agent |
| `CLI` | `driftwatch run -c config.yaml`, `driftwatch init` | engine, config, reporter | agent (integration) |

## The hashing contract (the crux)

- **Per-row hash** = `md5( FIELD_SEP.join(canonical(pk…), canonical(col…)) )`, taken as the
  first 60 bits → int. PK is part of every row hash → all row hashes distinct → safe to
  aggregate with an order-independent op.
- **Segment checksum** = `SUM(row_hash) mod 2**63` - order-independent, so neither side sorts;
  each engine computes it natively and only the digest crosses the wire.
- **`canonical(value)`** (must be reproducible in Postgres, DuckDB, Snowflake, and the Python
  reference): NULL→`\N` sentinel; bool→`0`/`1`; int→base-10; decimal→fixed scale, trailing
  zeros trimmed; float→`%.{FLOAT_PRECISION}g` (KNOWN SHARP EDGE - configurable precision,
  documented as best-effort); timestamp→UTC ISO at microsecond precision; bytes→lowercase hex;
  text→UTF-8 as-is.
- **Source of truth:** the Python reference in `hashing.py`. A **conformance test** asserts each
  connector's SQL produces digests identical to the Python reference over the same dataset.

## Lag handling (option C)

- **Watermark cutoff** captured **once** at run start: `cutoff = min(source_max_wm,
  target_max_wm) − grace` (or `now − grace` if no shared watermark source). Threaded into every
  checksum/count/fetch → run is internally consistent even as data changes; fresh-but-
  unpropagated rows excluded by construction.
- **Recheck pass:** candidate diverging PKs from segmentation are set aside; after a configurable
  delay (+ optional rounds/backoff) re-fetch **only those PKs** on both sides and reclassify.
  Only keys that still diverge are reported as confirmed drift. Bounded to candidates → cheap.

## Error handling (robustness contract)

- **Exit codes:** `0` in-sync, `1` confirmed drift, `2` operational error (conn/query/timeout),
  `3` config error. **Never report in-sync on error.**
- Strictly **read-only** on both sides; runs are **idempotent** → retry/resume safe.
- Transient DB errors → bounded retry + backoff.
- Fail-fast validation: missing PK/column, type mismatch, non-monotonic/missing watermark.
- **Determinism:** single captured cutoff → same input → same verdict.

## Packaging / DX

- `pyproject.toml`, src-layout, typed (`py.typed`), **Apache-2.0**, `requires-python>=3.9`.
- `pip install driftwatch[postgres,snowflake,duckdb]`; core stays tiny.
- Connectors self-register via `driftwatch.connectors` entry points; registry resolves by
  driver name (built-ins + third-party packages).
- Console: `driftwatch run -c config.yaml [--format text|json] [--only NAME]`, `driftwatch init`.
- README quickstart runs **fully locally** (Postgres→DuckDB) in <1 min.
- CI: unit + conformance + e2e on Postgres+DuckDB every push; Snowflake tests **gated on creds**
  (skipped, not failed, when absent).

## Testing

- **Unit:** segmentation over `MemoryConnector`: in-sync, missing, extra, changed, sparse/dense,
  empty, single row, composite PK. Deterministic.
- **Conformance:** same dataset in Postgres + DuckDB (+ Snowflake in CI) → identical segment
  checksums + identical verdicts. Guards the hashing contract.
- **Lag:** warehouse missing fresh rows → within grace = no drift; past grace + unrecovered =
  drift. Recheck: candidate that reconciles after delay is dropped; genuine survives.
- **E2E:** seed Postgres + DuckDB, inject known drift, run CLI, assert exit code + JSON lists
  exactly the injected keys. One runnable end-to-end check.

## Build plan (waves)

1. **Foundation (hand-written, this checkpoint):** scaffold, `models`, `connector` (ABC +
   registry), `hashing` (contract + Python reference), `config`, `MemoryConnector`, pyproject.
2. **Wave 1 (parallel agents, disjoint files):** Engine+LagHandler; PostgresConnector;
   DuckDBConnector; SnowflakeConnector; Reporter. Each builds only against frozen foundation
   contracts; each owns its own file(s) + unit tests → no shared-state conflict.
3. **Integration:** CLI wiring; conformance + lag + e2e tests; README; finalize pyproject.
4. **Verify:** run full suite (Postgres via testcontainer/local + DuckDB); fix until green.
