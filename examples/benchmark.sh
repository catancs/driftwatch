#!/usr/bin/env bash
# Boot a throwaway Postgres and run the scale benchmark against it and a DuckDB
# warehouse. Needs Docker and `pip install ".[postgres,duckdb]"`.
#
#   bash examples/benchmark.sh                 # 100k, 1M, 10M rows
#   SIZES=100000,1000000 bash examples/benchmark.sh
#
set -euo pipefail

NAME=driftwatch-bench-pg
PORT=${PORT:-55433}
PY=${PYTHON:-python3}
HERE="$(cd "$(dirname "$0")" && pwd)"

cleanup() { docker rm -f "$NAME" >/dev/null 2>&1 || true; }
trap cleanup EXIT
cleanup

echo "starting Postgres (postgres:16) on port $PORT ..."
docker run -d --rm --name "$NAME" -e POSTGRES_PASSWORD=demo -p "$PORT:5432" postgres:16 >/dev/null

echo -n "waiting for Postgres "
for _ in $(seq 1 60); do
  if docker exec "$NAME" pg_isready -U postgres >/dev/null 2>&1; then echo " ready"; break; fi
  echo -n "."; sleep 1
done

export PG_DSN="postgresql://postgres:demo@localhost:$PORT/postgres"
PYTHONPATH="$HERE/../src" "$PY" "$HERE/benchmark.py"
