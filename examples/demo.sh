#!/usr/bin/env bash
# Boot a throwaway Postgres, run the cross-engine demo against it and a DuckDB
# warehouse, then clean up. Needs Docker, and `pip install ".[postgres,duckdb]"`.
#
#   bash examples/demo.sh
#
set -euo pipefail

NAME=driftwatch-demo-pg
PORT=${PORT:-55432}
PY=${PYTHON:-python3}
HERE="$(cd "$(dirname "$0")" && pwd)"

cleanup() { docker rm -f "$NAME" >/dev/null 2>&1 || true; }
trap cleanup EXIT
cleanup

echo "starting Postgres (postgres:16) on port $PORT ..."
docker run -d --rm --name "$NAME" -e POSTGRES_PASSWORD=demo -p "$PORT:5432" postgres:16 >/dev/null

echo -n "waiting for Postgres to accept connections "
for _ in $(seq 1 60); do
  if docker exec "$NAME" pg_isready -U postgres >/dev/null 2>&1; then echo " ready"; break; fi
  echo -n "."; sleep 1
done

export PG_DSN="postgresql://postgres:demo@localhost:$PORT/postgres"
PYTHONPATH="$HERE/../src" "$PY" "$HERE/demo.py"
