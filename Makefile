.PHONY: install dev test run docker figures

install:   ## install with the common connectors
	pip install ".[postgres,snowflake,duckdb]"

dev:       ## install dev + duckdb for the offline test suite
	pip install ".[dev,duckdb]"

test:      ## run the test suite
	pytest -q

run:       ## run the reconciliation defined in driftwatch.yaml
	driftwatch run -c driftwatch.yaml

docker:    ## build the container image
	docker build -t driftwatch .

figures:   ## regenerate the README figures (needs matplotlib)
	python3 docs/render_figures.py

demo:      ## run the live Postgres + DuckDB demo (needs Docker)
	bash examples/demo.sh

bench:     ## run the scale benchmark, 100k/1M/10M rows (needs Docker)
	bash examples/benchmark.sh
