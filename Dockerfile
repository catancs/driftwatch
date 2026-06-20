FROM python:3.12-slim

WORKDIR /app

# Install the package with the common connectors. Copy only what the build needs.
COPY pyproject.toml README.md LICENSE ./
COPY src ./src
RUN pip install --no-cache-dir ".[postgres,snowflake,duckdb]"

# driftwatch is the entrypoint, so `docker run <image> run -c ...` works directly.
ENTRYPOINT ["driftwatch"]
CMD ["--help"]
