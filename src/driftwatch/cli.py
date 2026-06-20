"""Command-line interface.

`driftwatch run -c config.yaml` runs each comparison and exits with the worst code:
0 in-sync, 1 confirmed drift, 2 operational error, 3 config error.
`driftwatch init` prints a starter config.

This is a thin wrapper: it resolves connectors from the registry, hands each comparison
to the engine, and renders via the reporter. The always-on daemon (future) is just a
scheduler loop around this same `run` path.
"""

from __future__ import annotations

import argparse
import sys
from typing import List, Optional

from .config import ConfigError, ConnectionConfig, load_config
from .connector import get_connector_class
from .engine import compare
from .models import DriftReport
from .reporter import exit_code, render_json, render_text

SAMPLE_CONFIG = """\
# driftwatch config - reconcile a derived dataset against its source of truth.
# Secrets come from the environment via ${VAR}; never put credentials in this file.
connections:
  source:
    driver: postgres
    dsn: ${PG_DSN}              # e.g. postgresql://user@host:5432/app
  target:
    driver: snowflake
    account: ${SNOWFLAKE_ACCOUNT}
    user: ${SNOWFLAKE_USER}
    password: ${SNOWFLAKE_PASSWORD}
    warehouse: COMPUTE_WH
    database: ANALYTICS
    schema: PUBLIC

comparisons:
  - name: orders
    source_table: public.orders
    target_table: ANALYTICS.PUBLIC.ORDERS
    primary_key: [id]
    watermark_column: updated_at   # only compare rows older than the grace window
    grace: 15m                      # so warehouse lag is not reported as drift
    compare_columns: "*"            # or an explicit list; exclude_columns also supported
    recheck:
      delay: 60s                    # re-check candidate drift after this delay
      rounds: 1
"""


def _make_connector(conn: ConnectionConfig):
    cls = get_connector_class(conn.driver)
    return cls(**conn.params)


def cmd_run(args: argparse.Namespace) -> int:
    try:
        cfg = load_config(args.config)
    except ConfigError as e:
        print("config error: %s" % e, file=sys.stderr)
        return 3

    comparisons = cfg.comparisons
    if args.only:
        wanted = set(args.only)
        comparisons = [c for c in comparisons if c.name in wanted]
        missing = wanted - {c.name for c in comparisons}
        if missing:
            print("config error: --only names not found: %s" % ", ".join(sorted(missing)),
                  file=sys.stderr)
            return 3

    source = None
    target = None
    worst = 0
    try:
        try:
            source = _make_connector(cfg.source)
            target = _make_connector(cfg.target)
        except Exception as e:  # noqa: BLE001 - connection setup failure is operational
            print("error: could not connect: %s" % e, file=sys.stderr)
            return 2

        for cmp in comparisons:
            try:
                report = compare(source, target, cmp)
            except Exception as e:  # noqa: BLE001 - per-comparison operational error
                # one comparison failing must not abort the others, and must never be
                # silently reported as in-sync.
                report = DriftReport(comparison=cmp.name, in_sync=False, error=str(e))
            if args.format == "json":
                print(render_json(report))
            else:
                print(render_text(report))
            worst = max(worst, exit_code(report))
        return worst
    finally:
        for c in (source, target):
            if c is not None:
                try:
                    c.close()
                except Exception:  # noqa: BLE001 - cleanup must not mask the real result
                    pass


def cmd_init(args: argparse.Namespace) -> int:
    sys.stdout.write(SAMPLE_CONFIG)
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="driftwatch",
        description="Continuous, cross-engine reconciliation of derived data vs. its source of truth.",
    )
    sub = parser.add_subparsers(dest="command")

    p_run = sub.add_parser("run", help="run reconciliation from a config file")
    p_run.add_argument("-c", "--config", required=True, help="path to the YAML config")
    p_run.add_argument("--format", choices=["text", "json"], default="text",
                       help="output format (default: text)")
    p_run.add_argument("--only", action="append", metavar="NAME",
                       help="run only this comparison (repeatable)")
    p_run.set_defaults(func=cmd_run)

    p_init = sub.add_parser("init", help="print a starter config to stdout")
    p_init.set_defaults(func=cmd_init)

    return parser


def main(argv: Optional[List[str]] = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    if not getattr(args, "func", None):
        parser.print_help()
        return 2
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
