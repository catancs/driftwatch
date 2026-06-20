"""Declarative config: parse + validate YAML, interpolate ``${ENV}`` secrets, fail fast.

A bad config raises ``ConfigError`` with a precise message (exit code 3 at the CLI),
never a silent mis-compare.
"""

from __future__ import annotations

import os
import re
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

import yaml

from .hashing import DEFAULT_FLOAT_PRECISION

_DURATION_RE = re.compile(r"^\s*(\d+(?:\.\d+)?)\s*([smhd])\s*$")
_ENV_RE = re.compile(r"\$\{([A-Za-z_][A-Za-z0-9_]*)\}")
_DURATION_UNITS = {"s": 1, "m": 60, "h": 3600, "d": 86400}


class ConfigError(Exception):
    """Raised when configuration is missing, malformed, or internally inconsistent."""


@dataclass
class ConnectionConfig:
    driver: str
    params: Dict[str, Any] = field(default_factory=dict)


@dataclass
class RecheckConfig:
    delay_seconds: float = 60.0
    rounds: int = 1


@dataclass
class ComparisonConfig:
    name: str
    source_table: str
    target_table: str
    primary_key: List[str]
    watermark_column: Optional[str] = None
    grace_seconds: float = 0.0
    compare_columns: Optional[List[str]] = None  # None => resolve "*" at runtime
    exclude_columns: List[str] = field(default_factory=list)
    segment_fanout: int = 16
    leaf_size: int = 5000
    float_precision: int = DEFAULT_FLOAT_PRECISION
    recheck: RecheckConfig = field(default_factory=RecheckConfig)


@dataclass
class Config:
    source: ConnectionConfig
    target: ConnectionConfig
    comparisons: List[ComparisonConfig]


def parse_duration(value: Any) -> float:
    """Parse '15m' / '60s' / '2h' / '1d' (or a bare number of seconds) into seconds."""
    if isinstance(value, (int, float)):
        return float(value)
    if not isinstance(value, str):
        raise ConfigError("duration must be a string like '15m' or a number of seconds")
    m = _DURATION_RE.match(value)
    if not m:
        raise ConfigError("invalid duration %r (use forms like '30s', '15m', '2h', '1d')" % value)
    return float(m.group(1)) * _DURATION_UNITS[m.group(2)]


def _interpolate(obj: Any) -> Any:
    """Recursively replace ${ENV_VAR} in string values from the environment."""
    if isinstance(obj, str):
        def repl(match: "re.Match[str]") -> str:
            name = match.group(1)
            if name not in os.environ:
                raise ConfigError("environment variable %r referenced in config is not set" % name)
            return os.environ[name]

        return _ENV_RE.sub(repl, obj)
    if isinstance(obj, dict):
        return {k: _interpolate(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_interpolate(v) for v in obj]
    return obj


def _connection(raw: Any, which: str) -> ConnectionConfig:
    if not isinstance(raw, dict):
        raise ConfigError("connections.%s must be a mapping" % which)
    params = dict(raw)
    driver = params.pop("driver", None)
    if not driver:
        raise ConfigError("connections.%s is missing 'driver'" % which)
    return ConnectionConfig(driver=str(driver), params=params)


def _as_str_list(value: Any, ctx: str) -> List[str]:
    if isinstance(value, str):
        return [value]
    if isinstance(value, list) and all(isinstance(v, str) for v in value):
        return list(value)
    raise ConfigError("%s must be a string or list of strings" % ctx)


def _comparison(raw: Any, idx: int) -> ComparisonConfig:
    if not isinstance(raw, dict):
        raise ConfigError("comparisons[%d] must be a mapping" % idx)
    ctx = "comparisons[%d]" % idx
    try:
        name = str(raw["name"])
        source_table = str(raw["source_table"])
        target_table = str(raw["target_table"])
    except KeyError as e:
        raise ConfigError("%s is missing required key %s" % (ctx, e))

    pk = _as_str_list(raw.get("primary_key"), "%s.primary_key" % ctx) if raw.get("primary_key") \
        else None
    if not pk:
        raise ConfigError("%s.primary_key is required and must be non-empty" % ctx)

    compare_raw = raw.get("compare_columns", "*")
    compare_columns: Optional[List[str]]
    if compare_raw == "*" or compare_raw is None:
        compare_columns = None
    else:
        compare_columns = _as_str_list(compare_raw, "%s.compare_columns" % ctx)

    recheck_raw = raw.get("recheck", {}) or {}
    if not isinstance(recheck_raw, dict):
        raise ConfigError("%s.recheck must be a mapping" % ctx)
    recheck = RecheckConfig(
        delay_seconds=parse_duration(recheck_raw.get("delay", "60s")),
        rounds=int(recheck_raw.get("rounds", 1)),
    )

    cmp = ComparisonConfig(
        name=name,
        source_table=source_table,
        target_table=target_table,
        primary_key=pk,
        watermark_column=(str(raw["watermark_column"]) if raw.get("watermark_column") else None),
        grace_seconds=parse_duration(raw.get("grace", "0s")),
        compare_columns=compare_columns,
        exclude_columns=_as_str_list(raw.get("exclude_columns", []), "%s.exclude_columns" % ctx)
        if raw.get("exclude_columns") else [],
        segment_fanout=int(raw.get("segment_fanout", 16)),
        leaf_size=int(raw.get("leaf_size", 5000)),
        float_precision=int(raw.get("float_precision", DEFAULT_FLOAT_PRECISION)),
        recheck=recheck,
    )
    _validate_comparison(cmp, ctx)
    return cmp


def _validate_comparison(cmp: ComparisonConfig, ctx: str) -> None:
    if cmp.segment_fanout < 2:
        raise ConfigError("%s.segment_fanout must be >= 2" % ctx)
    if cmp.leaf_size < 1:
        raise ConfigError("%s.leaf_size must be >= 1" % ctx)
    if cmp.recheck.rounds < 0:
        raise ConfigError("%s.recheck.rounds must be >= 0" % ctx)
    if cmp.recheck.delay_seconds < 0:
        raise ConfigError("%s.recheck.delay must be >= 0" % ctx)
    if cmp.grace_seconds > 0 and not cmp.watermark_column:
        raise ConfigError(
            "%s sets a grace window but no watermark_column; grace requires a watermark" % ctx
        )


def load_config(path: str) -> Config:
    try:
        with open(path, "r", encoding="utf-8") as f:
            raw = yaml.safe_load(f)
    except FileNotFoundError:
        raise ConfigError("config file not found: %s" % path)
    except yaml.YAMLError as e:
        raise ConfigError("could not parse YAML: %s" % e)

    return load_config_dict(raw)


def load_config_dict(raw: Any) -> Config:
    if not isinstance(raw, dict):
        raise ConfigError("top-level config must be a mapping")
    raw = _interpolate(raw)

    connections = raw.get("connections")
    if not isinstance(connections, dict):
        raise ConfigError("config must have a 'connections' mapping with 'source' and 'target'")
    source = _connection(connections.get("source"), "source")
    target = _connection(connections.get("target"), "target")

    comps_raw = raw.get("comparisons")
    if not isinstance(comps_raw, list) or not comps_raw:
        raise ConfigError("config must have a non-empty 'comparisons' list")
    comparisons = [_comparison(c, i) for i, c in enumerate(comps_raw)]

    seen = set()
    for c in comparisons:
        if c.name in seen:
            raise ConfigError("duplicate comparison name %r" % c.name)
        seen.add(c.name)

    return Config(source=source, target=target, comparisons=comparisons)
