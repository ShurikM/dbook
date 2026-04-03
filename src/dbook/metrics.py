"""User-defined metric definitions support."""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from pathlib import Path

logger = logging.getLogger(__name__)


@dataclass
class MetricDefinition:
    """A canonical business metric definition."""
    name: str
    sql: str
    description: str
    tables: list[str] = field(default_factory=list)


def load_metrics(metrics_path: str | Path) -> list[MetricDefinition]:
    """Load metric definitions from a YAML file.

    Returns empty list if file doesn't exist or yaml not installed.
    """
    path = Path(metrics_path)
    if not path.exists():
        return []

    try:
        import yaml  # type: ignore[import-untyped]
    except ImportError:
        logger.warning("PyYAML not installed. Install with: pip install pyyaml")
        # Fallback: try to parse simple YAML-like format manually
        return _parse_simple_yaml(path)

    try:
        with open(path) as f:
            data = yaml.safe_load(f)

        if not data or "metrics" not in data:
            return []

        metrics = []
        for name, definition in data["metrics"].items():
            metrics.append(MetricDefinition(
                name=name,
                sql=definition.get("sql", ""),
                description=definition.get("description", ""),
                tables=definition.get("tables", []),
            ))
        return metrics
    except Exception as e:
        logger.warning(f"Failed to load metrics from {path}: {e}")
        return []


def _parse_simple_yaml(path: Path) -> list[MetricDefinition]:
    """Simple YAML parser fallback for when PyYAML isn't installed."""
    try:
        content = path.read_text()
        metrics: list[MetricDefinition] = []
        current_name: str | None = None
        current: dict[str, str | list[str]] = {}

        for line in content.split("\n"):
            stripped = line.strip()
            if not stripped or stripped.startswith("#"):
                continue

            # Detect metric name (indented under "metrics:")
            if stripped == "metrics:":
                continue

            indent = len(line) - len(line.lstrip())

            if indent == 2 and stripped.endswith(":"):
                # Save previous metric
                if current_name and current:
                    tables_val = current.get("tables", [])
                    tables_list = tables_val if isinstance(tables_val, list) else []
                    metrics.append(MetricDefinition(
                        name=current_name,
                        sql=str(current.get("sql", "")),
                        description=str(current.get("description", "")),
                        tables=tables_list,
                    ))
                current_name = stripped[:-1]
                current = {}
            elif indent == 4 and ":" in stripped:
                key, _, value = stripped.partition(":")
                value = value.strip().strip('"').strip("'")
                if key.strip() == "tables":
                    # Parse simple list: ["a", "b"]
                    value = value.strip("[]")
                    current["tables"] = [
                        v.strip().strip('"').strip("'")
                        for v in value.split(",")
                        if v.strip()
                    ]
                else:
                    current[key.strip()] = value

        # Save last metric
        if current_name and current:
            tables_val = current.get("tables", [])
            tables_list = tables_val if isinstance(tables_val, list) else []
            metrics.append(MetricDefinition(
                name=current_name,
                sql=str(current.get("sql", "")),
                description=str(current.get("description", "")),
                tables=tables_list,
            ))

        return metrics
    except Exception:
        return []
