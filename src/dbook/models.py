"""Data models for dbook metadata."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone


@dataclass(frozen=True)
class ColumnInfo:
    """Metadata for a single database column."""
    name: str
    type: str
    nullable: bool
    default: str | None = None
    is_primary_key: bool = False
    comment: str | None = None
    # PII fields — populated by Phase 4 (Presidio)
    pii_type: str | None = None
    pii_confidence: float = 0.0
    sensitivity: str = "none"  # none | low | medium | high


@dataclass(frozen=True)
class ForeignKeyInfo:
    """A foreign key relationship."""
    columns: tuple[str, ...]
    referred_schema: str | None
    referred_table: str
    referred_columns: tuple[str, ...]
    name: str | None = None


@dataclass(frozen=True)
class IndexInfo:
    """An index on a table."""
    name: str | None
    columns: tuple[str, ...]
    unique: bool = False


@dataclass
class TableMeta:
    """Complete metadata for a single database table."""
    name: str
    schema: str | None
    columns: list[ColumnInfo] = field(default_factory=list)
    primary_key: tuple[str, ...] = ()
    foreign_keys: list[ForeignKeyInfo] = field(default_factory=list)
    indexes: list[IndexInfo] = field(default_factory=list)
    row_count: int | None = None
    comment: str | None = None
    sample_data: list[dict] = field(default_factory=list)
    schema_hash: str = ""
    summary: str = ""
    column_purposes: dict[str, str] = field(default_factory=dict)


@dataclass
class SchemaMeta:
    """Metadata for a database schema."""
    name: str
    tables: dict[str, TableMeta] = field(default_factory=dict)
    narrative: str = ""


@dataclass
class BookMeta:
    """Top-level metadata for an entire database book."""
    database_url: str
    dialect: str
    schemas: dict[str, SchemaMeta] = field(default_factory=dict)
    compiled_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    compiler_version: str = "0.1.0"
    mode: str = "base"  # "base" | "pii" | "llm" | "full"
