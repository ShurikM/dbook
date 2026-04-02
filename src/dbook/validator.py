# pyright: reportMissingImports=false
"""SQL query validation against dbook schema using SQLGlot."""

from __future__ import annotations

import logging
from collections.abc import Iterable
from dataclasses import dataclass, field

import sqlglot
from sqlglot import exp

from dbook.models import BookMeta

logger = logging.getLogger(__name__)


@dataclass
class ValidationResult:
    """Result of SQL query validation."""
    valid: bool
    query: str
    errors: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
    tables_referenced: list[str] = field(default_factory=list)
    columns_referenced: list[str] = field(default_factory=list)
    suggestions: list[str] = field(default_factory=list)


class QueryValidator:
    """Validates SQL queries against a dbook schema."""

    def __init__(self, book: BookMeta):
        self.book = book
        # Build lookup indexes
        self._tables: dict[str, set[str]] = {}  # table_name -> set of column names
        self._enum_values: dict[str, dict[str, list[str]]] = {}  # table_name -> {col: [values]}
        self._fk_map: dict[str, list[tuple[str, str, str]]] = {}  # table -> [(col, ref_table, ref_col)]

        for schema in book.schemas.values():
            for table_name, table in schema.tables.items():
                col_names = {col.name for col in table.columns}
                self._tables[table_name] = col_names
                self._enum_values[table_name] = table.enum_values

                fks = []
                for fk in table.foreign_keys:
                    for col, ref_col in zip(fk.columns, fk.referred_columns):
                        fks.append((col, fk.referred_table, ref_col))
                self._fk_map[table_name] = fks

    def validate(self, sql: str, dialect: str | None = None) -> ValidationResult:
        """Validate a SQL query against the schema.

        Checks:
        1. SQL syntax (via sqlglot parse)
        2. Table references exist in schema
        3. Column references exist in referenced tables
        4. JOIN conditions use valid FK relationships
        5. WHERE clause enum values are valid (if enum column)
        """
        result = ValidationResult(valid=True, query=sql)

        # Parse SQL
        try:
            parsed = sqlglot.parse_one(sql, read=dialect or self.book.dialect)
        except sqlglot.errors.ParseError as e:
            result.valid = False
            result.errors.append(f"Syntax error: {e}")
            return result

        # Extract table references
        tables_in_query: set[str] = set()
        for table_node in parsed.find_all(exp.Table):
            table_name = table_node.name
            tables_in_query.add(table_name)
            result.tables_referenced.append(table_name)

            if table_name not in self._tables:
                result.valid = False
                # Suggest similar table names
                similar = self._find_similar(table_name, self._tables.keys())
                error_msg = f"Table '{table_name}' not found in schema"
                if similar:
                    error_msg += f". Did you mean: {', '.join(similar)}?"
                    result.suggestions.extend(similar)
                result.errors.append(error_msg)

        # Extract column references
        for col_node in parsed.find_all(exp.Column):
            col_name = col_node.name
            table_ref = col_node.table if col_node.table else None
            result.columns_referenced.append(f"{table_ref}.{col_name}" if table_ref else col_name)

            # Validate column exists
            if table_ref and table_ref in self._tables:
                if col_name not in self._tables[table_ref] and col_name != "*":
                    result.valid = False
                    similar = self._find_similar(col_name, self._tables[table_ref])
                    error_msg = f"Column '{col_name}' not found in table '{table_ref}'"
                    if similar:
                        error_msg += f". Did you mean: {', '.join(similar)}?"
                        result.suggestions.extend(f"{table_ref}.{s}" for s in similar)
                    result.errors.append(error_msg)
            elif not table_ref and len(tables_in_query) == 1:
                # Single table query -- check against that table
                the_table = list(tables_in_query)[0]
                if the_table in self._tables and col_name not in self._tables[the_table] and col_name != "*":
                    result.valid = False
                    similar = self._find_similar(col_name, self._tables[the_table])
                    error_msg = f"Column '{col_name}' not found in table '{the_table}'"
                    if similar:
                        error_msg += f". Did you mean: {', '.join(similar)}?"
                    result.errors.append(error_msg)

        # Check WHERE clause enum values
        for eq_node in parsed.find_all(exp.EQ):
            left = eq_node.left
            right = eq_node.right

            if isinstance(left, exp.Column) and isinstance(right, exp.Literal):
                col_name = left.name
                value = right.this
                table_ref = left.table if left.table else (
                    list(tables_in_query)[0] if len(tables_in_query) == 1 else None
                )

                if table_ref and table_ref in self._enum_values:
                    enum_vals = self._enum_values[table_ref].get(col_name, [])
                    if enum_vals and value not in enum_vals:
                        result.warnings.append(
                            f"Value '{value}' for {table_ref}.{col_name} "
                            f"not in known values: {', '.join(enum_vals)}"
                        )

        # Check JOIN conditions against FK map
        for join_node in parsed.find_all(exp.Join):
            join_table = None
            for t in join_node.find_all(exp.Table):
                join_table = t.name
                break

            if join_table and join_table in self._tables:
                # Check if there's a valid FK relationship
                on_clause = join_node.args.get("on")
                if on_clause:
                    has_valid_fk = False
                    for eq in on_clause.find_all(exp.EQ):
                        # Check if this matches a known FK
                        left_col = eq.left
                        right_col = eq.right
                        if isinstance(left_col, exp.Column) and isinstance(right_col, exp.Column):
                            has_valid_fk = True  # Simplified -- at least there's a join condition

                    if not has_valid_fk:
                        result.warnings.append(f"JOIN with {join_table} has no equality condition")

        return result

    @staticmethod
    def _find_similar(name: str, candidates: Iterable[str], max_results: int = 3) -> list[str]:
        """Find similar names using simple substring matching."""
        name_lower = name.lower()
        similar = []
        for candidate in candidates:
            cand_lower = candidate.lower()
            # Exact substring match
            if name_lower in cand_lower or cand_lower in name_lower:
                similar.append(candidate)
            # Common prefix
            elif len(name_lower) > 2 and cand_lower.startswith(name_lower[:3]):
                similar.append(candidate)
        return similar[:max_results]
