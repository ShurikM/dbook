"""FK graph for JOIN path resolution."""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass

from dbook.models import BookMeta


@dataclass
class JoinHop:
    """A single hop in a JOIN path."""

    from_table: str
    from_column: str
    to_table: str
    to_column: str
    description: str  # e.g., "the customer who placed this order"


@dataclass
class JoinPath:
    """A pre-computed JOIN path between two tables."""

    from_table: str
    to_table: str
    hops: list[JoinHop]
    sql: str  # Pre-built JOIN clause


class FKGraph:
    """FK relationship graph with path resolution."""

    def __init__(self, book: BookMeta):
        self.book = book
        # Build adjacency list: table -> [(fk_col, ref_table, ref_col)]
        self._outgoing: dict[str, list[tuple[str, str, str]]] = defaultdict(list)
        self._incoming: dict[str, list[tuple[str, str, str]]] = defaultdict(list)
        self._all_tables: set[str] = set()

        for schema in book.schemas.values():
            for table_name, table in schema.tables.items():
                self._all_tables.add(table_name)
                for fk in table.foreign_keys:
                    for col, ref_col in zip(fk.columns, fk.referred_columns):
                        self._outgoing[table_name].append(
                            (col, fk.referred_table, ref_col)
                        )
                        self._incoming[fk.referred_table].append(
                            (col, table_name, ref_col)
                        )

    def find_path(
        self, from_table: str, to_table: str, max_hops: int = 4
    ) -> JoinPath | None:
        """Find the shortest JOIN path between two tables via FK relationships.

        Uses BFS. Returns None if no path exists within max_hops.
        """
        if from_table == to_table:
            return JoinPath(from_table=from_table, to_table=to_table, hops=[], sql="")

        if from_table not in self._all_tables or to_table not in self._all_tables:
            return None

        # BFS
        visited = {from_table}
        # Each element is a path of (from_t, from_c, to_t, to_c) tuples
        queue: list[list[tuple[str, str, str, str]]] = []

        # Seed with outgoing and incoming edges from the start table
        for col, ref_table, ref_col in self._outgoing.get(from_table, []):
            queue.append([(from_table, col, ref_table, ref_col)])
        for col, src_table, ref_col in self._incoming.get(from_table, []):
            queue.append([(from_table, ref_col, src_table, col)])

        while queue:
            path = queue.pop(0)
            current = path[-1][2]  # last hop's target

            if current == to_table:
                # Build JoinPath
                hops = []
                for from_t, from_c, to_t, to_c in path:
                    hops.append(
                        JoinHop(
                            from_table=from_t,
                            from_column=from_c,
                            to_table=to_t,
                            to_column=to_c,
                            description=_hop_description(from_t, to_t),
                        )
                    )
                sql = _build_join_sql(from_table, hops)
                return JoinPath(
                    from_table=from_table, to_table=to_table, hops=hops, sql=sql
                )

            if current in visited or len(path) >= max_hops:
                continue
            visited.add(current)

            for col, ref_table, ref_col in self._outgoing.get(current, []):
                if ref_table not in visited:
                    queue.append(path + [(current, col, ref_table, ref_col)])
            for col, src_table, ref_col in self._incoming.get(current, []):
                if src_table not in visited:
                    queue.append(path + [(current, ref_col, src_table, col)])

        return None

    def find_all_paths_from(
        self, table: str, max_hops: int = 3
    ) -> list[JoinPath]:
        """Find paths from a table to all reachable tables."""
        paths = []
        for target in sorted(self._all_tables):
            if target != table:
                path = self.find_path(table, target, max_hops)
                if path:
                    paths.append(path)
        return paths

    def get_join_sql(self, tables: list[str]) -> str | None:
        """Generate a JOIN clause connecting multiple tables.

        Finds paths connecting all specified tables into one query.
        """
        if len(tables) < 2:
            return None

        # Start from first table, find paths to all others
        base = tables[0]
        all_hops: list[JoinHop] = []
        joined = {base}

        for target in tables[1:]:
            if target in joined:
                continue
            # Try to find path from any already-joined table
            best_path: JoinPath | None = None
            for source in joined:
                path = self.find_path(source, target, max_hops=3)
                if path and (
                    best_path is None or len(path.hops) < len(best_path.hops)
                ):
                    best_path = path

            if best_path:
                all_hops.extend(best_path.hops)
                for hop in best_path.hops:
                    joined.add(hop.to_table)
            else:
                return None  # Can't connect all tables

        if not all_hops:
            return None

        return _build_join_sql(base, all_hops)

    def to_dict(self) -> dict:
        """Serialize the FK graph for JSON output."""
        result: dict = {
            "tables": sorted(self._all_tables),
            "edges": [],
            "source_tables": sorted(self.source_tables()),
            "leaf_tables": sorted(self.leaf_tables()),
        }
        for table in sorted(self._all_tables):
            for col, ref_table, ref_col in self._outgoing.get(table, []):
                result["edges"].append(
                    {
                        "from_table": table,
                        "from_column": col,
                        "to_table": ref_table,
                        "to_column": ref_col,
                    }
                )
        return result

    def source_tables(self) -> set[str]:
        """Tables with no outgoing FKs (root/reference tables)."""
        return {t for t in self._all_tables if not self._outgoing.get(t)}

    def leaf_tables(self) -> set[str]:
        """Tables with no incoming FKs (leaf/transaction tables)."""
        return {t for t in self._all_tables if not self._incoming.get(t)}


def _hop_description(from_table: str, to_table: str) -> str:
    """Generate a description for a FK hop."""
    _FK_TERMS = {
        "user": "the user/customer",
        "account": "the customer account",
        "order": "the parent order",
        "product": "the product",
        "invoice": "the invoice",
        "payment": "the payment",
        "category": "the category",
        "warehouse": "the warehouse",
        "ticket": "the support ticket",
    }
    for pattern, desc in _FK_TERMS.items():
        if pattern in to_table.lower():
            return desc
    return f"related {to_table}"


def _build_join_sql(base_table: str, hops: list[JoinHop]) -> str:
    """Build a SQL JOIN clause from a list of hops."""
    parts = [f"SELECT * FROM {base_table}"]  # noqa: S608
    for hop in hops:
        parts.append(
            f"JOIN {hop.to_table} ON {hop.from_table}.{hop.from_column}"
            f" = {hop.to_table}.{hop.to_column}"
        )
    return "\n".join(parts)
