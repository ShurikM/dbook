"""FK-based lineage tracking — show data flow across tables."""

from __future__ import annotations

from dbook.models import BookMeta, SchemaMeta


def generate_lineage(book: BookMeta) -> str:
    """Generate a lineage section showing data flow across the database.

    Builds a directed graph from FK relationships and outputs it as markdown.
    Shows: source tables (no incoming FKs) -> intermediate -> leaf tables (no outgoing FKs).
    """
    # Build adjacency graph
    # edge: table_A --fk_col--> table_B means A references B
    edges: list[tuple[str, str, str]] = []  # (from_table, to_table, via_column)
    all_tables: set[str] = set()
    referenced_by: dict[str, list[str]] = {}  # table -> [tables that reference it]
    references: dict[str, list[str]] = {}  # table -> [tables it references]

    for schema in book.schemas.values():
        for table_name, table in schema.tables.items():
            all_tables.add(table_name)
            references.setdefault(table_name, [])
            referenced_by.setdefault(table_name, [])

            for fk in table.foreign_keys:
                ref_table = fk.referred_table
                col = ", ".join(fk.columns)
                edges.append((table_name, ref_table, col))
                references[table_name].append(ref_table)
                referenced_by.setdefault(ref_table, []).append(table_name)

    if not edges:
        return ""

    # Classify tables
    root_tables = sorted(
        [t for t in all_tables if not references.get(t)]
    )  # No outgoing FKs (referenced by others)
    leaf_tables = sorted(
        [t for t in all_tables if not referenced_by.get(t)]
    )  # No incoming FKs
    intermediate_tables = sorted(
        all_tables - set(root_tables) - set(leaf_tables)
    )

    lines: list[str] = []
    lines.append("## Data Lineage")
    lines.append("")

    # Root tables (data sources — other tables depend on them)
    if root_tables:
        lines.append(
            "**Source tables** (no dependencies, other tables reference these):"
        )
        for t in root_tables:
            incoming = referenced_by.get(t, [])
            if incoming:
                lines.append(
                    f"- `{t}` <- referenced by: "
                    f"{', '.join(sorted(set(incoming)))}"
                )
            else:
                lines.append(f"- `{t}`")
        lines.append("")

    # Intermediate tables (both reference and are referenced)
    if intermediate_tables:
        lines.append(
            "**Intermediate tables** (reference other tables and are referenced):"
        )
        for t in intermediate_tables:
            refs_out = sorted(set(references.get(t, [])))
            refs_in = sorted(set(referenced_by.get(t, [])))
            lines.append(
                f"- `{t}` -> depends on: {', '.join(refs_out)} "
                f"| <- used by: {', '.join(refs_in)}"
            )
        lines.append("")

    # Leaf tables (depend on others, nothing depends on them)
    if leaf_tables:
        lines.append(
            "**Leaf tables** (depend on other tables, nothing references these):"
        )
        for t in leaf_tables:
            refs_out = sorted(set(references.get(t, [])))
            if refs_out:
                lines.append(
                    f"- `{t}` -> depends on: {', '.join(refs_out)}"
                )
            else:
                lines.append(f"- `{t}` (standalone)")
        lines.append("")

    # Data flow chains (follow FK paths from leaf to root)
    chains = _find_data_chains(edges, root_tables, all_tables)
    if chains:
        lines.append("**Data flow chains:**")
        for chain in chains[:10]:  # Cap at 10 chains
            lines.append(f"- {' -> '.join(chain)}")
        lines.append("")

    return "\n".join(lines)


def generate_schema_lineage(schema: SchemaMeta) -> str:
    """Generate a lineage section for a single schema's _manifest.md.

    Only includes relationships where both tables are within the schema.
    """
    edges: list[tuple[str, str, str]] = []
    all_tables: set[str] = set()
    referenced_by: dict[str, list[str]] = {}
    references: dict[str, list[str]] = {}

    schema_table_names = set(schema.tables.keys())

    for table_name, table in schema.tables.items():
        all_tables.add(table_name)
        references.setdefault(table_name, [])
        referenced_by.setdefault(table_name, [])

        for fk in table.foreign_keys:
            ref_table = fk.referred_table
            # Only include edges where the referred table is in this schema
            if ref_table not in schema_table_names:
                continue
            col = ", ".join(fk.columns)
            edges.append((table_name, ref_table, col))
            references[table_name].append(ref_table)
            referenced_by.setdefault(ref_table, []).append(table_name)

    if not edges:
        return ""

    # Classify tables
    root_tables = sorted(
        [t for t in all_tables if not references.get(t)]
    )
    leaf_tables = sorted(
        [t for t in all_tables if not referenced_by.get(t)]
    )
    intermediate_tables = sorted(
        all_tables - set(root_tables) - set(leaf_tables)
    )

    lines: list[str] = []
    lines.append("## Data Lineage")
    lines.append("")

    if root_tables:
        lines.append(
            "**Source tables** (no dependencies, other tables reference these):"
        )
        for t in root_tables:
            incoming = referenced_by.get(t, [])
            if incoming:
                lines.append(
                    f"- `{t}` <- referenced by: "
                    f"{', '.join(sorted(set(incoming)))}"
                )
            else:
                lines.append(f"- `{t}`")
        lines.append("")

    if intermediate_tables:
        lines.append(
            "**Intermediate tables** (reference other tables and are referenced):"
        )
        for t in intermediate_tables:
            refs_out = sorted(set(references.get(t, [])))
            refs_in = sorted(set(referenced_by.get(t, [])))
            lines.append(
                f"- `{t}` -> depends on: {', '.join(refs_out)} "
                f"| <- used by: {', '.join(refs_in)}"
            )
        lines.append("")

    if leaf_tables:
        lines.append(
            "**Leaf tables** (depend on other tables, nothing references these):"
        )
        for t in leaf_tables:
            refs_out = sorted(set(references.get(t, [])))
            if refs_out:
                lines.append(
                    f"- `{t}` -> depends on: {', '.join(refs_out)}"
                )
            else:
                lines.append(f"- `{t}` (standalone)")
        lines.append("")

    return "\n".join(lines)


def _find_data_chains(
    edges: list[tuple[str, str, str]],
    root_tables: list[str],
    all_tables: set[str],
    max_depth: int = 5,
) -> list[list[str]]:
    """Find data flow chains by following FK references from important root tables."""
    # Build reverse adjacency: ref_table -> [tables that reference it]
    reverse_adj: dict[str, list[str]] = {}
    for from_t, to_t, _ in edges:
        reverse_adj.setdefault(to_t, []).append(from_t)

    chains: list[list[str]] = []
    for root in root_tables[:5]:  # Top 5 root tables
        # BFS from root following reverse edges (who references me?)
        visited = {root}
        queue = [[root]]

        while queue:
            path = queue.pop(0)
            current = path[-1]

            if len(path) > max_depth:
                continue

            children = reverse_adj.get(current, [])
            if not children and len(path) > 1:
                # End of chain
                chains.append(list(reversed(path)))  # Reverse: show leaf -> root

            for child in sorted(set(children)):
                if child not in visited:
                    visited.add(child)
                    queue.append(path + [child])

    # Deduplicate and sort by length (longest chains first)
    seen: set[str] = set()
    unique_chains: list[list[str]] = []
    for chain in sorted(chains, key=len, reverse=True):
        key = " -> ".join(chain)
        if key not in seen:
            seen.add(key)
            unique_chains.append(chain)

    return unique_chains[:10]
