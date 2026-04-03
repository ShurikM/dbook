# pyright: reportMissingImports=false
"""Optional embedding-based semantic search for table/column discovery."""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from dbook.models import BookMeta

logger = logging.getLogger(__name__)

_EMBEDDINGS_AVAILABLE = False
_SentenceTransformer: Any = None
_np: Any = None
try:
    from sentence_transformers import SentenceTransformer as _ST
    import numpy as _numpy

    _SentenceTransformer = _ST
    _np = _numpy
    _EMBEDDINGS_AVAILABLE = True
except ImportError:
    pass


@dataclass
class SearchResult:
    """A semantic search result."""

    table: str
    schema: str
    score: float
    match_type: str  # "table_name", "column", "description", "summary"
    matched_text: str


class SemanticIndex:
    """Embedding-based index for semantic table/column search."""

    def __init__(
        self, book: BookMeta, model_name: str = "all-MiniLM-L6-v2",
    ):
        if not _EMBEDDINGS_AVAILABLE:
            raise ImportError(
                "sentence-transformers required for embedding search. "
                "Install with: pip install dbook[embeddings]"
            )

        self.book = book
        self.model = _SentenceTransformer(model_name)
        self._texts: list[str] = []
        self._metadata: list[dict[str, str]] = []
        self._embeddings: Any = None

        self._build_index()

    def _build_index(self) -> None:
        """Build the embedding index from all table/column metadata."""
        for schema_name, schema in self.book.schemas.items():
            for table_name, table in schema.tables.items():
                # Index table name
                self._texts.append(table_name.replace("_", " "))
                self._metadata.append({
                    "table": table_name,
                    "schema": schema_name,
                    "match_type": "table_name",
                    "text": table_name,
                })

                # Index table summary
                if table.summary:
                    self._texts.append(table.summary)
                    self._metadata.append({
                        "table": table_name,
                        "schema": schema_name,
                        "match_type": "summary",
                        "text": table.summary[:100],
                    })

                # Index column names (grouped per table)
                col_names = ", ".join(
                    c.name.replace("_", " ") for c in table.columns
                )
                col_text = f"{table_name}: {col_names}"
                self._texts.append(col_text)
                self._metadata.append({
                    "table": table_name,
                    "schema": schema_name,
                    "match_type": "columns",
                    "text": col_text[:100],
                })

        # Compute embeddings
        if self._texts:
            self._embeddings = self.model.encode(
                self._texts, normalize_embeddings=True,
            )

    def search(self, query: str, top_k: int = 5) -> list[SearchResult]:
        """Search for tables/columns matching a natural language query."""
        if self._embeddings is None or len(self._texts) == 0:
            return []

        query_embedding = self.model.encode(
            [query], normalize_embeddings=True,
        )
        scores = (query_embedding @ self._embeddings.T)[0]

        # Get top-k indices
        top_indices = scores.argsort()[-top_k:][::-1]

        results: list[SearchResult] = []
        seen_tables: set[str] = set()
        for idx in top_indices:
            meta = self._metadata[idx]
            table = meta["table"]
            if table in seen_tables:
                continue
            seen_tables.add(table)

            results.append(SearchResult(
                table=table,
                schema=meta["schema"],
                score=float(scores[idx]),
                match_type=meta["match_type"],
                matched_text=meta["text"],
            ))

        return results

    def save_index(self, path: str | Path) -> None:
        """Save the index for reuse without recomputing embeddings."""
        np = _np
        path = Path(path)
        path.mkdir(parents=True, exist_ok=True)

        np.save(str(path / "embeddings.npy"), self._embeddings)
        with open(path / "metadata.json", "w") as f:
            json.dump(
                {"texts": self._texts, "metadata": self._metadata}, f,
            )

    @classmethod
    def load_index(
        cls,
        path: str | Path,
        model_name: str = "all-MiniLM-L6-v2",
    ) -> SemanticIndex:
        """Load a pre-computed index."""
        np = _np
        path = Path(path)

        idx = cls.__new__(cls)
        idx.model = _SentenceTransformer(model_name)
        idx._embeddings = np.load(str(path / "embeddings.npy"))

        with open(path / "metadata.json") as f:
            data = json.load(f)
        idx._texts = data["texts"]
        idx._metadata = data["metadata"]

        return idx
