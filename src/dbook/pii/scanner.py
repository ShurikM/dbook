"""PII scanner — detects PII in column names and sample data."""

from __future__ import annotations

import importlib
import logging
from collections import defaultdict
from typing import Any

from dbook.models import BookMeta, ColumnInfo, TableMeta
from dbook.pii.patterns import detect_pii_by_column_name

logger = logging.getLogger(__name__)


def _presidio_available() -> bool:
    """Check whether presidio_analyzer is importable."""
    try:
        importlib.import_module("presidio_analyzer")
    except ImportError:
        return False
    return True


class PIIScanner:
    """Detects PII in database metadata."""

    def __init__(self, use_presidio: bool = True):
        self._has_presidio = _presidio_available()
        self.use_presidio = use_presidio and self._has_presidio
        self._analyzer: Any = None

        if self.use_presidio:
            _mod = importlib.import_module("presidio_analyzer")
            self._analyzer = _mod.AnalyzerEngine()
            logger.info("Presidio analyzer initialized")
        else:
            if use_presidio and not self._has_presidio:
                logger.warning(
                    "Presidio not available. Using column name patterns only. "
                    "Install with: pip install dbook[pii]"
                )

    def scan_table(self, table: TableMeta) -> TableMeta:
        """Scan a table's columns and sample data for PII.

        Returns a new TableMeta with updated PII annotations on columns
        and redacted sample data.
        """
        new_columns = []
        pii_columns: dict[str, tuple[str, float, str]] = {}

        for col in table.columns:
            # Layer 1: Column name pattern matching
            pii_type, confidence, sensitivity = detect_pii_by_column_name(col.name)

            if pii_type:
                pii_columns[col.name] = (pii_type, confidence, sensitivity)
                new_columns.append(ColumnInfo(
                    name=col.name,
                    type=col.type,
                    nullable=col.nullable,
                    default=col.default,
                    is_primary_key=col.is_primary_key,
                    comment=col.comment,
                    pii_type=pii_type,
                    pii_confidence=confidence,
                    sensitivity=sensitivity,
                ))
            else:
                new_columns.append(col)

        # Layer 2: Presidio on sample data (if available)
        if self.use_presidio and self._analyzer and table.sample_data:
            presidio_detections = self._scan_sample_data(table.sample_data, table.columns)

            # Merge Presidio detections with column name detections
            for col_name, (pii_type, confidence, sensitivity) in presidio_detections.items():
                if col_name not in pii_columns:
                    # New PII found by Presidio that column name didn't catch
                    pii_columns[col_name] = (pii_type, confidence, sensitivity)
                    # Update the column in new_columns
                    for i, col in enumerate(new_columns):
                        if col.name == col_name:
                            new_columns[i] = ColumnInfo(
                                name=col.name,
                                type=col.type,
                                nullable=col.nullable,
                                default=col.default,
                                is_primary_key=col.is_primary_key,
                                comment=col.comment,
                                pii_type=pii_type,
                                pii_confidence=confidence,
                                sensitivity=sensitivity,
                            )
                            break
                else:
                    # Both detected — use higher confidence
                    existing = pii_columns[col_name]
                    if confidence > existing[1]:
                        pii_columns[col_name] = (pii_type, confidence, sensitivity)
                        for i, col in enumerate(new_columns):
                            if col.name == col_name:
                                new_columns[i] = ColumnInfo(
                                    name=col.name,
                                    type=col.type,
                                    nullable=col.nullable,
                                    default=col.default,
                                    is_primary_key=col.is_primary_key,
                                    comment=col.comment,
                                    pii_type=pii_type,
                                    pii_confidence=confidence,
                                    sensitivity=sensitivity,
                                )
                                break

        # Redact sample data for all detected PII columns
        redacted_samples = _redact_sample_data(table.sample_data, pii_columns)

        # Return updated table
        table.columns = new_columns
        table.sample_data = redacted_samples
        return table

    def _scan_sample_data(
        self,
        sample_data: list[dict],
        columns: list[ColumnInfo],
    ) -> dict[str, tuple[str, float, str]]:
        """Run Presidio on sample data values to detect PII."""
        if not self._analyzer:
            return {}

        detections: dict[str, dict[str, list[float]]] = defaultdict(lambda: defaultdict(list))

        for row in sample_data:
            for col_name, value in row.items():
                if value is None or not isinstance(value, str):
                    # Try converting to string for analysis
                    if value is not None:
                        value = str(value)
                    else:
                        continue

                if not value.strip():
                    continue

                results = self._analyzer.analyze(text=value, language="en")
                for result in results:
                    detections[col_name][result.entity_type].append(result.score)

        # Aggregate: for each column, take the entity type with highest avg confidence
        pii_results: dict[str, tuple[str, float, str]] = {}
        for col_name, entity_scores in detections.items():
            best_type = None
            best_confidence = 0.0
            for entity_type, scores in entity_scores.items():
                avg_score = sum(scores) / len(scores)
                if avg_score > best_confidence:
                    best_type = entity_type
                    best_confidence = avg_score

            if best_type and best_confidence >= 0.5:
                sensitivity = _sensitivity_from_type(best_type)
                pii_results[col_name] = (best_type, best_confidence, sensitivity)

        return pii_results


def scan_book(book: BookMeta, use_presidio: bool = True) -> BookMeta:
    """Scan an entire BookMeta for PII. Modifies in place and returns it."""
    scanner = PIIScanner(use_presidio=use_presidio)

    total_pii = 0
    for schema in book.schemas.values():
        for table in schema.tables.values():
            scanner.scan_table(table)
            pii_count = sum(1 for col in table.columns if col.pii_type)
            total_pii += pii_count

    book.mode = "pii" if book.mode == "base" else "full"
    logger.info(f"PII scan complete: {total_pii} PII columns detected")
    return book


def _redact_sample_data(
    sample_data: list[dict],
    pii_columns: dict[str, tuple[str, float, str]],
) -> list[dict]:
    """Redact PII values in sample data rows."""
    if not pii_columns:
        return sample_data

    redacted = []
    for row in sample_data:
        new_row = {}
        for col_name, value in row.items():
            if col_name in pii_columns:
                pii_type = pii_columns[col_name][0]
                new_row[col_name] = f"[REDACTED:{pii_type}]"
            else:
                new_row[col_name] = value
        redacted.append(new_row)
    return redacted


def _sensitivity_from_type(pii_type: str) -> str:
    """Map Presidio entity type to sensitivity level."""
    critical = {"US_SSN", "CREDIT_CARD", "US_PASSPORT", "UK_NHS", "US_ITIN"}
    high = {"EMAIL_ADDRESS", "PHONE_NUMBER", "DATE_TIME", "PERSON", "LOCATION", "NRP"}
    medium = {"IP_ADDRESS", "URL", "US_DRIVER_LICENSE"}

    if pii_type in critical:
        return "critical"
    elif pii_type in high:
        return "high"
    elif pii_type in medium:
        return "medium"
    return "low"
