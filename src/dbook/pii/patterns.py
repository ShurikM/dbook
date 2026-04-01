"""PII column name patterns — regex-based detection without external deps."""

from __future__ import annotations

import re

# (regex_pattern, pii_type, confidence, sensitivity_level)
PII_COLUMN_PATTERNS: list[tuple[str, str, float, str]] = [
    # Critical
    (r"(?i)\b(ssn|social_security|social_sec)\b", "SSN", 0.95, "critical"),
    (r"(?i)\b(credit_card|card_number|cc_num|card_num)\b", "CREDIT_CARD", 0.95, "critical"),
    (r"(?i)\b(passport|passport_num|passport_number)\b", "PASSPORT", 0.95, "critical"),
    (r"(?i)\b(tax_id|tin|tax_number)\b", "TAX_ID", 0.90, "critical"),

    # High
    (r"(?i)\b(email|e_mail|email_address|contact_email)\b", "EMAIL", 0.90, "high"),
    (r"(?i)\b(phone|mobile|cell|telephone|phone_number)\b", "PHONE", 0.90, "high"),
    (r"(?i)\b(date_of_birth|dob|birth_date|birthday)\b", "DATE_OF_BIRTH", 0.90, "high"),
    (r"(?i)\b(first_name|last_name|full_name|surname|given_name)\b", "PERSON", 0.85, "high"),
    (r"(?i)\b(address|street_address|home_address|mailing_address)\b", "ADDRESS", 0.80, "high"),
    (r"(?i)\b(zip|zip_code|postal|postal_code)\b", "ADDRESS", 0.70, "high"),

    # Medium
    (r"(?i)\b(ip_address|ip_addr|remote_ip|client_ip)\b", "IP_ADDRESS", 0.80, "medium"),
    (r"(?i)\b(user_agent)\b", "USER_AGENT", 0.70, "medium"),
    (r"(?i)\b(name)\b", "PERSON", 0.60, "medium"),

    # Low
    (r"(?i)\b(device_id|device_identifier)\b", "DEVICE_ID", 0.60, "low"),
    (r"(?i)\b(card_last_four|last_four|card_suffix)\b", "CREDIT_CARD_PARTIAL", 0.70, "low"),
]


def detect_pii_by_column_name(column_name: str) -> tuple[str | None, float, str]:
    """Check if a column name matches known PII patterns.

    Returns (pii_type, confidence, sensitivity) or (None, 0.0, "none").
    """
    for pattern, pii_type, confidence, sensitivity in PII_COLUMN_PATTERNS:
        if re.search(pattern, column_name):
            return pii_type, confidence, sensitivity
    return None, 0.0, "none"
