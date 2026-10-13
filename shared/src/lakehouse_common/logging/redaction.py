"""Redaction for anything logged, persisted or sent: secrets and personal e-mail addresses.

Moved from ``observability.runs`` (still exported there): one definition for error messages,
structured logs and alerts (skill ``security``: no tokens, credentials or PII in logs).
"""

import re
from typing import Final

_SENSITIVE_KEY: Final = (
    r"[A-Za-z0-9_-]*(?:password|passwd|pwd|token|secret|api[_-]?key)[A-Za-z0-9_-]*"
)
# (pattern, replacement), applied in order: assignments and query parameters
# (``access_token=...&page=2`` keeps ``&page=2``), authorization headers, the password of
# ``scheme://user:password@host`` URLs, then e-mail addresses (personal data).
_REDACTIONS: Final = (
    (re.compile(rf"(?i)\b({_SENSITIVE_KEY})(\s*[=:]\s*)[^\s&;,'\"]+"), r"\1\2***"),
    (re.compile(r"(?i)\b(bearer|basic)(\s+)[A-Za-z0-9._~+/=-]+"), r"\1\2***"),
    (re.compile(r"(?i)(\b[a-z][a-z0-9+.-]*://[^/\s:@]+:)[^/\s@]+(@)"), r"\1***\2"),
    (re.compile(r"\b[A-Za-z0-9._%+-]+@[A-Za-z0-9-]+(?:\.[A-Za-z0-9-]+)+\b"), "***@***"),
)


def mask_secrets(text: str) -> str:
    """``text`` with secret-looking values and e-mail addresses masked: for anything logged,
    persisted or sent (error messages, log lines, alerts, request URLs)."""
    for pattern, replacement in _REDACTIONS:
        text = pattern.sub(replacement, text)
    return text
