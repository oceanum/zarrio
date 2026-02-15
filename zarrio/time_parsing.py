"""Time parsing utilities for rolling archive."""

from datetime import datetime
from typing import Optional, List

DEFAULT_TIMESTAMP_FORMATS = [
    "%Y%m%dT%H%M%S",  # 20240101T000000
    "%Y-%m-%dT%H:%M:%S",  # 2024-01-01T00:00:00
    "%Y%m%d%H%M%S",  # 20240101000000
]


def parse_timestamp_from_string(
    value: str, formats: Optional[List[str]] = None
) -> Optional[datetime]:
    """
    Parse timestamp from string using multiple format attempts.

    Args:
        value: String to parse
        formats: Optional list of strptime formats to try

    Returns:
        Parsed datetime or None if unparseable
    """
    formats = formats or DEFAULT_TIMESTAMP_FORMATS

    for fmt in formats:
        try:
            return datetime.strptime(value, fmt)
        except ValueError:
            continue

    # Fallback to dateutil
    return parse_with_dateutil(value)


def extract_timestamp_from_group_name(group_path: str) -> Optional[datetime]:
    """
    Extract timestamp from the last segment of a group path.

    Example:
        "cycle/20240101T000000" -> datetime(2024, 1, 1, 0, 0, 0)
        "forecast/2024-01-01T06:00:00" -> datetime(2024, 1, 1, 6, 0, 0)

    Args:
        group_path: Group path (e.g., "cycle/20240101T000000")

    Returns:
        Parsed datetime or None if unparseable
    """
    # Extract last segment
    last_segment = group_path.split("/")[-1]
    return parse_timestamp_from_string(last_segment)


def parse_with_dateutil(value: str) -> Optional[datetime]:
    """
    Fallback parsing using dateutil parser.

    Args:
        value: String to parse

    Returns:
        Parsed datetime or None if unparseable
    """
    try:
        from dateutil import parser

        return parser.parse(value)
    except (ImportError, ValueError):
        return None
