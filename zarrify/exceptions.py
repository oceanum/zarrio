"""
Custom exceptions for zarrify.
"""

class OnzarrError(Exception):
    """Base exception for zarrify."""
    pass


class ConversionError(OnzarrError):
    """Raised when data conversion fails."""
    pass


class PackingError(OnzarrError):
    """Raised when data packing fails."""
    pass


class TimeAlignmentError(OnzarrError):
    """Raised when time alignment fails."""
    pass


class ConfigurationError(OnzarrError):
    """Raised when configuration is invalid."""
    pass


class RetryLimitExceededError(OnzarrError):
    """Raised when retry limit is exceeded."""
    pass