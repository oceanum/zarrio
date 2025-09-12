"""
Public API for zarrify.
"""

from .core import (
    ZarrConverter,
    convert_to_zarr,
    append_to_zarr
)
from .packing import Packer
from .time import TimeManager
from .config import Config
from .models import (
    ZarrConverterConfig,
    ChunkingConfig,
    PackingConfig,
    CompressionConfig,
    TimeConfig,
    VariableConfig,
    MissingDataConfig
)
from .exceptions import (
    OnzarrError,
    ConversionError,
    PackingError,
    TimeAlignmentError,
    ConfigurationError
)
from .__init__ import __version__, __author__, __email__

__all__ = [
    # Core classes
    "ZarrConverter",
    "Packer",
    "TimeManager",
    "Config",
    
    # Configuration classes
    "ZarrConverterConfig",
    "ChunkingConfig",
    "PackingConfig",
    "CompressionConfig",
    "TimeConfig",
    "VariableConfig",
    "MissingDataConfig",
    
    # Core functions
    "convert_to_zarr",
    "append_to_zarr",
    
    # Exceptions
    "OnzarrError",
    "ConversionError",
    "PackingError",
    "TimeAlignmentError",
    "ConfigurationError",
    
    # Metadata
    "__version__",
    "__author__",
    "__email__",
]