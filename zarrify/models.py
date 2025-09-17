"""
Pydantic models for zarrify configuration and data validation.
"""

from typing import Dict, Optional, List, Union, Any
from pathlib import Path
import yaml
import json
from datetime import datetime

from pydantic import BaseModel, Field, field_validator, ConfigDict


class ChunkingConfig(BaseModel):
    """Configuration for data chunking."""
    model_config = ConfigDict(extra="allow")
    
    time: Optional[int] = Field(None, description="Chunk size for time dimension")
    lat: Optional[int] = Field(None, description="Chunk size for latitude dimension")
    lon: Optional[int] = Field(None, description="Chunk size for longitude dimension")
    depth: Optional[int] = Field(None, description="Chunk size for depth dimension")


class PackingConfig(BaseModel):
    """Configuration for data packing."""
    enabled: bool = Field(False, description="Whether to enable data packing")
    bits: int = Field(16, description="Number of bits for packing", ge=8, le=32)
    manual_ranges: Optional[Dict[str, Dict[str, float]]] = Field(
        None, 
        description="Manual min/max ranges for variables (e.g., {'temperature': {'min': 0, 'max': 100}})"
    )
    auto_buffer_factor: float = Field(
        0.01, 
        description="Buffer factor for automatically calculated ranges (e.g., 0.01 = 1% buffer)", 
        ge=0.0
    )
    check_range_exceeded: bool = Field(
        True, 
        description="Whether to check if data exceeds specified ranges"
    )
    range_exceeded_action: str = Field(
        "warn", 
        description="Action when data exceeds range ('warn', 'error', 'ignore')"
    )
    
    @field_validator("bits")
    @classmethod
    def validate_bits(cls, v: int) -> int:
        if v not in [8, 16, 32]:
            raise ValueError("bits must be one of 8, 16, or 32")
        return v
    
    @field_validator("range_exceeded_action")
    @classmethod
    def validate_range_exceeded_action(cls, v: str) -> str:
        if v not in ["warn", "error", "ignore"]:
            raise ValueError("range_exceeded_action must be one of 'warn', 'error', 'ignore'")
        return v


class CompressionConfig(BaseModel):
    """Configuration for data compression."""
    method: Optional[str] = Field(None, description="Compression method (e.g., 'blosc:zstd:3')")
    cname: str = Field("zstd", description="Compression algorithm name")
    clevel: int = Field(1, description="Compression level", ge=0, le=9)
    shuffle: str = Field("shuffle", description="Shuffle type")


class TimeConfig(BaseModel):
    """Configuration for time handling."""
    dim: str = Field("time", description="Name of the time dimension")
    append_dim: str = Field("time", description="Dimension to append along")
    global_start: Optional[Union[str, datetime]] = Field(None, description="Global start time")
    global_end: Optional[Union[str, datetime]] = Field(None, description="Global end time")
    freq: Optional[str] = Field(None, description="Time frequency")


class VariableConfig(BaseModel):
    """Configuration for variable handling."""
    include: Optional[List[str]] = Field(None, description="Variables to include")
    exclude: Optional[List[str]] = Field(None, description="Variables to exclude")


class MissingDataConfig(BaseModel):
    """Configuration for missing data handling."""
    check_vars: Optional[Union[str, List[str]]] = Field(
        "all", 
        description="Data variables to check for missing values ('all', None, or list)"
    )
    retries_on_missing: int = Field(
        0, 
        description="Number of retries if missing values are encountered", 
        ge=0
    )
    missing_check_vars: Optional[Union[str, List[str]]] = Field(
        "all",
        description="Data variables to check and ensure there are not missing values in region writing"
    )
    
    @field_validator("check_vars")
    @classmethod
    def validate_check_vars(cls, v: Optional[Union[str, List[str]]]) -> Optional[Union[str, List[str]]]:
        if v is not None and not isinstance(v, (str, list)):
            raise ValueError("`check_vars` must be one of 'all', None or a list of data vars")
        if isinstance(v, str) and v != "all":
            raise ValueError("`check_vars` as string must be 'all'")
        return v
    
    @field_validator("missing_check_vars")
    @classmethod
    def validate_missing_check_vars(cls, v: Optional[Union[str, List[str]]]) -> Optional[Union[str, List[str]]]:
        if v is not None and not isinstance(v, (str, list)):
            raise ValueError("`missing_check_vars` must be one of 'all', None or a list of data vars")
        if isinstance(v, str) and v != "all":
            raise ValueError("`missing_check_vars` as string must be 'all'")
        return v


class DatameshDatasource(BaseModel):
    """Configuration for datamesh datasource.
    
    Note:
    - When writing using the zarr client, the driver should remain "vzarr".
    - When writing using the xarray API, the driver should be "zarr".
    - All the Datasource fields that are set will be updated in datamesh, even if None.
    - The schema, geometry and the time range will be updated from the dataset if not
      set. In order to avoid this, set them explicitly.
    """
    id: str = Field(..., description="Datamesh datasource ID")
    name: Optional[str] = Field(None, description="Human-readable name for the datasource")
    description: Optional[str] = Field(None, description="Description of the datasource")
    coordinates: Optional[Dict[str, str]] = Field(None, description="Coordinate mapping (e.g., {'x': 'longitude', 'y': 'latitude', 't': 'time'})")
    details: Optional[str] = Field(None, description="URL with more details about the datasource")
    tags: Optional[List[str]] = Field(None, description="Tags associated with the datasource")
    driver: str = Field(
        default="vzarr",
        description="Driver to use for datamesh datasource",
    )
    
    # Additional fields that can be set explicitly to override auto-detection
    dataschema: Optional[Dict[str, Any]] = Field(None, description="Explicit schema for the datasource")
    geometry: Optional[Dict[str, Any]] = Field(None, description="Explicit geometry for the datasource")
    tstart: Optional[Union[str, datetime]] = Field(None, description="Explicit start time for the datasource")
    tend: Optional[Union[str, datetime]] = Field(None, description="Explicit end time for the datasource")
    
    model_config = ConfigDict(extra="allow")


class DatameshConfig(BaseModel):
    """Configuration for datamesh integration."""
    datasource: Optional[Union[DatameshDatasource, Dict[str, Any]]] = Field(None, description="Datamesh datasource configuration")
    token: Optional[str] = Field(None, description="Datamesh token for authentication")
    service: str = Field("https://datamesh-v1.oceanum.io", description="Datamesh service URL")
    use_zarr_client: bool = Field(True, description="Whether to use the datamesh zarr client for writing")


class ZarrConverterConfig(BaseModel):
    """Main configuration for ZarrConverter."""
    chunking: ChunkingConfig = Field(default_factory=ChunkingConfig, description="Chunking configuration")
    compression: Optional[CompressionConfig] = Field(None, description="Compression configuration")
    packing: PackingConfig = Field(default_factory=PackingConfig, description="Packing configuration")
    time: TimeConfig = Field(default_factory=TimeConfig, description="Time configuration")
    variables: VariableConfig = Field(default_factory=VariableConfig, description="Variable configuration")
    missing_data: MissingDataConfig = Field(default_factory=MissingDataConfig, description="Missing data configuration")
    datamesh: Optional[DatameshConfig] = Field(None, description="Datamesh integration configuration")
    attrs: Dict[str, Any] = Field(default_factory=dict, description="Additional global attributes")
    target_chunk_size_mb: Optional[int] = Field(None, description="Target chunk size in MB for intelligent chunking")
    access_pattern: str = Field("balanced", description="Access pattern for chunking optimization ('temporal', 'spatial', 'balanced')")
    
    # Backward compatibility fields
    retries_on_missing: int = Field(0, description="Number of retries if missing values are encountered", ge=0)
    missing_check_vars: Optional[Union[str, List[str]]] = Field(
        "all", 
        description="Data variables to check and ensure there are not missing values in region writing"
    )
    
    @field_validator("access_pattern")
    @classmethod
    def validate_access_pattern(cls, v: str) -> str:
        if v not in ["temporal", "spatial", "balanced"]:
            raise ValueError("access_pattern must be one of 'temporal', 'spatial', or 'balanced'")
        return v
    
    @field_validator("retries_on_missing")
    @classmethod
    def validate_retries_on_missing(cls, v: int) -> int:
        if v < 0:
            raise ValueError("retries_on_missing must be non-negative")
        return v
    
    @field_validator("missing_check_vars")
    @classmethod
    def validate_missing_check_vars(cls, v: Optional[Union[str, List[str]]]) -> Optional[Union[str, List[str]]]:
        if v is not None and not isinstance(v, (str, list)):
            raise ValueError("`missing_check_vars` must be one of 'all', None or a list of data vars")
        if isinstance(v, str) and v != "all":
            raise ValueError("`missing_check_vars` as string must be 'all'")
        return v
    
    @field_validator('datamesh')
    @classmethod
    def validate_datamesh(cls, v: Optional[DatameshConfig]) -> Optional[DatameshConfig]:
        if v is not None and isinstance(v.datasource, dict):
            v.datasource = DatameshDatasource(**v.datasource)
        return v
    
    @classmethod
    def from_yaml_file(cls, config_path: Union[str, Path]) -> "ZarrConverterConfig":
        """Load configuration from YAML file."""
        path = Path(config_path)
        if not path.exists():
            raise FileNotFoundError(f"Configuration file {config_path} not found")
        
        with open(path, "r") as f:
            config_dict = yaml.safe_load(f)
        
        return cls(**config_dict)
    
    @classmethod
    def from_json_file(cls, config_path: Union[str, Path]) -> "ZarrConverterConfig":
        """Load configuration from JSON file."""
        path = Path(config_path)
        if not path.exists():
            raise FileNotFoundError(f"Configuration file {config_path} not found")
        
        with open(path, "r") as f:
            config_dict = json.load(f)
        
        return cls(**config_dict)
    
    def to_yaml_file(self, config_path: Union[str, Path]) -> None:
        """Save configuration to YAML file."""
        path = Path(config_path)
        with open(path, "w") as f:
            yaml.dump(self.model_dump(), f, default_flow_style=False)
    
    def to_json_file(self, config_path: Union[str, Path]) -> None:
        """Save configuration to JSON file."""
        path = Path(config_path)
        with open(path, "w") as f:
            json.dump(self.model_dump(), f, indent=2)


def load_config_from_file(config_path: Union[str, Path]) -> ZarrConverterConfig:
    """Load configuration from YAML or JSON file."""
    path = Path(config_path)
    if not path.exists():
        raise FileNotFoundError(f"Configuration file {config_path} not found")
    
    with open(path, "r") as f:
        if path.suffix.lower() in [".yml", ".yaml"]:
            config_dict = yaml.safe_load(f)
        elif path.suffix.lower() == ".json":
            config_dict = json.load(f)
        else:
            raise ValueError("Configuration file must be YAML or JSON")
    
    return ZarrConverterConfig(**config_dict)