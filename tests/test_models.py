"""
Tests for Pydantic models in zarrio.
"""

import pytest
import tempfile
import json
import yaml
from pathlib import Path

from zarrio.models import (
    ZarrConverterConfig,
    ChunkingConfig,
    PackingConfig,
    CompressionConfig,
    TimeConfig,
    VariableConfig,
    MissingDataConfig,
    load_config_from_file
)


def test_chunking_config():
    """Test ChunkingConfig model."""
    # Test valid configuration
    config = ChunkingConfig(time=100, lat=50, lon=100)
    assert config.time == 100
    assert config.lat == 50
    assert config.lon == 100
    
    # Test with extra dimensions
    config = ChunkingConfig(time=100, depth=20)
    assert config.time == 100
    assert config.depth == 20


def test_packing_config():
    """Test PackingConfig model."""
    # Test valid configuration
    config = PackingConfig(enabled=True, bits=16)
    assert config.enabled is True
    assert config.bits == 16
    
    # Test default values
    config = PackingConfig()
    assert config.enabled is False
    assert config.bits == 16
    
    # Test bit validation
    with pytest.raises(ValueError):
        PackingConfig(bits=12)  # Invalid bits


def test_compression_config():
    """Test CompressionConfig model."""
    # Test valid configuration
    config = CompressionConfig(method="blosc:zstd:3", cname="zstd", clevel=3, shuffle="shuffle")
    assert config.method == "blosc:zstd:3"
    assert config.cname == "zstd"
    assert config.clevel == 3
    assert config.shuffle == "shuffle"
    
    # Test default values
    config = CompressionConfig()
    assert config.method is None
    assert config.cname == "zstd"
    assert config.clevel == 1
    assert config.shuffle == "shuffle"


def test_time_config():
    """Test TimeConfig model."""
    # Test valid configuration
    config = TimeConfig(dim="time", append_dim="time", global_start="2023-01-01", global_end="2023-12-31")
    assert config.dim == "time"
    assert config.append_dim == "time"
    assert config.global_start == "2023-01-01"
    assert config.global_end == "2023-12-31"
    
    # Test default values
    config = TimeConfig()
    assert config.dim == "time"
    assert config.append_dim == "time"
    assert config.global_start is None
    assert config.global_end is None


def test_variable_config():
    """Test VariableConfig model."""
    # Test valid configuration
    config = VariableConfig(include=["temperature", "pressure"], exclude=["humidity"])
    assert config.include == ["temperature", "pressure"]
    assert config.exclude == ["humidity"]
    
    # Test default values
    config = VariableConfig()
    assert config.include is None
    assert config.exclude is None


def test_zarr_converter_config():
    """Test ZarrConverterConfig model."""
    # Test valid configuration
    config = ZarrConverterConfig(
        chunking=ChunkingConfig(time=100, lat=50, lon=100),
        packing=PackingConfig(enabled=True, bits=16),
        compression=CompressionConfig(method="blosc:zstd:3"),
        time=TimeConfig(dim="time", append_dim="time"),
        variables=VariableConfig(include=["temperature"], exclude=["humidity"]),
        attrs={"title": "Test dataset"}
    )
    
    assert config.chunking.time == 100
    assert config.chunking.lat == 50
    assert config.chunking.lon == 100
    assert config.packing.enabled is True
    assert config.packing.bits == 16
    assert config.compression.method == "blosc:zstd:3"
    assert config.time.dim == "time"
    assert config.time.append_dim == "time"
    assert config.variables.include == ["temperature"]
    assert config.variables.exclude == ["humidity"]
    assert config.attrs["title"] == "Test dataset"


def test_nested_config_creation():
    """Test creation of nested configurations."""
    # Test creating config from nested dictionary
    config_dict = {
        "chunking": {"time": 100, "lat": 50},
        "packing": {"enabled": True, "bits": 16},
        "compression": {"method": "blosc:zstd:3"},
        "time": {"dim": "time", "append_dim": "time"},
        "variables": {"include": ["temperature"], "exclude": ["humidity"]},
        "attrs": {"title": "Test dataset"}
    }
    
    config = ZarrConverterConfig(**config_dict)
    
    assert config.chunking.time == 100
    assert config.chunking.lat == 50
    assert config.packing.enabled is True
    assert config.packing.bits == 16
    assert config.compression.method == "blosc:zstd:3"
    assert config.time.dim == "time"
    assert config.time.append_dim == "time"
    assert config.variables.include == ["temperature"]
    assert config.variables.exclude == ["humidity"]
    assert config.attrs["title"] == "Test dataset"


def test_config_from_yaml_file():
    """Test loading configuration from YAML file."""
    with tempfile.TemporaryDirectory() as tmpdir:
        # Create YAML config file
        config_file = Path(tmpdir) / "config.yaml"
        config_dict = {
            "chunking": {"time": 100, "lat": 50, "lon": 100},
            "packing": {"enabled": True, "bits": 16},
            "compression": {"method": "blosc:zstd:3"},
            "time": {"dim": "time", "append_dim": "time"},
            "variables": {"include": ["temperature"], "exclude": ["humidity"]},
            "attrs": {"title": "Test dataset"}
        }
        
        with open(config_file, "w") as f:
            yaml.dump(config_dict, f)
        
        # Load config from file
        config = load_config_from_file(config_file)
        
        assert config.chunking.time == 100
        assert config.chunking.lat == 50
        assert config.chunking.lon == 100
        assert config.packing.enabled is True
        assert config.packing.bits == 16
        assert config.compression.method == "blosc:zstd:3"
        assert config.time.dim == "time"
        assert config.time.append_dim == "time"
        assert config.variables.include == ["temperature"]
        assert config.variables.exclude == ["humidity"]
        assert config.attrs["title"] == "Test dataset"


def test_config_from_json_file():
    """Test loading configuration from JSON file."""
    with tempfile.TemporaryDirectory() as tmpdir:
        # Create JSON config file
        config_file = Path(tmpdir) / "config.json"
        config_dict = {
            "chunking": {"time": 100, "lat": 50, "lon": 100},
            "packing": {"enabled": True, "bits": 16},
            "compression": {"method": "blosc:zstd:3"},
            "time": {"dim": "time", "append_dim": "time"},
            "variables": {"include": ["temperature"], "exclude": ["humidity"]},
            "attrs": {"title": "Test dataset"}
        }
        
        with open(config_file, "w") as f:
            json.dump(config_dict, f)
        
        # Load config from file
        config = load_config_from_file(config_file)
        
        assert config.chunking.time == 100
        assert config.chunking.lat == 50
        assert config.chunking.lon == 100
        assert config.packing.enabled is True
        assert config.packing.bits == 16
        assert config.compression.method == "blosc:zstd:3"
        assert config.time.dim == "time"
        assert config.time.append_dim == "time"
        assert config.variables.include == ["temperature"]
        assert config.variables.exclude == ["humidity"]
        assert config.attrs["title"] == "Test dataset"


def test_invalid_config_file():
    """Test handling of invalid configuration files."""
    with tempfile.TemporaryDirectory() as tmpdir:
        # Test non-existent file
        with pytest.raises(FileNotFoundError):
            load_config_from_file(Path(tmpdir) / "nonexistent.yaml")
        
        # Test invalid file extension
        invalid_file = Path(tmpdir) / "config.txt"
        invalid_file.touch()
        
        with pytest.raises(ValueError):
            load_config_from_file(invalid_file)


if __name__ == "__main__":
    pytest.main([__file__])