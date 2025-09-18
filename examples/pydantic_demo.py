"""
Example demonstrating Pydantic configuration in zarrio.
"""

import tempfile
import json
import yaml
from pathlib import Path
import sys

# Add the current directory to Python path
sys.path.insert(0, str(Path(__file__).parent))

from zarrio.models import (
    ZarrConverterConfig, 
    ChunkingConfig, 
    PackingConfig, 
    CompressionConfig,
    TimeConfig,
    VariableConfig
)


def demonstrate_pydantic_config():
    """Demonstrate Pydantic configuration usage."""
    print("=== zarrio Pydantic Configuration Demo ===\n")
    
    # 1. Creating configuration programmatically
    print("1. Creating configuration programmatically:")
    config = ZarrConverterConfig(
        chunking=ChunkingConfig(time=100, lat=50, lon=100),
        compression=CompressionConfig(method="blosc:zstd:3"),
        packing=PackingConfig(enabled=True, bits=16),
        time=TimeConfig(dim="time", append_dim="time"),
        variables=VariableConfig(include=["temperature", "pressure"], exclude=["humidity"]),
        attrs={"title": "Demo dataset", "source": "zarrio"}
    )
    
    print(f"   Chunking: time={config.chunking.time}, lat={config.chunking.lat}, lon={config.chunking.lon}")
    print(f"   Compression: {config.compression.method}")
    print(f"   Packing: enabled={config.packing.enabled}, bits={config.packing.bits}")
    print(f"   Time dimension: {config.time.dim}")
    print(f"   Included variables: {config.variables.include}")
    print(f"   Attributes: {config.attrs}")
    
    # 2. Configuration from dictionary
    print("\n2. Configuration from dictionary:")
    config_dict = {
        "chunking": {"time": 200, "lat": 75, "depth": 20},
        "packing": {"enabled": False},
        "time": {"dim": "forecast_time", "append_dim": "forecast_time"},
        "attrs": {"institution": "Oceanum", "processing_date": "2023-01-01"}
    }
    
    config_from_dict = ZarrConverterConfig(**config_dict)
    print(f"   Time dimension: {config_from_dict.time.dim}")
    print(f"   Append dimension: {config_from_dict.time.append_dim}")
    print(f"   Depth chunking: {config_from_dict.chunking.depth}")
    print(f"   Institution: {config_from_dict.attrs['institution']}")
    
    # 3. Configuration file support
    print("\n3. Configuration file support:")
    with tempfile.TemporaryDirectory() as tmpdir:
        # Create YAML config file
        yaml_config_file = Path(tmpdir) / "config.yaml"
        yaml_config = {
            "chunking": {"time": 150, "lat": 60, "lon": 120},
            "compression": {"method": "blosc:zstd:2", "clevel": 2},
            "packing": {"enabled": True, "bits": 16},
            "variables": {"include": ["temperature", "pressure"], "exclude": ["humidity"]},
            "attrs": {"title": "YAML Config Demo", "version": "1.0"}
        }
        
        with open(yaml_config_file, "w") as f:
            yaml.dump(yaml_config, f)
        
        # Load from YAML file
        loaded_config = ZarrConverterConfig.from_yaml_file(yaml_config_file)
        print(f"   Loaded from YAML: time chunking = {loaded_config.chunking.time}")
        print(f"   Included variables: {loaded_config.variables.include}")
        
        # Create JSON config file
        json_config_file = Path(tmpdir) / "config.json"
        json_config = {
            "chunking": {"time": 125, "lat": 55, "lon": 110},
            "compression": {"method": "blosc:lz4:1", "clevel": 1},
            "packing": {"enabled": True, "bits": 8},
            "time": {"global_start": "2023-01-01", "global_end": "2023-12-31"},
            "attrs": {"title": "JSON Config Demo", "version": "1.0"}
        }
        
        with open(json_config_file, "w") as f:
            json.dump(json_config, f, indent=2)
        
        # Load from JSON file
        loaded_json_config = ZarrConverterConfig.from_json_file(json_config_file)
        print(f"   Loaded from JSON: time chunking = {loaded_json_config.chunking.time}")
        print(f"   Global start: {loaded_json_config.time.global_start}")
    
    # 4. Validation features
    print("\n4. Validation features:")
    try:
        # This will raise a validation error
        invalid_config = PackingConfig(bits=12)  # Invalid bits
    except Exception as e:
        print(f"   Validation error caught: {type(e).__name__}: {e}")
    
    # Valid config
    valid_config = PackingConfig(bits=16)
    print(f"   Valid packing config: enabled={valid_config.enabled}, bits={valid_config.bits}")
    
    # 5. Saving configurations
    print("\n5. Saving configurations:")
    with tempfile.TemporaryDirectory() as tmpdir:
        # Save to YAML
        yaml_output = Path(tmpdir) / "output.yaml"
        config.to_yaml_file(yaml_output)
        print(f"   Saved to YAML: {yaml_output}")
        
        # Save to JSON
        json_output = Path(tmpdir) / "output.json"
        config.to_json_file(json_output)
        print(f"   Saved to JSON: {json_output}")
    
    print("\n=== Demo completed successfully! ===")


if __name__ == "__main__":
    demonstrate_pydantic_config()