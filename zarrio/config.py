"""
Configuration management for zarrio.
"""

from typing import Dict, Any, Optional
from pathlib import Path
import yaml
import json


class Config:
    """Configuration management class."""
    
    def __init__(self, config_dict: Optional[Dict[str, Any]] = None):
        """
        Initialize configuration.
        
        Args:
            config_dict: Dictionary of configuration values
        """
        self._config = config_dict or {}
    
    @classmethod
    def from_file(cls, file_path: str) -> "Config":
        """
        Load configuration from file.
        
        Args:
            file_path: Path to configuration file
            
        Returns:
            Config instance
        """
        path = Path(file_path)
        if not path.exists():
            raise FileNotFoundError(f"Configuration file {file_path} not found")
        
        with open(path, "r") as f:
            if path.suffix.lower() in [".yml", ".yaml"]:
                config_dict = yaml.safe_load(f)
            elif path.suffix.lower() == ".json":
                config_dict = json.load(f)
            else:
                raise ValueError("Configuration file must be YAML or JSON")
        
        return cls(config_dict)
    
    def get(self, key: str, default: Any = None) -> Any:
        """
        Get configuration value.
        
        Args:
            key: Configuration key
            default: Default value if key not found
            
        Returns:
            Configuration value
        """
        return self._config.get(key, default)
    
    def set(self, key: str, value: Any) -> None:
        """
        Set configuration value.
        
        Args:
            key: Configuration key
            value: Configuration value
        """
        self._config[key] = value
    
    def to_dict(self) -> Dict[str, Any]:
        """
        Convert configuration to dictionary.
        
        Returns:
            Configuration dictionary
        """
        return self._config.copy()
    
    def __getitem__(self, key: str) -> Any:
        """Get configuration value using bracket notation."""
        return self._config[key]
    
    def __setitem__(self, key: str, value: Any) -> None:
        """Set configuration value using bracket notation."""
        self._config[key] = value
    
    def __contains__(self, key: str) -> bool:
        """Check if key exists in configuration."""
        return key in self._config


# Default configuration
DEFAULT_CONFIG = {
    "chunking": {},
    "compression": None,
    "packing": False,
    "packing_bits": 16,
    "time_dim": "time",
    "append_dim": "time",
    "variables": None,
    "drop_variables": None
}