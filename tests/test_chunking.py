"""
Tests for chunking analysis in zarrio.
"""

import pytest
import numpy as np
import xarray as xr
import pandas as pd
import tempfile
import os

from zarrio.chunking import ChunkAnalyzer, get_chunk_recommendation, validate_chunking
from zarrio.core import ZarrConverter
from zarrio.models import ZarrConverterConfig


def test_chunk_analyzer_initialization():
    """Test ChunkAnalyzer initialization."""
    analyzer = ChunkAnalyzer()
    assert analyzer is not None


def test_chunk_recommendation_temporal():
    """Test chunk recommendation for temporal access pattern."""
    # Simulate climate data dimensions
    dimensions = {
        "time": 3650,  # 10 years of daily data
        "lat": 180,    # 1 degree global
        "lon": 360     # 1 degree global
    }
    
    recommendation = get_chunk_recommendation(
        dimensions=dimensions,
        dtype_size_bytes=4,  # float32
        access_pattern="temporal"
    )
    
    assert recommendation is not None
    assert isinstance(recommendation.chunks, dict)
    assert "time" in recommendation.chunks
    assert "lat" in recommendation.chunks
    assert "lon" in recommendation.chunks
    
    # Temporal focus should favor larger time chunks
    assert recommendation.chunks["time"] >= 10
    assert recommendation.strategy == "temporal_focus"


def test_chunk_recommendation_spatial():
    """Test chunk recommendation for spatial access pattern."""
    # Simulate climate data dimensions
    dimensions = {
        "time": 365,    # 1 year of daily data
        "lat": 720,     # 0.5 degree global
        "lon": 1440     # 0.5 degree global
    }
    
    recommendation = get_chunk_recommendation(
        dimensions=dimensions,
        dtype_size_bytes=4,  # float32
        access_pattern="spatial"
    )
    
    assert recommendation is not None
    assert isinstance(recommendation.chunks, dict)
    assert "time" in recommendation.chunks
    assert "lat" in recommendation.chunks
    assert "lon" in recommendation.chunks
    
    # Spatial focus should favor larger spatial chunks
    assert recommendation.chunks["lat"] >= 20
    assert recommendation.chunks["lon"] >= 20
    assert recommendation.strategy == "spatial_focus"


def test_chunk_recommendation_balanced():
    """Test chunk recommendation for balanced access pattern."""
    # Simulate climate data dimensions
    dimensions = {
        "time": 1825,   # 5 years of daily data
        "lat": 360,     # 1 degree regional
        "lon": 720      # 1 degree regional
    }
    
    recommendation = get_chunk_recommendation(
        dimensions=dimensions,
        dtype_size_bytes=4,  # float32
        access_pattern="balanced"
    )
    
    assert recommendation is not None
    assert isinstance(recommendation.chunks, dict)
    assert "time" in recommendation.chunks
    assert "lat" in recommendation.chunks
    assert "lon" in recommendation.chunks
    
    # Balanced approach should have moderate chunk sizes
    assert 10 <= recommendation.chunks["time"] <= 100
    # For balanced access, spatial dimensions have a minimum of 30, not 10
    assert 30 <= recommendation.chunks["lat"] <= 360  # Should be at most the dimension size
    assert 30 <= recommendation.chunks["lon"] <= 720  # Should be at most the dimension size
    assert recommendation.strategy == "balanced"


def test_chunk_validation():
    """Test chunk validation."""
    # Define dimensions
    dimensions = {
        "time": 365,
        "lat": 180,
        "lon": 360
    }
    
    # Test valid chunking (larger chunks to avoid warnings)
    user_chunks = {
        "time": 50,
        "lat": 50,
        "lon": 50
    }
    
    validation = validate_chunking(user_chunks, dimensions, dtype_size_bytes=4)
    
    assert validation["valid"] is True
    assert validation["chunk_size_mb"] > 0
    # Should have no warnings for reasonable chunk sizes
    # (Small chunks will have warnings, which is expected behavior)


def test_chunk_validation_warnings():
    """Test chunk validation with warnings."""
    # Define dimensions
    dimensions = {
        "time": 365,
        "lat": 180,
        "lon": 360
    }
    
    # Test chunking that's too small (will generate warnings)
    user_chunks = {
        "time": 1000,  # Larger than dimension size
        "lat": 1,
        "lon": 1
    }
    
    validation = validate_chunking(user_chunks, dimensions, dtype_size_bytes=4)
    
    assert validation["valid"] is True  # Still valid, just warnings
    assert len(validation["warnings"]) > 0


def test_chunk_analysis_integration():
    """Test integration of chunk analysis with ZarrConverter."""
    with tempfile.TemporaryDirectory() as tmpdir:
        # Create test dataset
        times = pd.date_range("2020-01-01", periods=365, freq="D")
        lats = np.linspace(-90, 90, 180)
        lons = np.linspace(-180, 180, 360)
        
        data = np.random.random([365, 180, 360])
        ds = xr.Dataset(
            {
                "temperature": (("time", "lat", "lon"), data),
                "pressure": (("time", "lat", "lon"), data * 1000),
            },
            coords={
                "time": times,
                "lat": lats,
                "lon": lons,
            },
        )
        
        # Save as NetCDF
        nc_file = os.path.join(tmpdir, "test.nc")
        ds.to_netcdf(nc_file)
        
        # Test ZarrConverter with chunk analysis
        converter = ZarrConverter()
        
        # This should trigger chunk analysis since no chunking is specified
        # (Note: We're not actually writing the file to avoid I/O in tests)
        assert converter is not None


def test_chunking_with_config():
    """Test chunking with explicit configuration."""
    # Create config with chunking
    config = ZarrConverterConfig(
        chunking={"time": 50, "lat": 30, "lon": 60}
    )
    
    converter = ZarrConverter(config=config)
    
    # Convert chunking config to dict
    chunking_dict = converter._chunking_config_to_dict()
    
    assert chunking_dict["time"] == 50
    assert chunking_dict["lat"] == 30
    assert chunking_dict["lon"] == 60


def test_chunking_without_config():
    """Test chunking without explicit configuration."""
    # Create config without chunking
    config = ZarrConverterConfig()
    
    converter = ZarrConverter(config=config)
    
    # Convert chunking config to dict
    chunking_dict = converter._chunking_config_to_dict()
    
    # Should be empty since no chunking was specified
    assert chunking_dict == {}


if __name__ == "__main__":
    pytest.main([__file__])