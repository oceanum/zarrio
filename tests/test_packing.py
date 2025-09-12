"""
Tests for zarrify packing functionality.
"""

import pytest
import numpy as np
import xarray as xr
import pandas as pd

from zarrify.packing import Packer, FixedScaleOffset


def test_packer_initialization():
    """Test Packer initialization."""
    # Test valid nbits
    packer = Packer(nbits=8)
    assert packer.nbits == 8
    
    packer = Packer(nbits=16)
    assert packer.nbits == 16
    
    packer = Packer(nbits=32)
    assert packer.nbits == 32
    
    # Test invalid nbits
    with pytest.raises(ValueError):
        Packer(nbits=10)


def test_compute_scale_and_offset():
    """Test scale and offset computation."""
    packer = Packer(nbits=16)
    
    # Test normal case
    scale, offset = packer.compute_scale_and_offset(0.0, 100.0)
    expected_scale = 100.0 / (2**16 - 1)
    expected_offset = 0.0 + 2**15 * expected_scale
    assert abs(scale - expected_scale) < 1e-10
    assert abs(offset - expected_offset) < 1e-10
    
    # Test equal min and max
    scale, offset = packer.compute_scale_and_offset(50.0, 50.0)
    assert scale == 1.0
    assert offset == 50.0 + 2**15 * 1.0


def test_add_valid_range_attributes():
    """Test adding valid range attributes."""
    # Create test dataset
    data = np.random.random([5, 3, 4]) * 100
    ds = xr.Dataset(
        {
            "temperature": (("time", "lat", "lon"), data),
            "pressure": (("time", "lat", "lon"), data * 10),
        },
        coords={
            "time": pd.date_range("2000-01-01", periods=5),
            "lat": [-10, 0, 10],
            "lon": [20, 30, 40, 50],
        },
    )
    
    # Add valid range attributes
    packer = Packer()
    ds_with_attrs = packer.add_valid_range_attributes(ds)
    
    # Check that attributes were added
    assert "valid_min" in ds_with_attrs["temperature"].attrs
    assert "valid_max" in ds_with_attrs["temperature"].attrs
    assert "valid_min" in ds_with_attrs["pressure"].attrs
    assert "valid_max" in ds_with_attrs["pressure"].attrs
    
    # Check that values are reasonable
    temp_min = ds_with_attrs["temperature"].attrs["valid_min"]
    temp_max = ds_with_attrs["temperature"].attrs["valid_max"]
    assert temp_min <= data.min()
    assert temp_max >= data.max()
    
    pressure_min = ds_with_attrs["pressure"].attrs["valid_min"]
    pressure_max = ds_with_attrs["pressure"].attrs["valid_max"]
    assert pressure_min <= (data * 10).min()
    assert pressure_max >= (data * 10).max()


def test_setup_encoding():
    """Test encoding setup."""
    # Create test dataset with valid range attributes
    data = np.random.random([5, 3]) * 100
    ds = xr.Dataset(
        {
            "temperature": (("time", "lat"), data),
        },
        coords={
            "time": pd.date_range("2000-01-01", periods=5),
            "lat": [-10, 0, 10],
        },
    )
    
    # Add valid range attributes
    ds["temperature"].attrs["valid_min"] = 0.0
    ds["temperature"].attrs["valid_max"] = 100.0
    
    # Setup encoding
    packer = Packer(nbits=16)
    encoding = packer.setup_encoding(ds)
    
    # Check that encoding was created
    assert "temperature" in encoding
    assert "filters" in encoding["temperature"]
    assert len(encoding["temperature"]["filters"]) == 1
    
    # Check filter type if zarr is available
    if FixedScaleOffset is not None:
        assert isinstance(encoding["temperature"]["filters"][0], FixedScaleOffset)


def test_setup_encoding_no_valid_range():
    """Test encoding setup with no valid range attributes."""
    # Create test dataset without valid range attributes
    data = np.random.random([5, 3])
    ds = xr.Dataset(
        {
            "temperature": (("time", "lat"), data),
        },
        coords={
            "time": pd.date_range("2000-01-01", periods=5),
            "lat": [-10, 0, 10],
        },
    )
    
    # Setup encoding
    packer = Packer(nbits=16)
    encoding = packer.setup_encoding(ds)
    
    # Check that no encoding was created for variables without valid range
    assert "temperature" not in encoding


if __name__ == "__main__":
    pytest.main([__file__])