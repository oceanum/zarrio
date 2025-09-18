"""
Tests for zarrio CLI functionality.
"""

import pytest
import tempfile
import os
import subprocess
import sys
from pathlib import Path

from zarrio.cli import parse_chunking


def test_parse_chunking():
    """Test parsing chunking strings."""
    # Test empty string
    assert parse_chunking("") == {}
    
    # Test single dimension
    assert parse_chunking("time:100") == {"time": 100}
    
    # Test multiple dimensions
    assert parse_chunking("time:100,lat:50,lon:100") == {
        "time": 100, "lat": 50, "lon": 100
    }
    
    # Test with spaces
    assert parse_chunking("time: 100, lat: 50") == {"time": 100, "lat": 50}


def create_test_dataset(filename: str, output: str = "netcdf"):
    """Create a simple test dataset."""
    import numpy as np
    import pandas as pd
    import xarray as xr
    
    # Create test data
    data = np.random.random([5, 3, 4])
    ds = xr.Dataset(
        {
            "temperature": (("time", "lat", "lon"), data),
        },
        coords={
            "time": pd.date_range("2000-01-01", periods=5),
            "lat": [-10, 0, 10],
            "lon": [20, 30, 40, 50],
        },
    )
    
    if output == "netcdf":
        ds.to_netcdf(filename)
    elif output == "zarr":
        ds.to_zarr(filename)
    
    return filename


def test_cli_convert():
    """Test CLI convert command."""
    with tempfile.TemporaryDirectory() as tmpdir:
        # Create test data
        ncfile = os.path.join(tmpdir, "test.nc")
        zarrfile = os.path.join(tmpdir, "test.zarr")
        create_test_dataset(ncfile)
        
        # Run convert command
        result = subprocess.run([
            sys.executable, "-m", "zarrio.cli", "convert",
            ncfile, zarrfile
        ], capture_output=True, text=True)
        
        # Check that command succeeded
        assert result.returncode == 0
        
        # Check that output file was created
        assert os.path.exists(zarrfile)


def test_cli_convert_with_chunking():
    """Test CLI convert command with chunking."""
    with tempfile.TemporaryDirectory() as tmpdir:
        # Create test data
        ncfile = os.path.join(tmpdir, "test.nc")
        zarrfile = os.path.join(tmpdir, "test.zarr")
        create_test_dataset(ncfile)
        
        # Run convert command with chunking
        result = subprocess.run([
            sys.executable, "-m", "zarrio.cli", "convert",
            ncfile, zarrfile,
            "--chunking", "time:3,lat:2"
        ], capture_output=True, text=True)
        
        # Check that command succeeded
        assert result.returncode == 0
        
        # Check that output file was created
        assert os.path.exists(zarrfile)


def test_cli_convert_with_packing():
    """Test CLI convert command with packing."""
    with tempfile.TemporaryDirectory() as tmpdir:
        # Create test data with valid range attributes for packing
        ncfile = os.path.join(tmpdir, "test.nc")
        zarrfile = os.path.join(tmpdir, "test.zarr")
        
        # Create test data directly with valid range attributes
        import numpy as np
        import pandas as pd
        import xarray as xr
        
        # Create test data
        data = np.random.random([5, 3, 4])
        ds = xr.Dataset(
            {
                "temperature": (("time", "lat", "lon"), data),
            },
            coords={
                "time": pd.date_range("2000-01-01", periods=5),
                "lat": [-10, 0, 10],
                "lon": [20, 30, 40, 50],
            },
        )
        
        # Add valid range attributes for packing
        ds["temperature"].attrs["valid_min"] = 0.0
        ds["temperature"].attrs["valid_max"] = 1.0
        
        # Save to NetCDF
        ds.to_netcdf(ncfile)
        
        # Run convert command with packing
        result = subprocess.run([
            sys.executable, "-m", "zarrio.cli", "convert",
            ncfile, zarrfile,
            "--packing", "--packing-bits", "16"
        ], capture_output=True, text=True)
        
        # Check that command succeeded
        assert result.returncode == 0
        
        # Check that output file was created
        assert os.path.exists(zarrfile)


def test_cli_help():
    """Test CLI help command."""
    # Run help command
    result = subprocess.run([
        sys.executable, "-m", "zarrio.cli", "--help"
    ], capture_output=True, text=True)
    
    # Check that command succeeded
    assert result.returncode == 0
    
    # Check that help text contains expected content
    assert "zarrio" in result.stdout
    assert "convert" in result.stdout
    assert "append" in result.stdout


def test_cli_version():
    """Test CLI version command."""
    # Run version command
    result = subprocess.run([
        sys.executable, "-m", "zarrio.cli", "--version"
    ], capture_output=True, text=True)
    
    # Check that command succeeded
    assert result.returncode == 0
    
    # Check that version is in output
    assert "zarrio" in result.stdout


if __name__ == "__main__":
    pytest.main([__file__])