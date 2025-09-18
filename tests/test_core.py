"""
Tests for zarrio core functionality.
"""

import pytest
import tempfile
import numpy as np
import pandas as pd
import xarray as xr
import os
from pathlib import Path

from zarrio import (
    ZarrConverter,
    convert_to_zarr,
    append_to_zarr,
    ZarrConverterConfig,
    ChunkingConfig,
    PackingConfig,
    CompressionConfig,
    ConversionError
)


def create_test_dataset(
    filename: str,
    t0: str = "2000-01-01",
    periods: int = 10,
    output: str = "netcdf"
) -> str:
    """Create a test dataset for testing."""
    # Create test data
    np.random.seed(42)  # For reproducible tests
    data = np.random.random([periods, 3, 4])
    ds = xr.Dataset(
        {
            "temperature": (("time", "lat", "lon"), data),
            "pressure": (("time", "lat", "lon"), data * 1000),
        },
        coords={
            "time": pd.date_range(t0, periods=periods),
            "lat": [-10, 0, 10],
            "lon": [20, 30, 40, 50],
        },
    )
    
    # Add some attributes
    ds.attrs["title"] = "Test dataset"
    ds["temperature"].attrs["units"] = "K"
    ds["pressure"].attrs["units"] = "hPa"
    
    if output == "netcdf":
        ds.to_netcdf(filename)
    elif output == "zarr":
        ds.to_zarr(filename)
    
    return filename


def test_zarr_converter_basic():
    """Test basic ZarrConverter functionality."""
    with tempfile.TemporaryDirectory() as tmpdir:
        # Create test data
        ncfile = os.path.join(tmpdir, "test.nc")
        zarrfile = os.path.join(tmpdir, "test.zarr")
        create_test_dataset(ncfile)
        
        # Convert using ZarrConverter
        converter = ZarrConverter()
        converter.convert(ncfile, zarrfile)
        
        # Verify conversion
        assert os.path.exists(zarrfile)
        
        # Compare datasets
        ds_orig = xr.open_dataset(ncfile)
        ds_zarr = xr.open_zarr(zarrfile)
        
        # Check that data is approximately equal
        np.testing.assert_allclose(ds_orig["temperature"].values, ds_zarr["temperature"].values)
        np.testing.assert_allclose(ds_orig["pressure"].values, ds_zarr["pressure"].values)
        
        # Check coordinates
        assert np.array_equal(ds_orig["time"].values, ds_zarr["time"].values)
        assert np.array_equal(ds_orig["lat"].values, ds_zarr["lat"].values)
        assert np.array_equal(ds_orig["lon"].values, ds_zarr["lon"].values)
        
        # Check attributes
        assert ds_orig.attrs["title"] == ds_zarr.attrs["title"]
        assert ds_orig["temperature"].attrs["units"] == ds_zarr["temperature"].attrs["units"]
        assert ds_orig["pressure"].attrs["units"] == ds_zarr["pressure"].attrs["units"]


def test_convert_to_zarr_function():
    """Test convert_to_zarr function."""
    with tempfile.TemporaryDirectory() as tmpdir:
        # Create test data
        ncfile = os.path.join(tmpdir, "test.nc")
        zarrfile = os.path.join(tmpdir, "test.zarr")
        create_test_dataset(ncfile)
        
        # Convert using function
        convert_to_zarr(ncfile, zarrfile)
        
        # Verify conversion
        assert os.path.exists(zarrfile)
        
        # Compare datasets
        ds_orig = xr.open_dataset(ncfile)
        ds_zarr = xr.open_zarr(zarrfile)
        
        # Check that data is approximately equal
        np.testing.assert_allclose(ds_orig["temperature"].values, ds_zarr["temperature"].values)
        np.testing.assert_allclose(ds_orig["pressure"].values, ds_zarr["pressure"].values)


def test_convert_with_chunking():
    """Test conversion with chunking."""
    with tempfile.TemporaryDirectory() as tmpdir:
        # Create test data
        ncfile = os.path.join(tmpdir, "test.nc")
        zarrfile = os.path.join(tmpdir, "test.zarr")
        create_test_dataset(ncfile)
        
        # Convert with chunking
        convert_to_zarr(
            ncfile,
            zarrfile,
            chunking={"time": 5, "lat": 2, "lon": 3}
        )
        
        # Verify conversion
        assert os.path.exists(zarrfile)
        
        # Open and check that chunking was applied
        ds_zarr = xr.open_zarr(zarrfile)
        # Note: Actual chunk verification would require checking dask arrays


def test_convert_with_variables():
    """Test conversion with variable selection."""
    with tempfile.TemporaryDirectory() as tmpdir:
        # Create test data
        ncfile = os.path.join(tmpdir, "test.nc")
        zarrfile = os.path.join(tmpdir, "test.zarr")
        create_test_dataset(ncfile)
        
        # Convert with variable selection
        convert_to_zarr(
            ncfile,
            zarrfile,
            variables=["temperature"]
        )
        
        # Verify conversion
        assert os.path.exists(zarrfile)
        
        # Check that only selected variables are present
        ds_zarr = xr.open_zarr(zarrfile)
        assert "temperature" in ds_zarr.data_vars
        assert "pressure" not in ds_zarr.data_vars


def test_convert_with_drop_variables():
    """Test conversion with variable dropping."""
    with tempfile.TemporaryDirectory() as tmpdir:
        # Create test data
        ncfile = os.path.join(tmpdir, "test.nc")
        zarrfile = os.path.join(tmpdir, "test.zarr")
        create_test_dataset(ncfile)
        
        # Convert with variable dropping
        convert_to_zarr(
            ncfile,
            zarrfile,
            drop_variables=["pressure"]
        )
        
        # Verify conversion
        assert os.path.exists(zarrfile)
        
        # Check that only non-dropped variables are present
        ds_zarr = xr.open_zarr(zarrfile)
        assert "temperature" in ds_zarr.data_vars
        assert "pressure" not in ds_zarr.data_vars


def test_append_to_zarr():
    """Test appending data to existing Zarr store."""
    with tempfile.TemporaryDirectory() as tmpdir:
        # Create initial dataset
        ncfile1 = os.path.join(tmpdir, "test1.nc")
        zarrfile = os.path.join(tmpdir, "test.zarr")
        create_test_dataset(ncfile1, t0="2000-01-01", periods=5)
        
        # Convert first dataset
        convert_to_zarr(ncfile1, zarrfile)
        
        # Create second dataset to append
        ncfile2 = os.path.join(tmpdir, "test2.nc")
        create_test_dataset(ncfile2, t0="2000-01-06", periods=5)
        
        # Append second dataset
        append_to_zarr(ncfile2, zarrfile)
        
        # Verify append
        assert os.path.exists(zarrfile)
        
        # Check combined dataset
        ds_orig1 = xr.open_dataset(ncfile1)
        ds_orig2 = xr.open_dataset(ncfile2)
        ds_combined = xr.concat([ds_orig1, ds_orig2], dim="time")
        ds_zarr = xr.open_zarr(zarrfile)
        
        # Check that times are combined
        assert len(ds_zarr["time"]) == 10
        assert ds_zarr["time"].values[0] == ds_combined["time"].values[0]
        assert ds_zarr["time"].values[-1] == ds_combined["time"].values[-1]


def test_converter_with_packing():
    """Test ZarrConverter with data packing."""
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
                "pressure": (("time", "lat", "lon"), data * 1000),
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
        ds["pressure"].attrs["valid_min"] = 0.0
        ds["pressure"].attrs["valid_max"] = 1000.0
        
        # Save to NetCDF
        ds.to_netcdf(ncfile)
        
        # Convert with packing
        converter = ZarrConverter(packing=True, packing_bits=16)
        converter.convert(ncfile, zarrfile)
        
        # Verify conversion
        assert os.path.exists(zarrfile)


def test_conversion_error_handling():
    """Test error handling in conversion."""
    with tempfile.TemporaryDirectory() as tmpdir:
        # Try to convert non-existent file
        with pytest.raises(ConversionError):
            convert_to_zarr("nonexistent.nc", "output.zarr")


if __name__ == "__main__":
    pytest.main([__file__])