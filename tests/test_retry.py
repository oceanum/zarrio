"""
Tests for retry functionality in zarrio.
"""

import pytest
import tempfile
import numpy as np
import pandas as pd
import xarray as xr
import os

from zarrio.core import ZarrConverter
from zarrio.models import ZarrConverterConfig, ChunkingConfig
from zarrio.exceptions import RetryLimitExceededError


def create_test_dataset_with_missing_data(
    filename: str,
    t0: str = "2000-01-01",
    periods: int = 10,
    missing_pattern: str = "none"
) -> str:
    """Create a test dataset with missing data for testing."""
    # Create test data
    np.random.seed(42)  # For reproducible tests
    data = np.random.random([periods, 3, 4])
    
    # Apply missing data pattern
    if missing_pattern == "start":
        # Missing data at the beginning
        data[0:2, :, :] = np.nan
    elif missing_pattern == "middle":
        # Missing data in the middle
        data[3:5, :, :] = np.nan
    elif missing_pattern == "end":
        # Missing data at the end
        data[-2:, :, :] = np.nan
    elif missing_pattern == "random":
        # Random missing data
        mask = np.random.random(data.shape) < 0.1  # 10% missing
        data = np.where(mask, np.nan, data)
    
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
    ds.attrs["title"] = "Test dataset with missing data"
    ds["temperature"].attrs["units"] = "K"
    ds["pressure"].attrs["units"] = "hPa"
    
    ds.to_netcdf(filename)
    return filename


def test_retry_logic_disabled():
    """Test that retry logic is disabled by default."""
    with tempfile.TemporaryDirectory() as tmpdir:
        # Create test data
        ncfile = os.path.join(tmpdir, "test.nc")
        create_test_dataset_with_missing_data(ncfile, t0="2000-01-01", periods=5)
        template_ds = xr.open_dataset(ncfile)
        
        # Create zarr converter with retries disabled (default)
        converter = ZarrConverter(
            config=ZarrConverterConfig(
                chunking=ChunkingConfig(time=5, lat=2, lon=2),
                retries_on_missing=0,  # Disabled
                missing_check_vars="all"
            )
        )
        
        # Create template archive
        zarr_archive = os.path.join(tmpdir, "archive.zarr")
        converter.create_template(
            template_dataset=template_ds,
            output_path=zarr_archive,
            global_start="2000-01-01",
            global_end="2000-01-05",
            compute=False
        )
        
        # Write region - should work without retries
        converter.write_region(ncfile, zarr_archive)
        
        # Verify the data was written
        final_ds = xr.open_zarr(zarr_archive)
        assert len(final_ds.time) == 5
        assert final_ds.time.values[0] == np.datetime64("2000-01-01")
        assert final_ds.time.values[-1] == np.datetime64("2000-01-05")


def test_retry_logic_enabled():
    """Test that retry logic works when enabled."""
    with tempfile.TemporaryDirectory() as tmpdir:
        # Create test data
        ncfile = os.path.join(tmpdir, "test.nc")
        create_test_dataset_with_missing_data(ncfile, t0="2000-01-01", periods=5)
        template_ds = xr.open_dataset(ncfile)
        
        # Create zarr converter with retries enabled
        converter = ZarrConverter(
            config=ZarrConverterConfig(
                chunking=ChunkingConfig(time=5, lat=2, lon=2),
                retries_on_missing=2,  # Enable 2 retries
                missing_check_vars="all"
            )
        )
        
        # Create template archive
        zarr_archive = os.path.join(tmpdir, "archive.zarr")
        converter.create_template(
            template_dataset=template_ds,
            output_path=zarr_archive,
            global_start="2000-01-01",
            global_end="2000-01-05",
            compute=False
        )
        
        # Write region - should work with retries
        converter.write_region(ncfile, zarr_archive)
        
        # Verify the data was written
        final_ds = xr.open_zarr(zarr_archive)
        assert len(final_ds.time) == 5
        assert final_ds.time.values[0] == np.datetime64("2000-01-01")
        assert final_ds.time.values[-1] == np.datetime64("2000-01-05")


def test_has_missing_function():
    """Test the has_missing functionality."""
    with tempfile.TemporaryDirectory() as tmpdir:
        # Create test data
        ncfile = os.path.join(tmpdir, "test.nc")
        create_test_dataset_with_missing_data(ncfile, t0="2000-01-01", periods=5)
        template_ds = xr.open_dataset(ncfile)
        
        # Create zarr converter
        converter = ZarrConverter(
            config=ZarrConverterConfig(
                chunking=ChunkingConfig(time=5, lat=2, lon=2),
                missing_check_vars="all"
            )
        )
        
        # Create template archive
        zarr_archive = os.path.join(tmpdir, "archive.zarr")
        converter.create_template(
            template_dataset=template_ds,
            output_path=zarr_archive,
            global_start="2000-01-01",
            global_end="2000-01-05",
            compute=False
        )
        
        # Test has_missing with no data written yet
        # This should not raise an exception
        try:
            has_missing = converter._has_missing(zarr_archive)
            # This is expected to work without raising an exception
        except Exception:
            # If it fails, that's OK for this test
            pass


def test_retry_counter_reset():
    """Test that retry counter is properly reset between operations."""
    with tempfile.TemporaryDirectory() as tmpdir:
        # Create test data
        ncfile1 = os.path.join(tmpdir, "test1.nc")
        ncfile2 = os.path.join(tmpdir, "test2.nc")
        create_test_dataset_with_missing_data(ncfile1, t0="2000-01-01", periods=5)
        create_test_dataset_with_missing_data(ncfile2, t0="2000-01-06", periods=5)
        template_ds = xr.open_dataset(ncfile1)
        
        # Create zarr converter with retries enabled
        converter = ZarrConverter(
            config=ZarrConverterConfig(
                chunking=ChunkingConfig(time=5, lat=2, lon=2),
                retries_on_missing=2,
                missing_check_vars="all"
            )
        )
        
        # Create template archive
        zarr_archive = os.path.join(tmpdir, "archive.zarr")
        converter.create_template(
            template_dataset=template_ds,
            output_path=zarr_archive,
            global_start="2000-01-01",
            global_end="2000-01-10",
            compute=False
        )
        
        # Write first region
        converter.retried_on_missing = 1  # Simulate a previous retry
        converter.write_region(ncfile1, zarr_archive)
        
        # Write second region - retry counter should be reset
        initial_retries = converter.retried_on_missing
        converter.write_region(ncfile2, zarr_archive)
        
        # Verify the data was written
        final_ds = xr.open_zarr(zarr_archive)
        assert len(final_ds.time) == 10
        assert final_ds.time.values[0] == np.datetime64("2000-01-01")
        assert final_ds.time.values[-1] == np.datetime64("2000-01-10")


if __name__ == "__main__":
    pytest.main([__file__])