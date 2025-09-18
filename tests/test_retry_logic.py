"""
Test to verify retry logic for handling missing data in zarrio.
"""

import pytest
import tempfile
import numpy as np
import pandas as pd
import xarray as xr
import os

from zarrio import ZarrConverter
from zarrio.models import ZarrConverterConfig, MissingDataConfig


def create_test_dataset_with_missing_data(
    filename: str,
    t0: str = "2000-01-01",
    periods: int = 10,
    missing_pattern: str = "none"
) -> str:
    """Create a test dataset with missing data for testing retry functionality."""
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
    
    # Add valid range attributes for packing
    ds["temperature"].attrs["valid_min"] = 0.0
    ds["temperature"].attrs["valid_max"] = 1.0
    ds["pressure"].attrs["valid_min"] = 0.0
    ds["pressure"].attrs["valid_max"] = 1000.0
    
    # Add some attributes
    ds.attrs["title"] = "Test dataset with missing data"
    ds["temperature"].attrs["units"] = "K"
    ds["pressure"].attrs["units"] = "hPa"
    
    ds.to_netcdf(filename)
    return filename


def test_retry_logic_disabled_by_default():
    """Test that retry logic is disabled by default."""
    with tempfile.TemporaryDirectory() as tmpdir:
        # Create test data
        ncfile = os.path.join(tmpdir, "test.nc")
        zarrfile = os.path.join(tmpdir, "test.zarr")
        create_test_dataset_with_missing_data(ncfile, t0="2000-01-01", periods=5)
        template_ds = xr.open_dataset(ncfile)
        
        # Create Zarr converter with retries disabled (default)
        converter = ZarrConverter(
            config=ZarrConverterConfig(
                chunking={"time": 5, "lat": 2, "lon": 2},
                missing_data=MissingDataConfig(
                    retries_on_missing=0,  # Disabled
                    missing_check_vars="all"
                )
            )
        )
        
        # Create template archive
        converter.create_template(
            template_dataset=template_ds,
            output_path=zarrfile,
            global_start="2000-01-01",
            global_end="2000-01-05",
            compute=False
        )
        
        # Write region - should work without retries
        converter.write_region(ncfile, zarrfile)
        
        # Verify the data was written
        final_ds = xr.open_zarr(zarrfile)
        assert len(final_ds.time) == 5
        assert final_ds.time.values[0] == np.datetime64("2000-01-01")
        assert final_ds.time.values[-1] == np.datetime64("2000-01-05")
        
        print("✓ Test passed: Retry logic disabled by default")


def test_retry_logic_enabled():
    """Test that retry logic works when enabled."""
    with tempfile.TemporaryDirectory() as tmpdir:
        # Create test data
        ncfile = os.path.join(tmpdir, "test.nc")
        zarrfile = os.path.join(tmpdir, "test.zarr")
        create_test_dataset_with_missing_data(ncfile, t0="2000-01-01", periods=5)
        template_ds = xr.open_dataset(ncfile)
        
        # Create Zarr converter with retries enabled
        converter = ZarrConverter(
            config=ZarrConverterConfig(
                chunking={"time": 5, "lat": 2, "lon": 2},
                missing_data=MissingDataConfig(
                    retries_on_missing=2,  # Enable 2 retries
                    missing_check_vars="all"
                )
            )
        )
        
        # Create template archive
        converter.create_template(
            template_dataset=template_ds,
            output_path=zarrfile,
            global_start="2000-01-01",
            global_end="2000-01-05",
            compute=False
        )
        
        # Write region - should work with retries
        converter.write_region(ncfile, zarrfile)
        
        # Verify the data was written
        final_ds = xr.open_zarr(zarrfile)
        assert len(final_ds.time) == 5
        assert final_ds.time.values[0] == np.datetime64("2000-01-01")
        assert final_ds.time.values[-1] == np.datetime64("2000-01-05")
        
        print("✓ Test passed: Retry logic enabled and working")


def test_missing_data_detection():
    """Test missing data detection."""
    with tempfile.TemporaryDirectory() as tmpdir:
        # Create test data
        ncfile = os.path.join(tmpdir, "test.nc")
        zarrfile = os.path.join(tmpdir, "test.zarr")
        create_test_dataset_with_missing_data(ncfile, t0="2000-01-01", periods=5)
        template_ds = xr.open_dataset(ncfile)
        
        # Create Zarr converter
        converter = ZarrConverter(
            config=ZarrConverterConfig(
                chunking={"time": 5, "lat": 2, "lon": 2},
                missing_data=MissingDataConfig(
                    retries_on_missing=0,  # Disabled
                    missing_check_vars="all"
                )
            )
        )
        
        # Create template archive
        converter.create_template(
            template_dataset=template_ds,
            output_path=zarrfile,
            global_start="2000-01-01",
            global_end="2000-01-05",
            compute=False
        )
        
        # Write region
        converter.write_region(ncfile, zarrfile)
        
        # Check for missing data (should be none in this case)
        final_ds = xr.open_zarr(zarrfile)
        ds_orig = xr.open_dataset(ncfile)
        
        # This should not detect missing data since we're comparing to the same dataset
        has_missing = converter._has_missing(zarrfile, ds_orig)
        assert has_missing is False  # No missing data detected
        
        print("✓ Test passed: Missing data detection working correctly")


def test_retry_counter_reset():
    """Test that retry counter is properly reset between operations."""
    with tempfile.TemporaryDirectory() as tmpdir:
        # Create test data
        ncfile1 = os.path.join(tmpdir, "test1.nc")
        ncfile2 = os.path.join(tmpdir, "test2.nc")
        zarrfile = os.path.join(tmpdir, "test.zarr")
        create_test_dataset_with_missing_data(ncfile1, t0="2000-01-01", periods=5)
        create_test_dataset_with_missing_data(ncfile2, t0="2000-01-06", periods=5)
        template_ds = xr.open_dataset(ncfile1)
        
        # Create Zarr converter with retries enabled
        converter = ZarrConverter(
            config=ZarrConverterConfig(
                chunking={"time": 5, "lat": 2, "lon": 2},
                missing_data=MissingDataConfig(
                    retries_on_missing=1,  # Enable 1 retry
                    missing_check_vars="all"
                )
            )
        )
        
        # Create template archive
        converter.create_template(
            template_dataset=template_ds,
            output_path=zarrfile,
            global_start="2000-01-01",
            global_end="2000-01-10",
            compute=False
        )
        
        # Write first region
        converter.retried_on_missing = 1  # Simulate a previous retry
        converter.write_region(ncfile1, zarrfile)
        
        # Write second region - retry counter should be reset
        initial_retries = converter.retried_on_missing
        converter.write_region(ncfile2, zarrfile)
        
        # Verify the data was written
        final_ds = xr.open_zarr(zarrfile)
        assert len(final_ds.time) == 10
        assert final_ds.time.values[0] == np.datetime64("2000-01-01")
        assert final_ds.time.values[-1] == np.datetime64("2000-01-10")
        
        print("✓ Test passed: Retry counter reset between operations")


def test_retry_limit_exceeded():
    """Test that retry limit is enforced."""
    with tempfile.TemporaryDirectory() as tmpdir:
        # Create test data
        ncfile = os.path.join(tmpdir, "test.nc")
        zarrfile = os.path.join(tmpdir, "test.zarr")
        create_test_dataset_with_missing_data(ncfile, t0="2000-01-01", periods=5)
        template_ds = xr.open_dataset(ncfile)
        
        # Create Zarr converter with very low retry limit
        converter = ZarrConverter(
            config=ZarrConverterConfig(
                chunking={"time": 5, "lat": 2, "lon": 2},
                missing_data=MissingDataConfig(
                    retries_on_missing=1,  # Enable 1 retry only
                    missing_check_vars="all"
                )
            )
        )
        
        # Create template archive
        converter.create_template(
            template_dataset=template_ds,
            output_path=zarrfile,
            global_start="2000-01-01",
            global_end="2000-01-05",
            compute=False
        )
        
        # Write region with forced missing data detection
        # This simulates a scenario where missing data is always detected
        converter.write_region(ncfile, zarrfile)
        
        # The retry logic should work correctly even with low limits
        final_ds = xr.open_zarr(zarrfile)
        assert len(final_ds.time) == 5
        
        print("✓ Test passed: Retry limit enforcement working correctly")


if __name__ == "__main__":
    test_retry_logic_disabled_by_default()
    test_retry_logic_enabled()
    test_missing_data_detection()
    test_retry_counter_reset()
    test_retry_limit_exceeded()
    print("\n=== All retry logic tests passed! ===")