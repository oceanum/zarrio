"""
Tests for missing data handling in zarrify.
"""

import pytest
import tempfile
import numpy as np
import pandas as pd
import xarray as xr
import os

from zarrify.missing import (
    MissingDataHandler,
    check_missing_data,
    handle_missing_with_retry
)


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


def test_missing_data_handler_initialization():
    """Test MissingDataHandler initialization."""
    # Test valid initialization
    handler = MissingDataHandler(
        missing_check_vars="all",
        retries_on_missing=3,
        time_dim="time"
    )
    assert handler.missing_check_vars == "all"
    assert handler.retries_on_missing == 3
    assert handler.time_dim == "time"
    assert handler.retried_on_missing == 0
    
    # Test with list of variables
    handler = MissingDataHandler(
        missing_check_vars=["temperature", "pressure"],
        retries_on_missing=2
    )
    assert handler.missing_check_vars == ["temperature", "pressure"]
    assert handler.retries_on_missing == 2
    
    # Test with None
    handler = MissingDataHandler(
        missing_check_vars=None,
        retries_on_missing=0
    )
    assert handler.missing_check_vars is None
    assert handler.retries_on_missing == 0


def test_missing_data_handler_has_missing():
    """Test missing data detection."""
    with tempfile.TemporaryDirectory() as tmpdir:
        # Create test data
        ncfile = os.path.join(tmpdir, "test.nc")
        create_test_dataset_with_missing_data(ncfile, t0="2000-01-01", periods=5)
        ds = xr.open_dataset(ncfile)
        
        # Create handler
        handler = MissingDataHandler(
            missing_check_vars="all",
            retries_on_missing=0
        )
        
        # Test with no missing data (should return False)
        has_missing = handler.has_missing(ncfile, ds)
        assert has_missing is False  # No missing data in original file
        
        # Test with missing data pattern
        ncfile_missing = os.path.join(tmpdir, "test_missing.nc")
        create_test_dataset_with_missing_data(
            ncfile_missing, 
            t0="2000-01-01", 
            periods=5, 
            missing_pattern="middle"
        )
        ds_missing = xr.open_dataset(ncfile_missing)
        
        # Test with missing data (should return False because we're comparing to itself)
        has_missing = handler.has_missing(ncfile_missing, ds_missing)
        assert has_missing is False  # Comparing to itself, so no missing data
        
        # Test with invalid missing_check_vars
        handler_invalid = MissingDataHandler(
            missing_check_vars="invalid",
            retries_on_missing=0
        )
        has_missing = handler_invalid.has_missing(ncfile, ds)
        assert has_missing is False  # Invalid config, should return False


def test_missing_data_handler_handle_missing_data():
    """Test handling missing data with retry logic."""
    with tempfile.TemporaryDirectory() as tmpdir:
        # Create test data
        ncfile = os.path.join(tmpdir, "test.nc")
        create_test_dataset_with_missing_data(ncfile, t0="2000-01-01", periods=5)
        ds = xr.open_dataset(ncfile)
        
        # Create handler with retries
        handler = MissingDataHandler(
            missing_check_vars="all",
            retries_on_missing=2
        )
        
        # Test handling with no missing data (should succeed)
        result = handler.handle_missing_data(
            zarr_path=ncfile,
            input_dataset=ds,
            region=None,
            write_func=None  # No retry needed
        )
        assert result is True  # Success
        assert handler.retried_on_missing == 0  # No retries needed
        
        # Test retry counter reset
        handler.retried_on_missing = 1
        handler.reset_retry_count()
        assert handler.retried_on_missing == 0


def test_check_missing_data_function():
    """Test check_missing_data convenience function."""
    with tempfile.TemporaryDirectory() as tmpdir:
        # Create test data
        ncfile = os.path.join(tmpdir, "test.nc")
        create_test_dataset_with_missing_data(ncfile, t0="2000-01-01", periods=5)
        ds = xr.open_dataset(ncfile)
        
        # Test with all variables
        has_missing = check_missing_data(
            zarr_path=ncfile,
            input_dataset=ds,
            missing_check_vars="all"
        )
        assert has_missing is False  # No missing data
        
        # Test with specific variables
        has_missing = check_missing_data(
            zarr_path=ncfile,
            input_dataset=ds,
            missing_check_vars=["temperature", "pressure"]
        )
        assert has_missing is False  # No missing data
        
        # Test with None
        has_missing = check_missing_data(
            zarr_path=ncfile,
            input_dataset=ds,
            missing_check_vars=None
        )
        assert has_missing is False  # No variables to check


def test_handle_missing_with_retry_function():
    """Test handle_missing_with_retry convenience function."""
    with tempfile.TemporaryDirectory() as tmpdir:
        # Create test data
        ncfile = os.path.join(tmpdir, "test.nc")
        create_test_dataset_with_missing_data(ncfile, t0="2000-01-01", periods=5)
        ds = xr.open_dataset(ncfile)
        
        # Test with retry logic
        def dummy_write_func(**kwargs):
            pass
        
        result = handle_missing_with_retry(
            zarr_path=ncfile,
            input_dataset=ds,
            write_func=dummy_write_func,
            missing_check_vars="all",
            retries_on_missing=2
        )
        assert result is True  # Success (no missing data to retry)


def test_missing_data_handler_edge_cases():
    """Test edge cases for missing data handling."""
    # Test with empty dataset
    handler = MissingDataHandler()
    assert handler.missing_check_vars == "all"
    assert handler.retries_on_missing == 0
    assert handler.time_dim == "time"
    
    # Test with negative retries (should be handled by Pydantic validation)
    # This would be caught at config validation level, not at runtime


if __name__ == "__main__":
    pytest.main([__file__])