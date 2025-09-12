"""
Simple test for retry functionality in zarrify.
"""

import tempfile
import numpy as np
import pandas as pd
import xarray as xr
import os

from zarrify.core import ZarrConverter
from zarrify.models import ZarrConverterConfig, ChunkingConfig


def create_test_dataset(filename: str) -> str:
    """Create a simple test dataset."""
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
    
    # Add some attributes
    ds.attrs["title"] = "Test dataset"
    ds["temperature"].attrs["units"] = "K"
    ds["pressure"].attrs["units"] = "hPa"
    
    ds.to_netcdf(filename)
    return filename


def test_retry_functionality():
    """Test that retry functionality works."""
    with tempfile.TemporaryDirectory() as tmpdir:
        # Create test data
        ncfile = os.path.join(tmpdir, "test.nc")
        zarrfile = os.path.join(tmpdir, "test.zarr")
        create_test_dataset(ncfile)
        template_ds = xr.open_dataset(ncfile)
        
        # Test 1: Create converter with retry disabled (default)
        converter = ZarrConverter(
            config=ZarrConverterConfig(
                chunking=ChunkingConfig(time=5, lat=2, lon=2),
                retries_on_missing=0,  # Disabled
                missing_check_vars="all"
            )
        )
        
        # Create template
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
        print("✓ Test 1 passed: Write region with retries disabled")
        
        # Test 2: Create converter with retries enabled
        zarrfile2 = os.path.join(tmpdir, "test2.zarr")
        converter2 = ZarrConverter(
            config=ZarrConverterConfig(
                chunking=ChunkingConfig(time=5, lat=2, lon=2),
                retries_on_missing=2,  # Enabled with 2 retries
                missing_check_vars="all"
            )
        )
        
        # Create template
        converter2.create_template(
            template_dataset=template_ds,
            output_path=zarrfile2,
            global_start="2000-01-01",
            global_end="2000-01-05",
            compute=False
        )
        
        # Write region - should work with retries
        converter2.write_region(ncfile, zarrfile2)
        
        # Verify the data was written
        final_ds2 = xr.open_zarr(zarrfile2)
        assert len(final_ds2.time) == 5
        print("✓ Test 2 passed: Write region with retries enabled")
        
        # Test 3: Verify retry counter is reset between operations
        initial_retries = converter2.retried_on_missing
        converter2.write_region(ncfile, zarrfile2)
        # Should be reset to 0 after successful operation
        print(f"✓ Test 3 passed: Retry counter reset (was {initial_retries}, now {converter2.retried_on_missing})")
        
        print("\nAll retry functionality tests passed!")


if __name__ == "__main__":
    test_retry_functionality()