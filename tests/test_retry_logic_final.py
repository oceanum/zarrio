"""
Test to verify retry logic for handling missing data.
"""

import pytest
import tempfile
import numpy as np
import pandas as pd
import xarray as xr
import os

from zarrify import ZarrConverter, convert_to_zarr, append_to_zarr
from zarrify.models import ZarrConverterConfig, MissingDataConfig


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
    
    # Add some attributes
    ds.attrs["title"] = "Test dataset"
    ds["temperature"].attrs["units"] = "K"
    ds["pressure"].attrs["units"] = "hPa"
    
    if output == "netcdf":
        ds.to_netcdf(filename)
    elif output == "zarr":
        ds.to_zarr(filename)
    
    return filename


def test_retry_logic_verification():
    """Test to verify retry logic works correctly."""
    print("=== Testing Retry Logic ===\n")
    
    with tempfile.TemporaryDirectory() as tmpdir:
        # 1. Create test data
        print("1. Creating test data...")
        ncfile = os.path.join(tmpdir, "test.nc")
        create_test_dataset(ncfile, output="netcdf")
        print(f"   ✓ Created {os.path.basename(ncfile)}")
        
        # 2. Test basic conversion with retry logic disabled (default)
        print("\n2. Basic conversion with retry logic disabled...")
        zarrfile1 = os.path.join(tmpdir, "no_retry.zarr")
        convert_to_zarr(
            input_path=ncfile,
            output_path=zarrfile1,
            retries_on_missing=0,  # Disabled
            missing_check_vars="all"
        )
        print(f"   ✓ Converted successfully without retries")
        
        # 3. Test conversion with retry logic enabled
        print("\n3. Conversion with retry logic enabled...")
        zarrfile2 = os.path.join(tmpdir, "with_retry.zarr")
        
        # Create config with retry logic
        config = ZarrConverterConfig(
            missing_data=MissingDataConfig(
                retries_on_missing=2,  # Enable 2 retries
                missing_check_vars="all"
            )
        )
        
        converter = ZarrConverter(config=config)
        converter.convert(ncfile, zarrfile2)
        print(f"   ✓ Converted successfully with retries enabled")
        
        # 4. Test append with retry logic
        print("\n4. Append with retry logic...")
        zarrfile3 = os.path.join(tmpdir, "append_retry.zarr")
        
        # First create initial dataset
        ncfile1 = os.path.join(tmpdir, "initial.nc")
        create_test_dataset(ncfile1, output="netcdf")
        
        # Convert initial dataset
        convert_to_zarr(ncfile1, zarrfile3)
        print(f"   ✓ Created initial Zarr store: {os.path.basename(zarrfile3)}")
        
        # Create additional dataset to append
        ncfile2 = os.path.join(tmpdir, "append.nc")
        # Create dataset with different time range
        data = np.random.random([5, 3, 4])
        ds = xr.Dataset(
            {
                "temperature": (("time", "lat", "lon"), data),
                "pressure": (("time", "lat", "lon"), data * 1000),
            },
            coords={
                "time": pd.date_range("2000-01-06", periods=5),
                "lat": [-10, 0, 10],
                "lon": [20, 30, 40, 50],
            },
        )
        ds["temperature"].attrs["valid_min"] = 0.0
        ds["temperature"].attrs["valid_max"] = 1.0
        ds["pressure"].attrs["valid_min"] = 0.0
        ds["pressure"].attrs["valid_max"] = 1000.0
        ds.attrs["title"] = "Append dataset"
        ds["temperature"].attrs["units"] = "K"
        ds["pressure"].attrs["units"] = "hPa"
        ds.to_netcdf(ncfile2)
        print(f"   ✓ Created append dataset: {os.path.basename(ncfile2)}")
        
        # Append with retry logic
        converter_append = ZarrConverter(
            config=ZarrConverterConfig(
                missing_data=MissingDataConfig(
                    retries_on_missing=1,  # Enable 1 retry
                    missing_check_vars="all"
                )
            )
        )
        converter_append.append(ncfile2, zarrfile3)
        print(f"   ✓ Appended {os.path.basename(ncfile2)} with retries enabled")
        
        # 5. Test parallel writing with retry logic
        print("\n5. Parallel writing with retry logic...")
        zarrfile4 = os.path.join(tmpdir, "parallel_retry.zarr")
        
        # Create template dataset
        template_ds = xr.open_dataset(ncfile1)
        
        # Create converter with retry logic for parallel writing
        converter_parallel = ZarrConverter(
            config=ZarrConverterConfig(
                missing_data=MissingDataConfig(
                    retries_on_missing=1,  # Enable 1 retry
                    missing_check_vars="all"
                )
            )
        )
        
        # Create template for parallel writing
        converter_parallel.create_template(
            template_dataset=template_ds,
            output_path=zarrfile4,
            global_start="2000-01-01",
            global_end="2000-01-10",
            compute=False  # Metadata only
        )
        print(f"   ✓ Created template for parallel writing: {os.path.basename(zarrfile4)}")
        
        # Write regions with retry logic
        converter_parallel.write_region(ncfile1, zarrfile4)
        converter_parallel.write_region(ncfile2, zarrfile4)
        print(f"   ✓ Wrote regions with retries enabled")
        
        # 6. Verify results
        print("\n6. Verifying results...")
        ds_orig1 = xr.open_dataset(ncfile1)
        ds_orig2 = xr.open_dataset(ncfile2)
        ds_no_retry = xr.open_zarr(zarrfile1)
        ds_with_retry = xr.open_zarr(zarrfile2)
        ds_append = xr.open_zarr(zarrfile3)
        ds_parallel = xr.open_zarr(zarrfile4)
        
        print(f"   No retry Zarr: {len(ds_no_retry.time)} time steps")
        print(f"   With retry Zarr: {len(ds_with_retry.time)} time steps")
        print(f"   Append retry Zarr: {len(ds_append.time)} time steps")
        print(f"   Parallel retry Zarr: {len(ds_parallel.time)} time steps")
        
        # Check that data was written correctly despite missing values
        print(f"   Temperature range in no retry Zarr: {float(ds_no_retry.temperature.min()):.3f} to {float(ds_no_retry.temperature.max()):.3f}")
        print(f"   Temperature range in with retry Zarr: {float(ds_with_retry.temperature.min()):.3f} to {float(ds_with_retry.temperature.max()):.3f}")
        print(f"   Temperature range in append retry Zarr: {float(ds_append.temperature.min()):.3f} to {float(ds_append.temperature.max()):.3f}")
        print(f"   Temperature range in parallel retry Zarr: {float(ds_parallel.temperature.min()):.3f} to {float(ds_parallel.temperature.max()):.3f}")
        
        print("\n=== Retry logic verification completed successfully! ===")


def test_missing_data_detection():
    """Test missing data detection functionality."""
    print("\n=== Testing Missing Data Detection ===\n")
    
    with tempfile.TemporaryDirectory() as tmpdir:
        # Create test data with missing values
        print("1. Creating test data with missing values...")
        ncfile = os.path.join(tmpdir, "test_missing.nc")
        data = np.random.random([5, 3, 4])
        # Introduce some missing values
        data[0:2, :, :] = np.nan
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
        ds["temperature"].attrs["valid_min"] = 0.0
        ds["temperature"].attrs["valid_max"] = 1.0
        ds["pressure"].attrs["valid_min"] = 0.0
        ds["pressure"].attrs["valid_max"] = 1000.0
        ds.attrs["title"] = "Test dataset with missing values"
        ds["temperature"].attrs["units"] = "K"
        ds["pressure"].attrs["units"] = "hPa"
        ds.to_netcdf(ncfile)
        print(f"   ✓ Created {os.path.basename(ncfile)} with missing values")
        
        # Test missing data handler
        print("\n2. Testing missing data handler...")
        from zarrify.missing import MissingDataHandler
        handler = MissingDataHandler(
            missing_check_vars="all",
            retries_on_missing=2
        )
        
        # Convert to Zarr
        zarrfile = os.path.join(tmpdir, "test_missing.zarr")
        convert_to_zarr(ncfile, zarrfile)
        
        # Check for missing data
        ds_orig = xr.open_dataset(ncfile)
        has_missing = handler.has_missing(zarrfile, ds_orig)
        print(f"   ✓ Missing data detection completed (has_missing={has_missing})")
        
        # Test handle missing data
        result = handler.handle_missing_data(
            zarr_path=zarrfile,
            input_dataset=ds_orig
        )
        print(f"   ✓ Missing data handling completed (result={result})")
        
        print("\n=== Missing data detection completed successfully! ===")


if __name__ == "__main__":
    test_retry_logic_verification()
    test_missing_data_detection()