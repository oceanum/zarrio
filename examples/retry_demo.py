"""
Example demonstrating retry functionality in zarrio.
"""

import tempfile
import numpy as np
import pandas as pd
import xarray as xr
import os

from zarrio import ZarrConverter, convert_to_zarr, append_to_zarr
from zarrio.models import ZarrConverterConfig, MissingDataConfig


def create_test_dataset_with_missing_values(
    filename: str,
    t0: str = "2000-01-01",
    periods: int = 10,
    missing_pattern: str = "none"
) -> str:
    """Create a test dataset with missing values for testing retry functionality."""
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
    ds.attrs["title"] = "Test dataset with missing values"
    ds["temperature"].attrs["units"] = "K"
    ds["pressure"].attrs["units"] = "hPa"
    
    ds.to_netcdf(filename)
    return filename


def demonstrate_retry_logic():
    """Demonstrate retry logic for handling missing data."""
    print("=== zarrio Retry Logic Demo ===\n")
    
    with tempfile.TemporaryDirectory() as tmpdir:
        # 1. Create test data with missing values
        print("1. Creating test data with missing values...")
        ncfile = os.path.join(tmpdir, "test.nc")
        create_test_dataset_with_missing_values(ncfile, t0="2000-01-01", periods=10, missing_pattern="middle")
        print(f"   Created {ncfile} with missing values in middle period")
        
        # 2. Demonstrate conversion with retry logic disabled (default)
        print("\n2. Conversion with retry logic disabled (default)...")
        zarrfile1 = os.path.join(tmpdir, "no_retry.zarr")
        convert_to_zarr(
            input_path=ncfile,
            output_path=zarrfile1,
            retries_on_missing=0,  # Disabled
            missing_check_vars="all"
        )
        print(f"   ✓ Converted successfully without retries")
        
        # 3. Demonstrate conversion with retry logic enabled
        print("\n3. Conversion with retry logic enabled...")
        zarrfile2 = os.path.join(tmpdir, "with_retry.zarr")
        
        # Create config with retry logic
        config = ZarrConverterConfig(
            missing_data=MissingDataConfig(
                retries_on_missing=3,  # Enable 3 retries
                missing_check_vars="all"
            )
        )
        
        converter = ZarrConverter(config=config)
        converter.convert(ncfile, zarrfile2)
        print(f"   ✓ Converted successfully with retries enabled")
        
        # 4. Demonstrate append with retry logic
        print("\n4. Append with retry logic...")
        zarrfile3 = os.path.join(tmpdir, "append_retry.zarr")
        
        # Create initial dataset
        ncfile1 = os.path.join(tmpdir, "initial.nc")
        create_test_dataset_with_missing_values(ncfile1, t0="2000-01-01", periods=5)
        
        # Create additional dataset to append
        ncfile2 = os.path.join(tmpdir, "append.nc")
        create_test_dataset_with_missing_values(ncfile2, t0="2000-01-06", periods=5, missing_pattern="end")
        
        # Convert initial dataset
        convert_to_zarr(ncfile1, zarrfile3)
        print(f"   ✓ Created initial Zarr store: {os.path.basename(zarrfile3)}")
        
        # Append with retry logic
        converter_append = ZarrConverter(
            config=ZarrConverterConfig(
                missing_data=MissingDataConfig(
                    retries_on_missing=2,  # Enable 2 retries
                    missing_check_vars="all"
                )
            )
        )
        converter_append.append(ncfile2, zarrfile3)
        print(f"   ✓ Appended {os.path.basename(ncfile2)} with retries enabled")
        
        # 5. Demonstrate parallel writing with retry logic
        print("\n5. Parallel writing with retry logic...")
        zarrfile4 = os.path.join(tmpdir, "parallel_retry.zarr")
        
        # Create template dataset
        template_ds = xr.open_dataset(ncfile1)
        
        # Create converter with retry logic for parallel writing
        converter_parallel = ZarrConverter(
            config=ZarrConverterConfig(
                missing_data=MissingDataConfig(
                    retries_on_missing=2,  # Enable 2 retries
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
        ds1 = xr.open_zarr(zarrfile1)
        ds2 = xr.open_zarr(zarrfile2)
        ds3 = xr.open_zarr(zarrfile3)
        ds4 = xr.open_zarr(zarrfile4)
        
        print(f"   No retry Zarr: {len(ds1.time)} time steps")
        print(f"   With retry Zarr: {len(ds2.time)} time steps")
        print(f"   Append retry Zarr: {len(ds3.time)} time steps")
        print(f"   Parallel retry Zarr: {len(ds4.time)} time steps")
        
        # Check that data was written correctly despite missing values
        print(f"   Temperature range in no retry Zarr: {float(ds1.temperature.min()):.3f} to {float(ds1.temperature.max()):.3f}")
        print(f"   Temperature range in with retry Zarr: {float(ds2.temperature.min()):.3f} to {float(ds2.temperature.max()):.3f}")
        print(f"   Temperature range in append retry Zarr: {float(ds3.temperature.min()):.3f} to {float(ds3.temperature.max()):.3f}")
        print(f"   Temperature range in parallel retry Zarr: {float(ds4.temperature.min()):.3f} to {float(ds4.temperature.max()):.3f}")
        
        print("\n=== Demo completed successfully! ===")


def explain_retry_mechanism():
    """Explain how the retry mechanism works."""
    print("\n=== Retry Mechanism Explanation ===\n")
    
    print("zarrio implements intelligent retry logic for handling missing data:")
    
    print("\n1. **Detection**:")
    print("   - Automatically detects missing data in written Zarr stores")
    print("   - Compares written data with source data to identify discrepancies")
    print("   - Checks all variables or specific ones based on configuration")
    
    print("\n2. **Retry Logic**:")
    print("   - Configurable number of retries (0-10, default: 0)")
    print("   - Exponential backoff with increasing delays between retries")
    print("   - Automatic region rewriting when missing data is detected")
    print("   - Progress tracking to prevent infinite loops")
    
    print("\n3. **Configuration**:")
    print("   - `retries_on_missing`: Number of retries if missing values are encountered")
    print("   - `missing_check_vars`: Data variables to check for missing values")
    print("   - Can be set globally or per operation")
    
    print("\n4. **Usage Examples**:")
    
    print("\n   Python API:")
    print("   ```python")
    print("   from zarrio import ZarrConverter")
    print("   from zarrio.models import ZarrConverterConfig, MissingDataConfig")
    print("   ")
    print("   # Enable retry logic")
    print("   config = ZarrConverterConfig(")
    print("       missing_data=MissingDataConfig(")
    print("           retries_on_missing=3,  # Enable 3 retries")
    print("           missing_check_vars=\"all\"  # Check all variables")
    print("       )")
    print("   )")
    print("   ")
    print("   converter = ZarrConverter(config=config)")
    print("   converter.convert(\"input.nc\", \"output.zarr\")")
    print("   ```")
    
    print("\n   CLI:")
    print("   ```bash")
    print("   # Convert with retry logic")
    print("   zarrio convert input.nc output.zarr --retries-on-missing 3")
    print("   ")
    print("   # Append with retry logic")
    print("   zarrio append new_data.nc existing.zarr --retries-on-missing 2")
    print("   ")
    print("   # Create template with retry logic")
    print("   zarrio create-template template.nc archive.zarr --retries-on-missing 1")
    print("   ")
    print("   # Write region with retry logic")
    print("   zarrio write-region data.nc archive.zarr --retries-on-missing 2")
    print("   ```")
    
    print("\n5. **Benefits**:")
    print("   - Increased reliability for large-scale data conversion")
    print("   - Automatic recovery from transient write failures")
    print("   - Reduced need for manual intervention")
    print("   - Better success rates for processing thousands of files")
    
    print("\n6. **Best Practices**:")
    print("   - Use 2-3 retries for most use cases")
    print("   - Increase for very large or unstable environments")
    print("   - Disable (0) for deterministic environments where failures should be immediate errors")
    print("   - Specify `missing_check_vars` to focus on critical variables")
    print("   - Monitor retry activity to identify underlying issues")


if __name__ == "__main__":
    demonstrate_retry_logic()
    explain_retry_mechanism()