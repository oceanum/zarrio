"""
Demonstration of retry logic for handling missing data in zarrify.
"""

import tempfile
import numpy as np
import pandas as pd
import xarray as xr
import os

from zarrify import ZarrConverter, convert_to_zarr, append_to_zarr
from zarrify.models import (
    ZarrConverterConfig, 
    MissingDataConfig,
    ChunkingConfig,
    PackingConfig,
    CompressionConfig
)


def create_sample_dataset(filename: str, t0: str = "2000-01-01", periods: int = 10) -> str:
    """Create a sample dataset for demonstration."""
    # Create sample data
    np.random.seed(42)  # For reproducible results
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
    
    # Add valid range attributes for packing
    ds["temperature"].attrs["valid_min"] = 0.0
    ds["temperature"].attrs["valid_max"] = 1.0
    ds["pressure"].attrs["valid_min"] = 0.0
    ds["pressure"].attrs["valid_max"] = 1000.0
    
    # Add some attributes
    ds.attrs["title"] = "Sample dataset for retry logic demo"
    ds["temperature"].attrs["units"] = "K"
    ds["pressure"].attrs["units"] = "hPa"
    
    ds.to_netcdf(filename)
    return filename


def demonstrate_retry_logic():
    """Demonstrate retry logic for handling missing data."""
    print("=== zarrify Retry Logic Demo ===\n")
    
    with tempfile.TemporaryDirectory() as tmpdir:
        # 1. Create sample data
        print("1. Creating sample data...")
        ncfile = os.path.join(tmpdir, "sample.nc")
        create_sample_dataset(ncfile, t0="2000-01-01", periods=10)
        print(f"   ✓ Created {os.path.basename(ncfile)}")
        
        # 2. Demonstrate conversion without retry logic (default)
        print("\n2. Conversion without retry logic (default)...")
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
        
        # Create config with retry logic
        config = ZarrConverterConfig(
            chunking=ChunkingConfig(time=5, lat=2, lon=2),
            compression=CompressionConfig(method="blosc:zstd:3"),
            packing=PackingConfig(enabled=True, bits=16),
            missing_data=MissingDataConfig(
                retries_on_missing=3,  # Enable 3 retries
                missing_check_vars="all"
            )
        )
        
        converter = ZarrConverter(config=config)
        zarrfile2 = os.path.join(tmpdir, "with_retry.zarr")
        converter.convert(ncfile, zarrfile2)
        print(f"   ✓ Converted successfully with retries enabled")
        
        # 4. Demonstrate parallel writing with retry logic
        print("\n4. Parallel writing with retry logic...")
        zarrfile3 = os.path.join(tmpdir, "parallel_retry.zarr")
        
        # Create template dataset
        template_ds = xr.open_dataset(ncfile)
        
        # Create converter with retry logic for parallel writing
        converter_parallel = ZarrConverter(
            config=ZarrConverterConfig(
                chunking=ChunkingConfig(time=5, lat=2, lon=2),
                compression=CompressionConfig(method="blosc:zstd:3"),
                packing=PackingConfig(enabled=True, bits=16),
                missing_data=MissingDataConfig(
                    retries_on_missing=2,  # Enable 2 retries
                    missing_check_vars="all"
                )
            )
        )
        
        # Create template for parallel writing
        converter_parallel.create_template(
            template_dataset=template_ds,
            output_path=zarrfile3,
            global_start="2000-01-01",
            global_end="2000-01-10",
            compute=False  # Metadata only
        )
        print(f"   ✓ Created template for parallel writing: {os.path.basename(zarrfile3)}")
        
        # Write regions with retry logic
        converter_parallel.write_region(ncfile, zarrfile3)
        print(f"   ✓ Wrote region with retries enabled")
        
        # 5. Verify results
        print("\n5. Verifying results...")
        ds1 = xr.open_zarr(zarrfile1)
        ds2 = xr.open_zarr(zarrfile2)
        ds3 = xr.open_zarr(zarrfile3)
        
        print(f"   No retry Zarr: {len(ds1.time)} time steps")
        print(f"   With retry Zarr: {len(ds2.time)} time steps")
        print(f"   Parallel retry Zarr: {len(ds3.time)} time steps")
        
        # Check that data was written correctly
        print(f"   Temperature range in no retry Zarr: {float(ds1.temperature.min()):.3f} to {float(ds1.temperature.max()):.3f}")
        print(f"   Temperature range in with retry Zarr: {float(ds2.temperature.min()):.3f} to {float(ds2.temperature.max()):.3f}")
        print(f"   Temperature range in parallel retry Zarr: {float(ds3.temperature.min()):.3f} to {float(ds3.temperature.max()):.3f}")
        
        print("\n=== Retry logic demonstration completed successfully! ===")


def explain_retry_mechanism():
    """Explain how the retry mechanism works."""
    print("\n=== Retry Mechanism Explanation ===\n")
    
    print("zarrify implements intelligent retry logic for handling missing data:")
    
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
    print("   from zarrify import ZarrConverter")
    print("   from zarrify.models import ZarrConverterConfig, MissingDataConfig")
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
    print("   zarrify convert input.nc output.zarr --retries-on-missing 3")
    print("   ")
    print("   # Append with retry logic")
    print("   zarrify append new_data.nc existing.zarr --retries-on-missing 2")
    print("   ")
    print("   # Create template with retry logic")
    print("   zarrify create-template template.nc archive.zarr --retries-on-missing 1")
    print("   ")
    print("   # Write region with retry logic")
    print("   zarrify write-region data.nc archive.zarr --retries-on-missing 2")
    print("   ```")
    
    print("\n   Configuration Files (YAML):")
    print("   ```yaml")
    print("   # config.yaml")
    print("   missing_data:")
    print("     retries_on_missing: 3  # Enable 3 retries")
    print("     missing_check_vars: \"all\"  # Check all variables")
    print("   chunking:")
    print("     time: 100")
    print("     lat: 50")
    print("     lon: 100")
    print("   compression:")
    print("     method: blosc:zstd:3")
    print("   packing:")
    print("     enabled: true")
    print("     bits: 16")
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