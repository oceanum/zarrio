"""
Example demonstrating parallel writing functionality in zarrio.
"""

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

import numpy as np
import pandas as pd
import xarray as xr
import tempfile
import os
from pathlib import Path

from zarrio import ZarrConverter


def create_sample_data_files(tmpdir: str, num_files: int = 5) -> list:
    """Create sample NetCDF files for parallel processing."""
    files = []
    
    for i in range(num_files):
        # Create data for different time periods
        start_date = f"2023-01-{i*5+1:02d}"  # 2023-01-01, 2023-01-06, 2023-01-11, etc.
        periods = 5  # 5 days per file
        
        # Create test data
        data = np.random.random([periods, 10, 15])  # time, lat, lon
        ds = xr.Dataset(
            {
                "temperature": (("time", "lat", "lon"), data),
                "pressure": (("time", "lat", "lon"), data * 1000),
            },
            coords={
                "time": pd.date_range(start_date, periods=periods),
                "lat": np.linspace(-45, 45, 10),
                "lon": np.linspace(-90, 90, 15),
            },
        )
        
        # Add attributes
        ds.attrs["title"] = f"Sample data {i+1}"
        ds["temperature"].attrs["units"] = "degC"
        ds["pressure"].attrs["units"] = "hPa"
        
        # Save as NetCDF
        filename = os.path.join(tmpdir, f"sample_{i+1:03d}.nc")
        ds.to_netcdf(filename)
        files.append(filename)
        print(f"Created {filename} with time range {ds.time.values[0]} to {ds.time.values[-1]}")
    
    return files


def demonstrate_parallel_writing():
    """Demonstrate parallel writing functionality."""
    with tempfile.TemporaryDirectory() as tmpdir:
        print("=== zarrio Parallel Writing Demo ===\n")
        
        # Step 1: Create sample data files
        print("1. Creating sample NetCDF files...")
        data_files = create_sample_data_files(tmpdir, num_files=5)
        
        # Step 2: Use first file as template
        print("\n2. Creating template from first file...")
        template_ds = xr.open_dataset(data_files[0])
        
        # Step 3: Create Zarr converter with chunking
        converter = ZarrConverter(
            chunking={"time": 10, "lat": 5, "lon": 8},
            compression="blosc:zstd:1"
        )
        
        # Step 4: Create template archive covering full time range
        zarr_archive = os.path.join(tmpdir, "parallel_archive.zarr")
        print(f"3. Creating template archive at {zarr_archive}...")
        
        converter.create_template(
            template_dataset=template_ds,
            output_path=zarr_archive,
            global_start="2023-01-01",
            global_end="2023-01-25",  # Covering all 25 days
            compute=False  # Metadata only, no data computation
        )
        
        print("   Template created successfully!")
        
        # Step 5: Write regions in "parallel" (simulating parallel processes)
        print("\n4. Writing data regions to archive...")
        for i, data_file in enumerate(data_files):
            print(f"   Writing {os.path.basename(data_file)}...")
            converter.write_region(data_file, zarr_archive)
        
        print("   All regions written successfully!")
        
        # Step 6: Verify the result
        print("\n5. Verifying final archive...")
        final_ds = xr.open_zarr(zarr_archive)
        
        print(f"   Final archive time range: {final_ds.time.values[0]} to {final_ds.time.values[-1]}")
        print(f"   Total time steps: {len(final_ds.time)}")
        print(f"   Spatial dimensions: {final_ds.dims}")
        print(f"   Variables: {list(final_ds.data_vars.keys())}")
        
        # Check data integrity
        first_file_ds = xr.open_dataset(data_files[0])
        print(f"\n6. Data integrity check:")
        print(f"   Original temperature range: {first_file_ds.temperature.min().values:.3f} to {first_file_ds.temperature.max().values:.3f}")
        print(f"   Archive temperature range: {final_ds.temperature.min().values:.3f} to {final_ds.temperature.max().values:.3f}")
        
        print("\n=== Demo completed successfully! ===")


def demonstrate_cli_parallel_writing():
    """Demonstrate CLI-based parallel writing."""
    with tempfile.TemporaryDirectory() as tmpdir:
        print("\n=== CLI Parallel Writing Demo ===\n")
        
        # Create sample data
        data_files = create_sample_data_files(tmpdir, num_files=3)
        template_file = data_files[0]
        zarr_archive = os.path.join(tmpdir, "cli_archive.zarr")
        
        print("1. Creating template archive using CLI...")
        # In practice, you would run these commands in parallel processes:
        # zarrio create-template template.nc archive.zarr --global-start 2023-01-01 --global-end 2023-01-15
        # zarrio write-region data1.nc archive.zarr
        # zarrio write-region data2.nc archive.zarr
        # zarrio write-region data3.nc archive.zarr
        
        # Simulate the CLI operations
        template_ds = xr.open_dataset(template_file)
        converter = ZarrConverter()
        
        # Create template
        converter.create_template(
            template_dataset=template_ds,
            output_path=zarr_archive,
            global_start="2023-01-01",
            global_end="2023-01-15",
            compute=False
        )
        print("   Template created!")
        
        # Write regions (in practice, these would be separate CLI calls in parallel)
        for data_file in data_files:
            converter.write_region(data_file, zarr_archive)
            print(f"   Wrote {os.path.basename(data_file)}")
        
        final_ds = xr.open_zarr(zarr_archive)
        print(f"\n   Final archive: {len(final_ds.time)} time steps")
        print("=== CLI demo completed! ===")


if __name__ == "__main__":
    demonstrate_parallel_writing()
    demonstrate_cli_parallel_writing()