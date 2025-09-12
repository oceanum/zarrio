"""
Example usage of zarrify library.
"""

import numpy as np
import pandas as pd
import xarray as xr
import tempfile
import os

from zarrify import convert_to_zarr, append_to_zarr, ZarrConverter


def create_sample_data():
    """Create sample NetCDF data for demonstration."""
    # Create sample data
    times = pd.date_range("2023-01-01", periods=100, freq="D")
    lats = np.linspace(-90, 90, 181)
    lons = np.linspace(-180, 180, 361)
    
    # Create random data
    np.random.seed(42)
    temperature = 20 + 10 * np.random.random((100, 181, 361))
    pressure = 1013 + 50 * np.random.random((100, 181, 361))
    
    # Create dataset
    ds = xr.Dataset(
        {
            "temperature": (("time", "lat", "lon"), temperature),
            "pressure": (("time", "lat", "lon"), pressure),
        },
        coords={
            "time": times,
            "lat": lats,
            "lon": lons,
        },
    )
    
    # Add attributes
    ds.attrs["title"] = "Sample meteorological data"
    ds.attrs["institution"] = "Oceanum"
    ds["temperature"].attrs["units"] = "degC"
    ds["pressure"].attrs["units"] = "hPa"
    
    return ds


def main():
    """Demonstrate zarrify usage."""
    with tempfile.TemporaryDirectory() as tmpdir:
        # Create sample data
        print("Creating sample data...")
        ds = create_sample_data()
        
        # Save as NetCDF
        nc_file = os.path.join(tmpdir, "sample.nc")
        ds.to_netcdf(nc_file)
        print(f"Saved sample data to {nc_file}")
        
        # Example 1: Simple conversion
        print("\n1. Simple conversion:")
        zarr_file1 = os.path.join(tmpdir, "sample_simple.zarr")
        convert_to_zarr(nc_file, zarr_file1)
        print(f"Converted to Zarr: {zarr_file1}")
        
        # Example 2: Conversion with chunking
        print("\n2. Conversion with chunking:")
        zarr_file2 = os.path.join(tmpdir, "sample_chunked.zarr")
        convert_to_zarr(
            nc_file,
            zarr_file2,
            chunking={"time": 10, "lat": 90, "lon": 180}
        )
        print(f"Converted to chunked Zarr: {zarr_file2}")
        
        # Example 3: Conversion with compression
        print("\n3. Conversion with compression:")
        zarr_file3 = os.path.join(tmpdir, "sample_compressed.zarr")
        convert_to_zarr(
            nc_file,
            zarr_file3,
            compression="blosc:zstd:3"
        )
        print(f"Converted to compressed Zarr: {zarr_file3}")
        
        # Example 4: Conversion with packing
        print("\n4. Conversion with packing:")
        # Add valid range attributes for packing
        ds_packed = ds.copy()
        ds_packed["temperature"].attrs["valid_min"] = 0.0
        ds_packed["temperature"].attrs["valid_max"] = 40.0
        ds_packed["pressure"].attrs["valid_min"] = 900.0
        ds_packed["pressure"].attrs["valid_max"] = 1100.0
        
        # Save modified dataset
        nc_file_packed = os.path.join(tmpdir, "sample_packed.nc")
        ds_packed.to_netcdf(nc_file_packed)
        
        zarr_file4 = os.path.join(tmpdir, "sample_packed.zarr")
        convert_to_zarr(
            nc_file_packed,
            zarr_file4,
            packing=True,
            packing_bits=16
        )
        print(f"Converted to packed Zarr: {zarr_file4}")
        
        # Example 5: Using ZarrConverter class
        print("\n5. Using ZarrConverter class:")
        converter = ZarrConverter(
            chunking={"time": 20, "lat": 60, "lon": 120},
            compression="blosc:zstd:1",
            packing=True,
            packing_bits=16
        )
        zarr_file5 = os.path.join(tmpdir, "sample_converter.zarr")
        converter.convert(nc_file_packed, zarr_file5)
        print(f"Converted using ZarrConverter: {zarr_file5}")
        
        # Example 6: Appending data
        print("\n6. Appending data:")
        # Create a second dataset to append
        times2 = pd.date_range("2023-04-11", periods=50, freq="D")
        lats = np.linspace(-90, 90, 181)
        lons = np.linspace(-180, 180, 361)
        temperature2 = 20 + 10 * np.random.random((50, 181, 361))
        pressure2 = 1013 + 50 * np.random.random((50, 181, 361))
        
        ds2 = xr.Dataset(
            {
                "temperature": (("time", "lat", "lon"), temperature2),
                "pressure": (("time", "lat", "lon"), pressure2),
            },
            coords={
                "time": times2,
                "lat": lats,
                "lon": lons,
            },
        )
        
        # Save second dataset
        nc_file2 = os.path.join(tmpdir, "sample2.nc")
        ds2.to_netcdf(nc_file2)
        
        # Append to existing Zarr store
        append_to_zarr(nc_file2, zarr_file1)
        print(f"Appended data to: {zarr_file1}")
        
        # Verify the result
        ds_result = xr.open_zarr(zarr_file1)
        print(f"Resulting dataset has {len(ds_result.time)} time steps")
        
        print("\nDemonstration complete!")


if __name__ == "__main__":
    main()