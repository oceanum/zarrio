"""
Test script to create a sample NetCDF file for testing the analyze command.
"""

import numpy as np
import xarray as xr
import pandas as pd
import tempfile
import os


def create_sample_dataset():
    """Create a sample dataset for testing."""
    # Create sample data
    time = pd.date_range("2000-01-01", periods=100, freq="D")
    lat = np.linspace(-90, 90, 180)
    lon = np.linspace(-180, 180, 360)
    
    # Create temperature data with known range
    temperature = 20 + 10 * np.random.randn(100, 180, 360)  # Mean 20°C, std 10°C
    pressure = 101325 + 5000 * np.random.randn(100, 180, 360)  # Mean ~101325 Pa
    
    # Create dataset
    ds = xr.Dataset(
        {
            "temperature": (("time", "lat", "lon"), temperature),
            "pressure": (("time", "lat", "lon"), pressure),
        },
        coords={
            "time": time,
            "lat": lat,
            "lon": lon,
        },
    )
    
    # Add some attributes
    ds["temperature"].attrs["valid_min"] = -50.0
    ds["temperature"].attrs["valid_max"] = 50.0
    ds["pressure"].attrs["units"] = "Pa"
    
    return ds


def main():
    """Create a sample NetCDF file."""
    print("Creating sample dataset...")
    ds = create_sample_dataset()
    
    # Save as NetCDF
    with tempfile.NamedTemporaryFile(suffix=".nc", delete=False) as tmp_nc:
        nc_path = tmp_nc.name
        
    ds.to_netcdf(nc_path)
    print(f"Saved sample dataset to {nc_path}")
    
    # Test the analyze command
    print("\nTesting analyze command...")
    os.system(f"cd /home/tdurrant/source/onzarr/zarrify && python -m zarrify.cli analyze {nc_path}")
    
    # Clean up
    os.unlink(nc_path)
    print("\nTest completed!")


if __name__ == "__main__":
    main()