"""
Test script to demonstrate the performance testing feature of the analyze command.
"""

import numpy as np
import xarray as xr
import pandas as pd
import tempfile
import os


def create_sample_dataset():
    """Create a sample dataset for testing."""
    # Create smaller sample data for faster testing
    time = pd.date_range("2000-01-01", periods=20, freq="D")
    lat = np.linspace(-90, 90, 36)
    lon = np.linspace(-180, 180, 72)
    
    # Create temperature data with known range
    temperature = 20 + 10 * np.random.randn(20, 36, 72)  # Mean 20°C, std 10°C
    pressure = 101325 + 5000 * np.random.randn(20, 36, 72)  # Mean ~101325 Pa
    
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
    """Create a sample NetCDF file and test the performance analysis."""
    print("Creating sample dataset...")
    ds = create_sample_dataset()
    
    # Save as NetCDF
    with tempfile.NamedTemporaryFile(suffix=".nc", delete=False) as tmp_nc:
        nc_path = tmp_nc.name
        
    ds.to_netcdf(nc_path)
    print(f"Saved sample dataset to {nc_path}")
    
    # Test the analyze command with performance testing
    print("\nTesting analyze command with performance testing...")
    os.system(f"cd /home/tdurrant/source/onzarr/zarrio && python -m zarrio.cli analyze {nc_path} --test-performance")
    
    # Clean up
    os.unlink(nc_path)
    print("\nTest completed!")


if __name__ == "__main__":
    main()