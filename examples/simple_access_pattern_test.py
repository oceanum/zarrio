"""
Simple example demonstrating access pattern speed testing with zarrify.

This example shows how different chunking strategies affect performance
for different data access patterns.
"""

import numpy as np
import pandas as pd
import xarray as xr
import tempfile
import os
import time

from zarrify import ZarrConverter
from zarrify.models import ZarrConverterConfig, ChunkingConfig


def create_sample_data():
    """Create sample climate data."""
    times = pd.date_range("2020-01-01", periods=365, freq="D")
    lats = np.linspace(-90, 90, 181)
    lons = np.linspace(-180, 180, 361)
    
    # Create random data with some structure
    np.random.seed(42)
    temperature = 20 + 10 * np.random.random((365, 181, 361))
    pressure = 1013 + 50 * np.random.random((365, 181, 361))
    
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
    
    return ds


def time_series_access(ds):
    """Access time series at a fixed location."""
    # Extract time series for a fixed location
    return ds.temperature[:, 90, 180].values


def spatial_slice_access(ds):
    """Access spatial slice at a fixed time."""
    # Extract spatial slice for a fixed time
    return ds.temperature[182, :, :].values


def test_chunking_strategy(input_path, output_path, chunking_config, access_func, access_name):
    """
    Test a specific chunking strategy with an access pattern.
    
    Returns:
        Time taken for access in seconds
    """
    # Create Zarr with specific chunking
    config = ZarrConverterConfig(chunking=chunking_config)
    converter = ZarrConverter(config=config)
    converter.convert(input_path, output_path)
    
    # Time the access pattern
    ds = xr.open_zarr(output_path)
    start = time.perf_counter()
    result = access_func(ds)
    end = time.perf_counter()
    ds.close()
    
    return end - start


def main():
    """Run the speed comparison."""
    print("Zarrify Access Pattern Performance Test")
    print("=" * 45)
    
    with tempfile.TemporaryDirectory() as tmpdir:
        # Create and save sample data
        ds = create_sample_data()
        nc_file = os.path.join(tmpdir, "sample.nc")
        ds.to_netcdf(nc_file)
        
        # Define chunking strategies
        temporal_chunking = ChunkingConfig(time=100, lat=30, lon=60)
        spatial_chunking = ChunkingConfig(time=10, lat=90, lon=180)
        balanced_chunking = ChunkingConfig(time=30, lat=60, lon=90)
        
        # Test temporal access pattern
        print("\nTesting Temporal Access Pattern (time series at fixed location):")
        print("-" * 60)
        
        temporal_zarr = os.path.join(tmpdir, "temporal.zarr")
        spatial_zarr = os.path.join(tmpdir, "spatial.zarr")
        balanced_zarr = os.path.join(tmpdir, "balanced.zarr")
        
        time_temporal = test_chunking_strategy(nc_file, temporal_zarr, temporal_chunking, time_series_access, "temporal")
        time_spatial = test_chunking_strategy(nc_file, spatial_zarr, spatial_chunking, time_series_access, "temporal")
        time_balanced = test_chunking_strategy(nc_file, balanced_zarr, balanced_chunking, time_series_access, "temporal")
        
        print(f"Temporal chunking (time=100):     {time_temporal:.4f}s")
        print(f"Spatial chunking (time=10):       {time_spatial:.4f}s")
        print(f"Balanced chunking (time=30):      {time_balanced:.4f}s")
        
        # Test spatial access pattern
        print("\nTesting Spatial Access Pattern (spatial slice at fixed time):")
        print("-" * 60)
        
        time_temporal = test_chunking_strategy(nc_file, temporal_zarr, temporal_chunking, spatial_slice_access, "spatial")
        time_spatial = test_chunking_strategy(nc_file, spatial_zarr, spatial_chunking, spatial_slice_access, "spatial")
        time_balanced = test_chunking_strategy(nc_file, balanced_zarr, balanced_chunking, spatial_slice_access, "spatial")
        
        print(f"Temporal chunking (lat=30, lon=60):  {time_temporal:.4f}s")
        print(f"Spatial chunking (lat=90, lon=180):  {time_spatial:.4f}s")
        print(f"Balanced chunking (lat=60, lon=90):  {time_balanced:.4f}s")
        
        # Summary
        print("\nSummary:")
        print("-" * 20)
        print("For time series access: Temporal chunking is fastest")
        print("For spatial slice access: Spatial chunking is fastest")
        print("Choose chunking strategy based on your primary access pattern")


if __name__ == "__main__":
    main()