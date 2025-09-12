"""
Example demonstrating access pattern speed testing with zarrify.
This example creates Zarr archives with different chunking strategies
optimized for different access patterns and measures read performance.
"""

import numpy as np
import pandas as pd
import xarray as xr
import tempfile
import os
import time
from pathlib import Path

from zarrify import ZarrConverter, convert_to_zarr
from zarrify.models import ZarrConverterConfig, ChunkingConfig


def create_sample_data(time_steps=365, lat_points=181, lon_points=361):
    """Create sample climate data for demonstration."""
    print("Creating sample climate data...")
    
    # Create sample data dimensions
    times = pd.date_range("2020-01-01", periods=time_steps, freq="D")
    lats = np.linspace(-90, 90, lat_points)
    lons = np.linspace(-180, 180, lon_points)
    
    # Create realistic climate data
    np.random.seed(42)
    
    # Temperature (degC) - varies with latitude and season
    lat_factor = np.cos(np.radians(lats))  # Warmer at equator
    temp_base = 15 + 20 * lat_factor[:, np.newaxis]  # Base temperature by latitude
    seasonal = 10 * np.sin(2 * np.pi * np.arange(time_steps) / 365)  # Seasonal variation
    temperature = temp_base[np.newaxis, :, :] + seasonal[:, np.newaxis, np.newaxis] + \
                  5 * np.random.random((time_steps, lat_points, lon_points))  # Noise
    
    # Pressure (hPa) - varies with temperature and altitude
    pressure = 1013 + 0.5 * temperature + 20 * np.random.random((time_steps, lat_points, lon_points))
    
    # Wind speed (m/s) - varies spatially
    wind_speed = 5 + 3 * np.random.random((time_steps, lat_points, lon_points))
    
    # Create dataset
    ds = xr.Dataset(
        {
            "temperature": (("time", "lat", "lon"), temperature),
            "pressure": (("time", "lat", "lon"), pressure),
            "wind_speed": (("time", "lat", "lon"), wind_speed),
        },
        coords={
            "time": times,
            "lat": lats,
            "lon": lons,
        },
    )
    
    # Add attributes
    ds.attrs["title"] = "Sample climate data for access pattern testing"
    ds.attrs["institution"] = "Oceanum"
    ds["temperature"].attrs["units"] = "degC"
    ds["pressure"].attrs["units"] = "hPa"
    ds["wind_speed"].attrs["units"] = "m/s"
    
    print(f"Created dataset with shape: {ds.temperature.shape}")
    return ds


def benchmark_access_pattern(zarr_path, pattern_name, access_func):
    """
    Benchmark a specific access pattern.
    
    Args:
        zarr_path: Path to the Zarr store
        pattern_name: Name of the access pattern
        access_func: Function to perform the access pattern test
    
    Returns:
        Time taken for the access pattern in seconds
    """
    print(f"Benchmarking {pattern_name} access pattern...")
    
    # Open the Zarr store
    ds = xr.open_zarr(zarr_path)
    
    # Run the access pattern test
    start_time = time.perf_counter()
    result = access_func(ds)
    end_time = time.perf_counter()
    
    # Close the dataset
    ds.close()
    
    duration = end_time - start_time
    print(f"  Result: {result}")
    print(f"  Duration: {duration:.4f} seconds")
    return duration


def temporal_access_test(ds):
    """Test temporal access pattern - time series for a specific location."""
    # Get time series for a fixed location (middle of the domain)
    lat_idx = len(ds.lat) // 2
    lon_idx = len(ds.lon) // 2
    
    # Extract time series for all variables
    temp_series = ds.temperature[:, lat_idx, lon_idx].values
    pres_series = ds.pressure[:, lat_idx, lon_idx].values
    wind_series = ds.wind_speed[:, lat_idx, lon_idx].values
    
    # Return some statistics
    return {
        "temp_mean": float(np.mean(temp_series)),
        "pres_mean": float(np.mean(pres_series)),
        "wind_mean": float(np.mean(wind_series)),
        "temp_std": float(np.std(temp_series)),
    }


def spatial_access_test(ds):
    """Test spatial access pattern - spatial slice for a specific time."""
    # Get spatial slice for a fixed time (middle of the time series)
    time_idx = len(ds.time) // 2
    
    # Extract spatial slices for all variables
    temp_slice = ds.temperature[time_idx, :, :].values
    pres_slice = ds.pressure[time_idx, :, :].values
    wind_slice = ds.wind_speed[time_idx, :, :].values
    
    # Return some statistics
    return {
        "temp_mean": float(np.mean(temp_slice)),
        "pres_mean": float(np.mean(pres_slice)),
        "wind_mean": float(np.mean(wind_slice)),
        "temp_max": float(np.max(temp_slice)),
    }


def balanced_access_test(ds):
    """Test balanced access pattern - smaller spatiotemporal subset."""
    # Get a smaller spatiotemporal subset
    time_subset = slice(len(ds.time)//4, 3*len(ds.time)//4)
    lat_subset = slice(len(ds.lat)//4, 3*len(ds.lat)//4)
    lon_subset = slice(len(ds.lon)//4, 3*len(ds.lon)//4)
    
    # Extract subset for all variables
    temp_subset = ds.temperature[time_subset, lat_subset, lon_subset].values
    pres_subset = ds.pressure[time_subset, lat_subset, lon_subset].values
    
    # Return some statistics
    return {
        "temp_mean": float(np.mean(temp_subset)),
        "pres_mean": float(np.mean(pres_subset)),
        "temp_shape": temp_subset.shape,
    }


def create_zarr_with_chunking_strategy(ds, input_path, output_path, access_pattern):
    """
    Create a Zarr archive with chunking optimized for a specific access pattern.
    
    Args:
        ds: Input xarray Dataset
        input_path: Path to the input NetCDF file
        output_path: Output path for Zarr archive
        access_pattern: Access pattern to optimize for ("temporal", "spatial", "balanced")
    """
    print(f"Creating Zarr archive optimized for {access_pattern} access pattern...")
    
    # Define chunking strategies based on access pattern
    if access_pattern == "temporal":
        # Optimize for time series access: large time chunks, smaller spatial chunks
        chunking = ChunkingConfig(time=100, lat=30, lon=60)
    elif access_pattern == "spatial":
        # Optimize for spatial access: smaller time chunks, larger spatial chunks
        chunking = ChunkingConfig(time=10, lat=90, lon=180)
    else:  # balanced
        # Balanced approach: moderate chunks in all dimensions
        chunking = ChunkingConfig(time=30, lat=60, lon=90)
    
    # Create converter with the specified chunking
    config = ZarrConverterConfig(chunking=chunking)
    converter = ZarrConverter(config=config)
    
    # Convert to Zarr using the file path
    converter.convert(input_path, output_path)
    print(f"Created Zarr archive at: {output_path}")


def print_results_table(results):
    """Print results in a nicely formatted table."""
    print("\n" + "="*80)
    print("ACCESS PATTERN PERFORMANCE COMPARISON".center(80))
    print("="*80)
    
    # Header
    print(f"{'Access Pattern':<20} {'Chunking Strategy':<30} {'Time (s)':<15}")
    print("-"*80)
    
    # Results
    for pattern, data in results.items():
        chunking_str = f"time:{data['chunking'].time}, lat:{data['chunking'].lat}, lon:{data['chunking'].lon}"
        print(f"{pattern.capitalize():<20} {chunking_str:<30} {data['time']:<15.4f}")
    
    print("-"*80)
    
    # Summary
    fastest = min(results.items(), key=lambda x: x[1]['time'])
    slowest = max(results.items(), key=lambda x: x[1]['time'])
    print(f"\nFastest pattern: {fastest[0].capitalize()} ({fastest[1]['time']:.4f}s)")
    print(f"Slowest pattern: {slowest[0].capitalize()} ({slowest[1]['time']:.4f}s)")
    print(f"Performance difference: {slowest[1]['time']/fastest[1]['time']:.2f}x")


def main():
    """Main function to run the access pattern speed test."""
    print("Zarrify Access Pattern Speed Test")
    print("="*50)
    
    with tempfile.TemporaryDirectory() as tmpdir:
        # Create sample data
        ds = create_sample_data(time_steps=365, lat_points=181, lon_points=361)
        
        # Save as NetCDF for input
        nc_file = os.path.join(tmpdir, "sample.nc")
        ds.to_netcdf(nc_file)
        print(f"Saved sample data to {nc_file}")
        
        # Dictionary to store results
        results = {}
        
        # Test different access patterns
        access_patterns = ["temporal", "spatial", "balanced"]
        access_tests = {
            "temporal": temporal_access_test,
            "spatial": spatial_access_test,
            "balanced": balanced_access_test
        }
        
        for pattern in access_patterns:
            print(f"\n{'='*60}")
            print(f"Testing {pattern.upper()} Access Pattern")
            print(f"{'='*60}")
            
            # Create Zarr archive with chunking optimized for this pattern
            zarr_path = os.path.join(tmpdir, f"sample_{pattern}.zarr")
            create_zarr_with_chunking_strategy(ds, nc_file, zarr_path, pattern)
            
            # Benchmark the access pattern
            duration = benchmark_access_pattern(zarr_path, pattern, access_tests[pattern])
            
            # Store results
            results[pattern] = {
                "time": duration,
                "chunking": ds.zarrify_config.chunking if hasattr(ds, 'zarrify_config') else "default"
            }
            
            # For demonstration purposes, we'll extract the actual chunking from the created zarr
            # In a real implementation, we'd store this information when creating the zarr
            created_ds = xr.open_zarr(zarr_path)
            if hasattr(created_ds.temperature.data, 'chunks'):
                chunks = created_ds.temperature.data.chunks
                dims = created_ds.temperature.dims
                chunking_info = dict(zip(dims, [chunk[0] if chunk else 1 for chunk in chunks]))
                results[pattern]["chunking"] = type('ChunkingInfo', (), chunking_info)()
            else:
                # Fallback to the expected chunking
                if pattern == "temporal":
                    results[pattern]["chunking"] = type('ChunkingInfo', (), {"time": 100, "lat": 30, "lon": 60})()
                elif pattern == "spatial":
                    results[pattern]["chunking"] = type('ChunkingInfo', (), {"time": 10, "lat": 90, "lon": 180})()
                else:  # balanced
                    results[pattern]["chunking"] = type('ChunkingInfo', (), {"time": 30, "lat": 60, "lon": 90})()
            created_ds.close()
        
        # Print results table
        print_results_table(results)
        
        print("\nTest completed successfully!")


if __name__ == "__main__":
    main()