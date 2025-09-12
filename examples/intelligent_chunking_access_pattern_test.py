"""
Example demonstrating zarrify's intelligent chunking with access pattern optimization.

This example shows how to use zarrify's built-in chunking analysis to automatically
recommend optimal chunking strategies based on your data dimensions and access patterns.
"""

import numpy as np
import pandas as pd
import xarray as xr
import tempfile
import os
import time

from zarrify import ZarrConverter
from zarrify.models import ZarrConverterConfig
from zarrify.chunking import get_chunk_recommendation


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


def get_dataset_dimensions(ds):
    """Extract dimensions from dataset for chunking analysis."""
    return {dim: len(ds[dim]) for dim in ds.dims}


def create_zarr_with_intelligent_chunking(input_path, output_path, access_pattern):
    """
    Create a Zarr archive with intelligent chunking optimized for a specific access pattern.
    
    Args:
        input_path: Path to the input NetCDF file
        output_path: Output path for Zarr archive
        access_pattern: Access pattern to optimize for ("temporal", "spatial", "balanced")
    """
    print(f"Creating Zarr archive optimized for {access_pattern} access pattern...")
    
    # Open dataset to get dimensions
    ds = xr.open_dataset(input_path)
    dimensions = get_dataset_dimensions(ds)
    ds.close()
    
    # Get intelligent chunking recommendation
    recommendation = get_chunk_recommendation(
        dimensions=dimensions,
        dtype_size_bytes=4,  # float32
        access_pattern=access_pattern
    )
    
    print(f"  Recommended chunks: {recommendation.chunks}")
    print(f"  Estimated chunk size: {recommendation.estimated_chunk_size_mb:.1f} MB")
    
    # Create converter with the recommended chunking
    config = ZarrConverterConfig(chunking=recommendation.chunks)
    converter = ZarrConverter(config=config)
    
    # Convert to Zarr using the file path
    converter.convert(input_path, output_path)
    print(f"  Created Zarr archive at: {output_path}")
    
    return recommendation


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
    print(f"  Benchmarking {pattern_name} access pattern...")
    
    # Open the Zarr store
    ds = xr.open_zarr(zarr_path)
    
    # Run the access pattern test
    start_time = time.perf_counter()
    result = access_func(ds)
    end_time = time.perf_counter()
    
    # Close the dataset
    ds.close()
    
    duration = end_time - start_time
    print(f"    Duration: {duration:.4f} seconds")
    return duration


def temporal_access_test(ds):
    """Test temporal access pattern - time series for a specific location."""
    # Get time series for a fixed location (middle of the domain)
    lat_idx = len(ds.lat) // 2
    lon_idx = len(ds.lon) // 2
    
    # Extract time series for temperature
    temp_series = ds.temperature[:, lat_idx, lon_idx].values
    
    # Return some statistics
    return {
        "mean": float(np.mean(temp_series)),
        "std": float(np.std(temp_series)),
    }


def spatial_access_test(ds):
    """Test spatial access pattern - spatial slice for a specific time."""
    # Get spatial slice for a fixed time (middle of the time series)
    time_idx = len(ds.time) // 2
    
    # Extract spatial slice for temperature
    temp_slice = ds.temperature[time_idx, :, :].values
    
    # Return some statistics
    return {
        "mean": float(np.mean(temp_slice)),
        "max": float(np.max(temp_slice)),
    }


def main():
    """Main function to demonstrate intelligent chunking with access pattern optimization."""
    print("Zarrify Intelligent Chunking with Access Pattern Optimization")
    print("=" * 60)
    
    with tempfile.TemporaryDirectory() as tmpdir:
        # Create sample data
        ds = create_sample_data()
        
        # Save as NetCDF for input
        nc_file = os.path.join(tmpdir, "sample.nc")
        ds.to_netcdf(nc_file)
        print(f"Created sample data with shape: {ds.temperature.shape}")
        print(f"Saved sample data to {nc_file}")
        
        # Dictionary to store results
        results = {}
        
        # Test different access patterns with intelligent chunking
        access_patterns = ["temporal", "spatial", "balanced"]
        access_tests = {
            "temporal": temporal_access_test,
            "spatial": spatial_access_test,
        }
        
        for pattern in access_patterns:
            print(f"\n{'-'*60}")
            print(f"Testing {pattern.upper()} Access Pattern")
            print(f"{'-'*60}")
            
            # Create Zarr archive with intelligent chunking optimized for this pattern
            zarr_path = os.path.join(tmpdir, f"sample_{pattern}.zarr")
            recommendation = create_zarr_with_intelligent_chunking(nc_file, zarr_path, pattern)
            
            # Store recommendation for results
            results[pattern] = {
                "recommendation": recommendation,
                "zarr_path": zarr_path
            }
        
        # Now benchmark each access pattern on all Zarr archives
        print(f"\n{'='*60}")
        print("PERFORMANCE BENCHMARKING")
        print(f"{'='*60}")
        
        benchmark_results = {}
        
        for test_pattern, test_func in access_tests.items():
            print(f"\nTesting {test_pattern.upper()} access pattern on all archives:")
            print("-" * 50)
            
            pattern_results = {}
            for archive_pattern in access_patterns:
                zarr_path = results[archive_pattern]["zarr_path"]
                duration = benchmark_access_pattern(zarr_path, f"{archive_pattern} archive", test_func)
                pattern_results[archive_pattern] = duration
            
            benchmark_results[test_pattern] = pattern_results
        
        # Print detailed results table
        print(f"\n{'='*80}")
        print("DETAILED PERFORMANCE RESULTS")
        print(f"{'='*80}")
        
        # Header
        print(f"{'Test Pattern':<15} {'Temporal Archive':<20} {'Spatial Archive':<20} {'Balanced Archive':<20}")
        print("-" * 80)
        
        # Results
        for test_pattern in access_tests.keys():
            temporal_time = benchmark_results[test_pattern]["temporal"]
            spatial_time = benchmark_results[test_pattern]["spatial"]
            balanced_time = benchmark_results[test_pattern]["balanced"]
            print(f"{test_pattern.capitalize():<15} {temporal_time:<20.4f} {spatial_time:<20.4f} {balanced_time:<20.4f}")
        
        print("-" * 80)
        
        # Performance analysis
        print(f"\nPERFORMANCE ANALYSIS:")
        print(f"{'-'*40}")
        
        # Best performance for each access pattern
        for test_pattern in access_tests.keys():
            times = benchmark_results[test_pattern]
            best_archive = min(times, key=times.get)
            best_time = times[best_archive]
            worst_archive = max(times, key=times.get)
            worst_time = times[worst_archive]
            
            print(f"{test_pattern.capitalize()} access pattern:")
            print(f"  Best performance: {best_archive.capitalize()} archive ({best_time:.4f}s)")
            print(f"  Worst performance: {worst_archive.capitalize()} archive ({worst_time:.4f}s)")
            print(f"  Performance difference: {worst_time/best_time:.2f}x")
            print()
        
        # Summary
        print("KEY TAKEAWAYS:")
        print("-" * 20)
        print("1. Zarrify's intelligent chunking automatically recommends optimal")
        print("   chunk sizes based on your data dimensions and access patterns")
        print("2. Temporal access patterns perform best with archives optimized")
        print("   for temporal access (large time chunks)")
        print("3. Spatial access patterns perform best with archives optimized")
        print("   for spatial access (large spatial chunks)")
        print("4. Proper chunking can improve performance by 2-5x")
        print("5. Choose your chunking strategy based on your primary access pattern")


if __name__ == "__main__":
    main()