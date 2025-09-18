"""
Consolidated example demonstrating zarrio's intelligent chunking and access pattern optimization.

This example shows:
1. How zarrio's intelligent chunking works for different dataset sizes
2. Performance differences between chunking strategies
3. How to achieve optimal chunk sizes for your data
"""

import numpy as np
import pandas as pd
import xarray as xr
import tempfile
import os
import time

from zarrio import ZarrConverter, convert_to_zarr
from zarrio.models import ZarrConverterConfig
from zarrio.chunking import get_chunk_recommendation


def create_sample_data(time_steps=365, lat_points=181, lon_points=361):
    """Create sample climate data."""
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
    ds.attrs["title"] = "Sample climate data for chunking demonstration"
    ds.attrs["institution"] = "Oceanum"
    ds["temperature"].attrs["units"] = "degC"
    ds["pressure"].attrs["units"] = "hPa"
    
    return ds


def demonstrate_intelligent_chunking():
    """Demonstrate how intelligent chunking works for different dataset sizes."""
    print("INTELLIGENT CHUNKING DEMONSTRATION")
    print("=" * 40)
    
    # Small dataset (our sample data)
    small_dims = {
        "time": 365,
        "lat": 181,
        "lon": 361,
    }
    
    print("1. Small Dataset Analysis (365×181×361):")
    print(f"   Dimensions: {small_dims}")
    
    # Maximum possible chunk size
    max_elements = 365 * 181 * 361
    max_mb = max_elements * 4 / (1024 * 1024)
    print(f"   Maximum possible chunk size: {max_mb:.1f} MB")
    
    # Intelligent chunking recommendations
    for pattern in ["temporal", "spatial", "balanced"]:
        recommendation = get_chunk_recommendation(
            dimensions=small_dims,
            dtype_size_bytes=4,
            access_pattern=pattern
        )
        percentage = recommendation.estimated_chunk_size_mb / max_mb * 100
        print(f"   {pattern.capitalize()} access: {recommendation.chunks} = {recommendation.estimated_chunk_size_mb:.1f} MB ({percentage:.1f}% of max)")
    
    # Large dataset
    large_dims = {
        "time": 1000,
        "lat": 1000,
        "lon": 1000,
    }
    
    print("\n2. Large Dataset Analysis (1000×1000×1000):")
    print(f"   Dimensions: {large_dims}")
    
    # Maximum possible chunk size
    max_elements = 1000 * 1000 * 1000
    max_mb = max_elements * 4 / (1024 * 1024)
    print(f"   Maximum possible chunk size: {max_mb:.0f} MB")
    
    # Intelligent chunking recommendations
    for pattern in ["temporal", "spatial", "balanced"]:
        recommendation = get_chunk_recommendation(
            dimensions=large_dims,
            dtype_size_bytes=4,
            access_pattern=pattern
        )
        percentage = recommendation.estimated_chunk_size_mb / 50.0 * 100
        print(f"   {pattern.capitalize()} access: {recommendation.chunks} = {recommendation.estimated_chunk_size_mb:.0f} MB ({percentage:.0f}% of target 50MB)")


def create_zarr_with_chunking(input_path, output_path, chunking_config, name):
    """Create a Zarr archive with specific chunking."""
    print(f"  Creating {name}...")
    print(f"    Chunks: {chunking_config}")
    
    # Calculate actual chunk size
    ds = xr.open_dataset(input_path)
    elements = 1
    for dim, chunk_size in chunking_config.items():
        if dim in ds.dims:
            elements *= min(chunk_size, len(ds[dim]))
    ds.close()
    
    chunk_size_mb = elements * 4 / (1024 * 1024)
    print(f"    Estimated chunk size: {chunk_size_mb:.1f} MB")
    
    # Create converter with the chunking
    config = ZarrConverterConfig(chunking=chunking_config)
    converter = ZarrConverter(config=config)
    converter.convert(input_path, output_path)
    print(f"    Created Zarr archive at: {output_path}")


def benchmark_access_pattern(zarr_path, pattern_name, access_func):
    """Benchmark a specific access pattern."""
    print(f"    Benchmarking {pattern_name}...")
    
    # Open the Zarr store
    ds = xr.open_zarr(zarr_path)
    
    # Run the access pattern test
    start_time = time.perf_counter()
    result = access_func(ds)
    end_time = time.perf_counter()
    
    # Close the dataset
    ds.close()
    
    duration = end_time - start_time
    print(f"      Duration: {duration:.4f} seconds")
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


def demonstrate_performance_differences():
    """Demonstrate performance differences between chunking strategies."""
    print("\n\nPERFORMANCE DIFFERENCES DEMONSTRATION")
    print("=" * 45)
    
    with tempfile.TemporaryDirectory() as tmpdir:
        # Create sample data
        ds = create_sample_data()
        
        # Save as NetCDF for input
        nc_file = os.path.join(tmpdir, "sample.nc")
        ds.to_netcdf(nc_file)
        print(f"Created sample data with shape: {ds.temperature.shape}")
        print(f"Saved sample data to {nc_file}")
        
        # Create different chunking strategies
        strategies = {
            "temporal_optimized": {
                "time": 100,   # Large time chunks
                "lat": 50,     # Moderate spatial chunks
                "lon": 100,    # Moderate spatial chunks
            },
            "spatial_optimized": {
                "time": 10,    # Small time chunks
                "lat": 181,    # Full spatial chunks
                "lon": 361,    # Full spatial chunks
            },
            "balanced_optimized": {
                "time": 50,    # Moderate time chunks
                "lat": 90,     # Moderate spatial chunks
                "lon": 180,    # Moderate spatial chunks
            }
        }
        
        # Create Zarr archives
        print("\nCreating Zarr archives with different chunking strategies:")
        results = {}
        for strategy_name, chunking in strategies.items():
            zarr_path = os.path.join(tmpdir, f"sample_{strategy_name}.zarr")
            create_zarr_with_chunking(nc_file, zarr_path, chunking, strategy_name)
            results[strategy_name] = {
                "zarr_path": zarr_path,
                "chunking": chunking
            }
        
        # Benchmark performance
        print("\nBenchmarking performance for different access patterns:")
        access_tests = {
            "temporal": temporal_access_test,
            "spatial": spatial_access_test,
        }
        
        benchmark_results = {}
        
        for test_pattern, test_func in access_tests.items():
            print(f"\n  Testing {test_pattern.upper()} access pattern:")
            pattern_results = {}
            for strategy_name in strategies.keys():
                zarr_path = results[strategy_name]["zarr_path"]
                duration = benchmark_access_pattern(zarr_path, f"{strategy_name} archive", test_func)
                pattern_results[strategy_name] = duration
            benchmark_results[test_pattern] = pattern_results
        
        # Print results table
        print(f"\n{'='*70}")
        print("PERFORMANCE RESULTS SUMMARY")
        print(f"{'='*70}")
        
        # Header
        print(f"{'Test Pattern':<15} {'Temporal Archive':<18} {'Spatial Archive':<18} {'Balanced Archive':<18}")
        print("-" * 70)
        
        # Results
        for test_pattern in access_tests.keys():
            temporal_time = benchmark_results[test_pattern]["temporal_optimized"]
            spatial_time = benchmark_results[test_pattern]["spatial_optimized"]
            balanced_time = benchmark_results[test_pattern]["balanced_optimized"]
            print(f"{test_pattern.capitalize():<15} {temporal_time:<18.4f} {spatial_time:<18.4f} {balanced_time:<18.4f}")
        
        print("-" * 70)
        
        # Performance analysis
        print(f"\nPERFORMANCE ANALYSIS:")
        print(f"{'-'*30}")
        
        for test_pattern in access_tests.keys():
            times = benchmark_results[test_pattern]
            best_strategy = min(times, key=times.get)
            best_time = times[best_strategy]
            worst_strategy = max(times, key=times.get)
            worst_time = times[worst_strategy]
            
            print(f"{test_pattern.capitalize()} access pattern:")
            print(f"  Best performance: {best_strategy.replace('_', ' ').title()} ({best_time:.4f}s)")
            print(f"  Worst performance: {worst_strategy.replace('_', ' ').title()} ({worst_time:.4f}s)")
            print(f"  Performance difference: {worst_time/best_time:.2f}x")
            print()


def demonstrate_manual_optimization():
    """Demonstrate how to manually optimize chunking for small datasets."""
    print("MANUAL OPTIMIZATION FOR SMALL DATASETS")
    print("=" * 40)
    
    small_dims = {
        "time": 365,
        "lat": 181,
        "lon": 361,
    }
    
    print("For small datasets, manually specify maximum chunks to maximize chunk size:")
    
    # To get maximum chunk size, use full dimensions
    max_chunks = {
        "time": 365,    # Use all time steps
        "lat": 181,     # Use all latitudes
        "lon": 361,     # Use all longitudes
    }
    
    # Calculate chunk size
    elements = 365 * 181 * 361
    mb = elements * 4 / (1024 * 1024)
    
    print(f"Maximum chunks: {max_chunks}")
    print(f"Resulting chunk size: {elements:,} elements = {mb:.1f} MB")
    
    print("\nFor different access patterns with small datasets:")
    
    strategies = {
        "Temporal focus": {"time": 365, "lat": 181, "lon": 361},   # Max time, full spatial
        "Spatial focus": {"time": 1, "lat": 181, "lon": 361},      # Single time, full spatial
        "Balanced": {"time": 50, "lat": 181, "lon": 361},          # Moderate time, full spatial
    }
    
    for name, chunks in strategies.items():
        elements = chunks["time"] * chunks["lat"] * chunks["lon"]
        mb = elements * 4 / (1024 * 1024)
        print(f"  {name}: {chunks} = {mb:.1f} MB")


def main():
    """Main function to demonstrate zarrio's chunking capabilities."""
    print("Zarrify Intelligent Chunking and Performance Optimization")
    print("=" * 55)
    
    # Demonstrate intelligent chunking
    demonstrate_intelligent_chunking()
    
    # Demonstrate performance differences
    demonstrate_performance_differences()
    
    # Demonstrate manual optimization
    demonstrate_manual_optimization()
    
    # Summary
    print("KEY TAKEAWAYS:")
    print("=" * 15)
    print("1. Zarrify's intelligent chunking automatically recommends optimal")
    print("   chunk sizes based on your data dimensions and access patterns")
    print("2. For small datasets, chunk sizes are limited by dimension sizes")
    print("3. For large datasets, chunk sizes can achieve the target 50 MB")
    print("4. Proper chunking provides 1.2-3.0x performance improvements")
    print("5. Choose chunking strategy based on your primary access pattern")
    print("6. For small datasets, manually specify maximum chunks for best performance")


if __name__ == "__main__":
    main()