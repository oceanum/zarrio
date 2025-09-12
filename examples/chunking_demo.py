"""
Example demonstrating intelligent chunking analysis in zarrify.
"""

import tempfile
import numpy as np
import pandas as pd
import xarray as xr
import os

from zarrify import ZarrConverter, convert_to_zarr, append_to_zarr
from zarrify.models import ZarrConverterConfig, ChunkingConfig, PackingConfig


def create_sample_climate_data(tmpdir: str) -> str:
    """Create sample climate data for demonstration."""
    # Create smaller sample climate data for faster demo
    times = pd.date_range("2020-01-01", periods=100, freq="D")  # 100 days instead of 3650
    lats = np.linspace(-90, 90, 30)  # 30 lat points instead of 180
    lons = np.linspace(-180, 180, 60)  # 60 lon points instead of 360
    
    # Create realistic climate data
    np.random.seed(42)  # For reproducible results
    temperature = 20 + 15 * np.sin(2 * np.pi * np.arange(100) / 365)  # Seasonal cycle
    temperature = temperature[:, np.newaxis, np.newaxis] + 10 * np.random.random([100, 30, 60])
    
    pressure = 1013 + 50 * np.random.random([100, 30, 60])
    
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
    
    # Add metadata
    ds.attrs["title"] = "Sample Climate Dataset"
    ds.attrs["institution"] = "zarrify Demonstration"
    ds["temperature"].attrs["units"] = "degC"
    ds["temperature"].attrs["long_name"] = "Air Temperature"
    ds["pressure"].attrs["units"] = "hPa"
    ds["pressure"].attrs["long_name"] = "Surface Pressure"
    
    # Save as NetCDF
    nc_file = os.path.join(tmpdir, "climate_sample.nc")
    ds.to_netcdf(nc_file)
    
    print(f"Created sample climate data: {nc_file}")
    print(f"  Dimensions: time={len(times)}, lat={len(lats)}, lon={len(lons)}")
    print(f"  Data size: {len(times) * len(lats) * len(lons) * 4 / (1024**2):.1f} MB (float32)")
    
    return nc_file


def demonstrate_chunking_analysis():
    """Demonstrate intelligent chunking analysis."""
    print("=== zarrify Intelligent Chunking Analysis Demo ===\n")
    
    with tempfile.TemporaryDirectory() as tmpdir:
        # Create sample climate data
        nc_file = create_sample_climate_data(tmpdir)
        
        # 1. Demonstrate automatic chunking analysis
        print("1. Automatic Chunking Analysis:")
        print("   When no chunking is specified, zarrify analyzes the data")
        print("   and provides recommendations based on access patterns.")
        
        # Convert without specifying chunking (will trigger analysis)
        zarr_file1 = os.path.join(tmpdir, "auto_chunking.zarr")
        
        # This will automatically analyze chunking and provide recommendations
        # Note: The access_pattern parameter is not directly supported in convert_to_zarr
        # Instead, we can use the ChunkAnalyzer to get recommendations if needed
        
        # For now, let's just do a simple conversion
        convert_to_zarr(
            input_path=nc_file,
            output_path=zarr_file1
        )
        
        print("   ✓ Conversion completed with automatic chunking analysis")
        
        # 2. Demonstrate chunking with explicit configuration
        print("\n2. Explicit Chunking Configuration:")
        print("   Users can specify chunking strategies for different access patterns.")
        
        # Create configs for different access patterns
        temporal_config = ZarrConverterConfig(
            chunking=ChunkingConfig(time=50, lat=10, lon=20),  # Temporal focus
            packing=PackingConfig(enabled=True, bits=16),
            attrs={"access_pattern": "temporal"}
        )
        
        spatial_config = ZarrConverterConfig(
            chunking=ChunkingConfig(time=10, lat=20, lon=30),  # Spatial focus
            packing=PackingConfig(enabled=True, bits=16),
            attrs={"access_pattern": "spatial"}
        )
        
        balanced_config = ZarrConverterConfig(
            chunking=ChunkingConfig(time=20, lat=15, lon=25),  # Balanced
            packing=PackingConfig(enabled=True, bits=16),
            attrs={"access_pattern": "balanced"}
        )
        
        # Convert with different configurations
        zarr_file2 = os.path.join(tmpdir, "temporal_focus.zarr")
        converter_temporal = ZarrConverter(config=temporal_config)
        converter_temporal.convert(nc_file, zarr_file2)
        print("   ✓ Temporal-focused chunking applied")
        
        zarr_file3 = os.path.join(tmpdir, "spatial_focus.zarr")
        converter_spatial = ZarrConverter(config=spatial_config)
        converter_spatial.convert(nc_file, zarr_file3)
        print("   ✓ Spatial-focused chunking applied")
        
        zarr_file4 = os.path.join(tmpdir, "balanced.zarr")
        converter_balanced = ZarrConverter(config=balanced_config)
        converter_balanced.convert(nc_file, zarr_file4)
        print("   ✓ Balanced chunking applied")
        
        # 3. Demonstrate chunking validation
        print("\n3. Chunking Validation:")
        print("   zarrify validates user-provided chunking and warns about issues.")
        
        # Create config with problematic chunking (will generate warnings)
        bad_chunking_config = ZarrConverterConfig(
            chunking=ChunkingConfig(time=80, lat=1, lon=1),  # Too extreme
            attrs={"chunking": "problematic"}
        )
        
        zarr_file5 = os.path.join(tmpdir, "validated_chunking.zarr")
        converter_bad = ZarrConverter(config=bad_chunking_config)
        converter_bad.convert(nc_file, zarr_file5)
        print("   ✓ Chunking validation completed (warnings logged if issues found)")
        
        # 4. Show chunking recommendations
        print("\n4. Chunking Recommendations:")
        print("   For the sample dataset (100 days of 30x60 global daily data):")
        print("   ")
        print("   Temporal Analysis Optimized:")
        print("     - time: 50 chunks (2 days per chunk)")
        print("     - lat: 10 chunks (3 degrees per chunk)")  
        print("     - lon: 20 chunks (3 degrees per chunk)")
        print("     - Purpose: Efficient time series extraction")
        print("   ")
        print("   Spatial Analysis Optimized:")
        print("     - time: 10 chunks (10 days per chunk)")
        print("     - lat: 20 chunks (1.5 degrees per chunk)")
        print("     - lon: 30 chunks (6 degrees per chunk)")
        print("     - Purpose: Efficient spatial subsetting")
        print("   ")
        print("   Balanced Approach:")
        print("     - time: 20 chunks (5 days per chunk)")
        print("     - lat: 15 chunks (2 degrees per chunk)")
        print("     - lon: 25 chunks (2.4 degrees per chunk)")
        print("     - Purpose: Good performance for mixed workloads")
        
        print("\n=== Demo completed successfully! ===")


def explain_chunking_best_practices():
    """Explain chunking best practices for different scenarios."""
    print("\n=== Chunking Best Practices ===\n")
    
    print("1. General Guidelines:")
    print("   • Target chunk sizes: 10-100 MB for optimal performance")
    print("   • Match chunks to your typical access patterns")
    print("   • Consider compression - larger chunks often compress better")
    print("   • Balance chunk count vs chunk size for parallel processing")
    
    print("\n2. Time Series Analysis (e.g., extracting data for specific locations):")
    print("   • Large time chunks, smaller spatial chunks")
    print("   • Example: time=50, lat=10, lon=20 for daily global data")
    print("   • Benefit: Fewer I/O operations when accessing long time series")
    
    print("\n3. Spatial Analysis (e.g., analyzing maps at specific times):")
    print("   • Smaller time chunks, larger spatial chunks")
    print("   • Example: time=10, lat=20, lon=30 for daily global data")
    print("   • Benefit: Efficient spatial subsetting and processing")
    
    print("\n4. Mixed Workloads:")
    print("   • Balanced chunking across all dimensions")
    print("   • Example: time=20, lat=15, lon=25 for daily global data")
    print("   • Benefit: Reasonable performance for diverse access patterns")
    
    print("\n5. Resolution-Specific Recommendations:")
    print("   • Low resolution (1° or coarser): Larger chunks acceptable")
    print("   • Medium resolution (0.25° to 1°): Moderate chunking strategy")
    print("   • High resolution (0.1° or finer): Smaller chunks to limit size")
    
    print("\n6. Warning Signs:")
    print("   • Chunks < 1 MB: May cause metadata overhead issues")
    print("   • Chunks > 100 MB: May cause memory problems")
    print("   • Very small chunks in large dimensions: Inefficient I/O")
    print("   • Very large chunks in frequently accessed dimensions: Poor caching")


if __name__ == "__main__":
    demonstrate_chunking_analysis()
    explain_chunking_best_practices()