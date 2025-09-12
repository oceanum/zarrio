"""
Example demonstrating all key features of zarrify.
"""

import tempfile
import numpy as np
import pandas as pd
import xarray as xr
import os
import yaml

from zarrify import ZarrConverter, convert_to_zarr, append_to_zarr
from zarrify.models import (
    ZarrConverterConfig, 
    ChunkingConfig, 
    PackingConfig, 
    CompressionConfig,
    TimeConfig,
    VariableConfig
)


def create_sample_data_files(tmpdir: str, num_files: int = 4) -> list:
    """Create sample NetCDF files for demonstration."""
    files = []
    
    for i in range(num_files):
        # Create data for different time periods (non-overlapping)
        start_date = f"2020-01-{i*3+1:02d}"  # 2020-01-01, 2020-01-04, 2020-01-07, 2020-01-10
        periods = 3  # 3 days per file
        
        times = pd.date_range(start_date, periods=periods, freq="D")
        lats = np.linspace(-45, 45, 10)  # 10 lat points
        lons = np.linspace(-90, 90, 15)  # 15 lon points
        
        # Create random data
        np.random.seed(42 + i)  # Different seed for each file
        temperature = 20 + 10 * np.random.random((periods, 10, 15))
        pressure = 1013 + 50 * np.random.random((periods, 10, 15))
        humidity = 50 + 30 * np.random.random((periods, 10, 15))
        
        # Create dataset
        ds = xr.Dataset(
            {
                "temperature": (("time", "lat", "lon"), temperature),
                "pressure": (("time", "lat", "lon"), pressure),
                "humidity": (("time", "lat", "lon"), humidity),
            },
            coords={
                "time": times,
                "lat": lats,
                "lon": lons,
            },
        )
        
        # Add attributes
        ds.attrs["title"] = "Sample meteorological data"
        ds.attrs["institution"] = "zarrify Demo"
        ds["temperature"].attrs["units"] = "degC"
        ds["pressure"].attrs["units"] = "hPa"
        ds["humidity"].attrs["units"] = "%"
        
        # Save to NetCDF
        nc_file = os.path.join(tmpdir, f"sample_{i+1:02d}.nc")
        ds.to_netcdf(nc_file)
        files.append(nc_file)
        print(f"Created {nc_file}")
    
    return files


def demonstrate_all_features():
    """Demonstrate all key features of zarrify."""
    print("=== zarrify Feature Demonstration ===\n")
    
    with tempfile.TemporaryDirectory() as tmpdir:
        # Step 1: Create sample data files
        print("1. Creating sample NetCDF files...")
        data_files = create_sample_data_files(tmpdir, num_files=4)
        print()
        
        # Step 2: Demonstrate simple conversion
        print("2. Simple conversion with default settings...")
        simple_zarr = os.path.join(tmpdir, "simple_conversion.zarr")
        convert_to_zarr(data_files[0], simple_zarr)
        print(f"   ✓ Converted {os.path.basename(data_files[0])} to {os.path.basename(simple_zarr)}")
        print()
        
        # Step 3: Demonstrate advanced configuration
        print("3. Advanced conversion with Pydantic configuration...")
        
        # Create config programmatically
        config = ZarrConverterConfig(
            chunking=ChunkingConfig(time=30, lat=25, lon=50),
            compression=CompressionConfig(method="blosc:zstd:3"),
            packing=PackingConfig(enabled=True, bits=16),
            time=TimeConfig(dim="time", append_dim="time"),
            variables=VariableConfig(include=["temperature", "pressure"]),  # Exclude humidity
            attrs={"title": "Advanced conversion demo", "source": "zarrify"},
            retries_on_missing=2,  # Enable retries
            missing_check_vars="all"
        )
        
        converter = ZarrConverter(config=config)
        advanced_zarr = os.path.join(tmpdir, "advanced_conversion.zarr")
        converter.convert(data_files[0], advanced_zarr)
        print(f"   ✓ Converted with advanced config: {os.path.basename(advanced_zarr)}")
        print()
        
        # Step 4: Demonstrate configuration file
        print("4. Configuration file support...")
        
        # Create YAML config file
        yaml_config = {
            "chunking": {"time": 50, "lat": 30, "lon": 60},
            "compression": {"method": "blosc:zstd:2", "clevel": 2},
            "packing": {"enabled": True, "bits": 16},
            "time": {"dim": "time", "append_dim": "time"},
            "variables": {"include": ["temperature", "pressure", "humidity"]},
            "attrs": {"title": "YAML Config Demo", "version": "1.0"},
            "retries_on_missing": 3,
            "missing_check_vars": "all"
        }
        
        yaml_file = os.path.join(tmpdir, "config.yaml")
        with open(yaml_file, "w") as f:
            yaml.dump(yaml_config, f, default_flow_style=False)
        
        # Load from config file
        converter_from_yaml = ZarrConverter.from_config_file(yaml_file)
        yaml_zarr = os.path.join(tmpdir, "yaml_conversion.zarr")
        converter_from_yaml.convert(data_files[1], yaml_zarr)
        print(f"   ✓ Converted with YAML config: {os.path.basename(yaml_zarr)}")
        print()
        
        # Step 5: Demonstrate intelligent chunking analysis
        print("5. Intelligent chunking analysis...")
        
        # Create converter without specifying chunking (will trigger analysis)
        smart_converter = ZarrConverter(
            config=ZarrConverterConfig(
                packing=PackingConfig(enabled=True, bits=16),
                attrs={"title": "Smart chunking demo"}
            )
        )
        
        smart_zarr = os.path.join(tmpdir, "smart_conversion.zarr")
        # Convert with intelligent chunking (no access_pattern parameter)
        smart_converter.convert(
            data_files[2], 
            smart_zarr
        )
        print(f"   ✓ Converted with smart chunking: {os.path.basename(smart_zarr)}")
        print()
        
        # Step 6: Demonstrate parallel writing (the key feature!)
        print("6. Parallel writing support...")
        
        # Use first file as template
        template_ds = xr.open_dataset(data_files[0])
        
        # Create parallel writing converter with simpler configuration
        parallel_converter = ZarrConverter(
            chunking={"time": 5, "lat": 5, "lon": 8},  # Smaller chunks for smaller data
            compression="blosc:zstd:3",
            packing=True,
            retries_on_missing=2,  # Enable retries for parallel writing
            missing_check_vars="all"
        )
        
        # Create template archive covering full time range
        parallel_zarr = os.path.join(tmpdir, "parallel_archive.zarr")
        parallel_converter.create_template(
            template_dataset=template_ds,
            output_path=parallel_zarr,
            global_start="2020-01-01",
            global_end="2020-01-12",  # Covering all files (12 days)
            compute=False  # Metadata only, no data computation
        )
        print(f"   ✓ Created template archive: {os.path.basename(parallel_zarr)}")
        
        # Write regions in "parallel" (simulating parallel processes)
        for i, data_file in enumerate(data_files):
            print(f"   Writing {os.path.basename(data_file)}...")
            parallel_converter.write_region(data_file, parallel_zarr)
        
        print(f"   ✓ All regions written successfully!")
        
        # Verify the final parallel archive
        final_parallel_ds = xr.open_zarr(parallel_zarr)
        print(f"   Final archive time range: {final_parallel_ds.time.values[0]} to {final_parallel_ds.time.values[-1]}")
        print(f"   Total time steps: {len(final_parallel_ds.time)}")
        print(f"   Variables: {list(final_parallel_ds.data_vars.keys())}")
        print()
        
        # Step 7: Demonstrate data appending
        print("7. Data appending...")
        
        # Create initial dataset
        append_converter = ZarrConverter()
        initial_zarr = os.path.join(tmpdir, "append_initial.zarr")
        append_converter.convert(data_files[0], initial_zarr)
        print(f"   ✓ Created initial archive: {os.path.basename(initial_zarr)}")
        
        # Append additional data
        append_converter.append(data_files[1], initial_zarr)
        print(f"   ✓ Appended {os.path.basename(data_files[1])} to archive")
        
        # Verify appended archive
        final_append_ds = xr.open_zarr(initial_zarr)
        print(f"   Final archive time steps: {len(final_append_ds.time)}")
        print()
        
        print("=== All demonstrations completed successfully! ===")


if __name__ == "__main__":
    demonstrate_all_features()