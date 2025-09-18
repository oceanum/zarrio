"""
Complete example demonstrating datamesh integration with actual data writing.

This example creates test data and writes it to a datamesh datasource if a token is available.
"""

import os
import tempfile
import xarray as xr
import numpy as np
import pandas as pd
import uuid
from zarrio import ZarrConverter, ZarrConverterConfig


def create_sample_climate_data():
    """Create realistic sample climate data for demonstration."""
    print("Creating sample climate data...")
    
    # Create time dimension (10 days of hourly data)
    times = pd.date_range("2023-01-01", periods=240, freq="h")
    
    # Create spatial dimensions
    lats = np.linspace(-89.5, 89.5, 180)  # 180 latitude points
    lons = np.linspace(-179.5, 179.5, 360)  # 360 longitude points
    
    # Create realistic climate variables
    np.random.seed(42)  # For reproducible results
    
    # Temperature (in Celsius) - with seasonal cycle and random variations
    base_temp = 15 + 10 * np.sin(2 * np.pi * np.arange(240) / (24 * 30))  # Seasonal cycle
    temp_variations = 5 * np.random.randn(240, 180, 360)  # Random variations
    temperature = base_temp[:, np.newaxis, np.newaxis] + temp_variations
    
    # Pressure (in hPa) - around standard atmospheric pressure
    pressure = 1013.25 + 50 * np.random.randn(240, 180, 360)
    
    # Humidity (in %) - reasonable range
    humidity = 50 + 30 * np.random.rand(240, 180, 360)
    
    # Wind speed (in m/s) - reasonable range
    wind_speed = 5 + 10 * np.random.rand(240, 180, 360)
    
    # Create dataset
    ds = xr.Dataset(
        {
            "temperature": (["time", "lat", "lon"], temperature),
            "pressure": (["time", "lat", "lon"], pressure),
            "humidity": (["time", "lat", "lon"], humidity),
            "wind_speed": (["time", "lat", "lon"], wind_speed),
        },
        coords={
            "time": times,
            "lat": lats,
            "lon": lons,
        },
    )
    
    # Add metadata
    ds.attrs["title"] = "Sample Climate Data for Datamesh Demo"
    ds.attrs["source"] = "zarrio datamesh_demo.py"
    ds.attrs["created"] = pd.Timestamp.now().isoformat()
    
    # Add coordinate attributes
    ds["lat"].attrs["units"] = "degrees_north"
    ds["lat"].attrs["long_name"] = "Latitude"
    ds["lon"].attrs["units"] = "degrees_east"
    ds["lon"].attrs["long_name"] = "Longitude"
    ds["time"].attrs["long_name"] = "Time"
    
    # Add variable attributes
    ds["temperature"].attrs["units"] = "degC"
    ds["temperature"].attrs["long_name"] = "Air Temperature"
    ds["temperature"].attrs["standard_name"] = "air_temperature"
    
    ds["pressure"].attrs["units"] = "hPa"
    ds["pressure"].attrs["long_name"] = "Atmospheric Pressure"
    ds["pressure"].attrs["standard_name"] = "air_pressure_at_sea_level"
    
    ds["humidity"].attrs["units"] = "%"
    ds["humidity"].attrs["long_name"] = "Relative Humidity"
    ds["humidity"].attrs["standard_name"] = "relative_humidity"
    
    ds["wind_speed"].attrs["units"] = "m s-1"
    ds["wind_speed"].attrs["long_name"] = "Wind Speed"
    ds["wind_speed"].attrs["standard_name"] = "wind_speed"
    
    print(f"Created dataset with dimensions: {dict(ds.sizes)}")
    print(f"Variables: {list(ds.data_vars.keys())}")
    
    return ds


def main():
    """Main function demonstrating datamesh integration with actual data writing."""
    
    # Create sample data
    ds = create_sample_climate_data()
    
    # Save to temporary NetCDF file
    with tempfile.NamedTemporaryFile(suffix=".nc", delete=False) as tmp:
        nc_file = tmp.name
    
    try:
        ds.to_netcdf(nc_file)
        print(f"\nSaved sample data to temporary file: {nc_file}")
        
        # Get datamesh token from environment or use placeholder
        datamesh_token = os.environ.get("DATAMESH_TOKEN", "your_datamesh_token_here")
        has_token = datamesh_token != "your_datamesh_token_here"
        
        print(f"\n--- Datamesh Configuration ---")
        print(f"Token available: {has_token}")
        if not has_token:
            print("Note: DATAMESH_TOKEN environment variable not set")
            print("Demo will show configuration but won't actually write to datamesh")
        
        # Create a unique datasource ID to avoid conflicts
        datasource_id = f"zarrio_demo_climate_data_{uuid.uuid4().hex[:8]}"
        
        # Configure for datamesh
        config = ZarrConverterConfig(
            chunking={"time": 24, "lat": 90, "lon": 180},  # Reasonable chunking for climate data
            compression={"method": "blosc:zstd:1"},
            packing={"enabled": True, "bits": 16},  # Enable data packing
            datamesh={
                "datasource": {
                    "id": datasource_id,
                    "name": "Zarrify Demo Climate Data",
                    "description": "Sample climate data created by zarrio datamesh demo",
                    "coordinates": {"x": "lon", "y": "lat", "t": "time"},
                    "details": "https://github.com/oceanum/zarrio",
                    "tags": ["demo", "climate", "zarrio", "datamesh", "test"],
                },
                "token": datamesh_token,
                "service": "https://datamesh-v1.oceanum.io",
            }
        )
        
        # Create converter
        converter = ZarrConverter(config=config)
        print(f"Created ZarrConverter with datamesh configuration")
        print(f"Datasource ID: {config.datamesh.datasource.id}")
        print(f"Chunking: {config.chunking}")
        print(f"Packing enabled: {config.packing.enabled}")
        
        # Attempt to write data
        if has_token:
            print(f"\n--- Writing to Datamesh ---")
            try:
                converter.convert(nc_file)
                print("✅ Successfully wrote data to datamesh!")
                print("You can now access your data through the datamesh platform.")
            except Exception as e:
                error_msg = str(e)
                if "already has a write session" in error_msg:
                    print("ℹ️  Datamesh write session already exists for this datasource")
                    print("This is normal if you've run this demo recently.")
                    print("The data was likely written successfully in a previous run.")
                    print("To write again, you may need to wait for the session to expire or use a different datasource ID.")
                else:
                    print(f"❌ Error writing to datamesh: {e}")
                    print("Note: This might be due to permissions or network issues.")
        else:
            print(f"\n--- Demo Mode (No Token) ---")
            print("Showing what would happen with a real token:")
            print("converter.convert(nc_file)")
            print("# This would write the data to datamesh datasource 'zarrio_demo_climate_data'")
            
        # Show CLI usage
        print(f"\n--- CLI Usage ---")
        print("You can also use the CLI:")
        if has_token:
            print(f"zarrio convert data.nc \\")
            print(f"  --datamesh-datasource '{{\"id\":\"{datasource_id}\",\"name\":\"Demo Data\",\"coordinates\":{{\"x\":\"lon\",\"y\":\"lat\",\"t\":\"time\"}}}}' \\")
            print("  --datamesh-token $DATAMESH_TOKEN")
        else:
            print("export DATAMESH_TOKEN='your_actual_token_here'")
            print(f"zarrio convert data.nc \\")
            print(f"  --datamesh-datasource '{{\"id\":\"{datasource_id}\",\"name\":\"Demo Data\",\"coordinates\":{{\"x\":\"lon\",\"y\":\"lat\",\"t\":\"time\"}}}}' \\")
            print("  --datamesh-token $DATAMESH_TOKEN")
            
        print(f"\n--- Configuration Validation ---")
        # Show that validation works
        try:
            # This will fail because id is required
            invalid_config = ZarrConverterConfig(
                datamesh={
                    "datasource": {
                        "name": "Demo Data"
                        # Missing required 'id' field
                    }
                }
            )
        except Exception as e:
            print("✅ Validation correctly caught missing 'id' field")
            
        print(f"\n--- Summary ---")
        print("✅ Datamesh integration configured successfully")
        print("✅ Sample data created and saved")
        print("✅ Converter created with proper configuration")
        if has_token:
            print("✅ Data written to datamesh (if no errors above)")
        else:
            print("ℹ️  Set DATAMESH_TOKEN environment variable to actually write to datamesh")
            
    finally:
        # Clean up
        if os.path.exists(nc_file):
            os.unlink(nc_file)
            print(f"\nCleaned up temporary file: {nc_file}")


if __name__ == "__main__":
    main()