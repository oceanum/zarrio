"""
Example demonstrating the enhanced packing functionality in zarrify.
"""

import numpy as np
import xarray as xr
import pandas as pd
import tempfile
import os

from zarrify.packing import Packer


def create_sample_dataset():
    """Create a sample dataset for testing."""
    # Create sample data
    time = pd.date_range("2000-01-01", periods=10, freq="D")
    lat = np.linspace(-90, 90, 18)
    lon = np.linspace(-180, 180, 36)
    
    # Create temperature data with known range
    temperature = 20 + 10 * np.random.randn(10, 18, 36)  # Mean 20°C, std 10°C
    pressure = 101325 + 5000 * np.random.randn(10, 18, 36)  # Mean ~101325 Pa
    
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
    
    return ds


def main():
    """Demonstrate the enhanced packing functionality."""
    print("Creating sample dataset...")
    ds = create_sample_dataset()
    
    # Example 1: Basic packing using variable attributes
    print("\n=== Example 1: Basic packing using variable attributes ===")
    
    # Add valid range attributes to dataset
    ds_with_attrs = ds.copy()
    ds_with_attrs["temperature"].attrs["valid_min"] = -50.0
    ds_with_attrs["temperature"].attrs["valid_max"] = 50.0
    ds_with_attrs["pressure"].attrs["valid_min"] = 90000.0
    ds_with_attrs["pressure"].attrs["valid_max"] = 110000.0
    
    # Setup encoding using attributes
    packer = Packer(nbits=16)
    encoding = packer.setup_encoding(ds_with_attrs)
    
    print(f"Created encoding for {len(encoding)} variables")
    for var_name, enc in encoding.items():
        print(f"  {var_name}: scale and offset encoding configured")
    
    # Example 2: Packing with manual ranges
    print("\n=== Example 2: Packing with manual ranges ===")
    
    # Setup encoding with manual ranges
    manual_ranges = {
        "temperature": {"min": -50, "max": 50},
        "pressure": {"min": 90000, "max": 110000}
    }
    encoding = packer.setup_encoding(ds, manual_ranges=manual_ranges)
    
    print(f"Created encoding for {len(encoding)} variables with manual ranges")
    for var_name, enc in encoding.items():
        print(f"  {var_name}: scale and offset encoding configured")
    
    # Example 3: Packing with automatic range calculation
    print("\n=== Example 3: Packing with automatic range calculation ===")
    
    # Setup encoding with auto-calculated ranges
    encoding = packer.setup_encoding(ds, auto_buffer_factor=0.05)
    
    print(f"Created encoding for {len(encoding)} variables with auto-calculated ranges")
    for var_name, enc in encoding.items():
        print(f"  {var_name}: scale and offset encoding configured")
    
    # Example 4: Priority order demonstration
    print("\n=== Example 4: Priority order demonstration ===")
    
    # Setup encoding with manual ranges that override attributes
    encoding = packer.setup_encoding(
        ds_with_attrs,  # Dataset with attributes
        manual_ranges={  # But also providing manual ranges
            "temperature": {"min": -100, "max": 100},
            "pressure": {"min": 80000, "max": 120000}
        }
    )
    
    print("Using manual ranges that override existing attributes")
    print("You should see a warning about overriding attributes")
    
    # Example 5: Range exceeded checking
    print("\n=== Example 5: Range exceeded checking ===")
    
    # Create dataset with data exceeding specified range
    ds_exceed = ds.copy()
    ds_exceed["temperature"].data[0, 0, 0] = 1000  # Way outside expected range
    
    # Setup encoding with range checking
    try:
        encoding = packer.setup_encoding(
            ds_exceed,
            manual_ranges={"temperature": {"min": -50, "max": 50}},
            check_range_exceeded=True,
            range_exceeded_action="error"  # Will raise exception
        )
        print("No exception raised - this is unexpected")
    except ValueError as e:
        print(f"Caught expected exception: {e}")
    
    print("\nAll examples completed successfully!")


if __name__ == "__main__":
    main()