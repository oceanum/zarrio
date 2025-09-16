"""
Diagnostic script to understand what's happening with the compression testing.
"""

import numpy as np
import xarray as xr
import pandas as pd
import tempfile
import os


def diagnose_dataset_structure(ds):
    """Diagnose the structure of a dataset."""
    print("=== Dataset Diagnosis ===")
    print(f"Dimensions: {dict(ds.dims)}")
    print(f"Variables: {list(ds.data_vars.keys())}")
    print(f"Coordinates: {list(ds.coords.keys())}")
    
    print("\nVariable Details:")
    print("-" * 20)
    for var_name in list(ds.data_vars.keys())[:5]:  # Show first 5 variables
        var = ds[var_name]
        print(f"{var_name}:")
        print(f"  Shape: {var.shape}")
        print(f"  Dtype: {var.dtype}")
        print(f"  Size (elements): {np.prod(var.shape)}")
        size_mb = np.prod(var.shape) * var.dtype.itemsize / (1024**2)
        print(f"  Raw size estimate: {size_mb:.2f} MB")
        if 'valid_min' in var.attrs and 'valid_max' in var.attrs:
            print(f"  Valid range: [{var.attrs['valid_min']}, {var.attrs['valid_max']}]")
    
    if len(ds.data_vars) > 5:
        print(f"... and {len(ds.data_vars) - 5} more variables")
    
    # Total size estimate
    total_elements = sum(np.prod(var.shape) for var in ds.data_vars.values())
    total_size_mb = total_elements * 4 / (1024**2)  # Assuming float32
    print(f"\nTotal elements: {total_elements:,}")
    print(f"Total size estimate (float32): {total_size_mb:.2f} MB")


def create_minimal_test_subset(ds, time_steps=5):
    """Create a minimal subset for testing."""
    print("\n=== Creating Minimal Test Subset ===")
    
    # Take a small time subset
    if 'time' in ds.dims and ds.dims['time'] > time_steps:
        test_ds = ds.isel(time=slice(0, time_steps))
        print(f"Reduced time dimension from {ds.dims['time']} to {time_steps}")
    else:
        test_ds = ds.copy()
        print(f"Using all {ds.dims.get('time', 'N/A')} time steps")
    
    # Show reduced size
    new_total_elements = sum(np.prod(var.shape) for var in test_ds.data_vars.values())
    new_total_size_mb = new_total_elements * 4 / (1024**2)
    print(f"New total elements: {new_total_elements:,}")
    print(f"New size estimate: {new_total_size_mb:.2f} MB")
    
    return test_ds


def test_simple_write_operation(test_ds):
    """Test a simple write operation to understand what's happening."""
    print("\n=== Simple Write Test ===")
    
    with tempfile.TemporaryDirectory() as tmp_dir:
        zarr_path = os.path.join(tmp_dir, "test.zarr")
        
        # Simple write without any compression
        print("Writing dataset...")
        test_ds.to_zarr(zarr_path, mode="w")
        print("Write completed.")
        
        # Measure actual size
        def get_size(start_path):
            total_size = 0
            for dirpath, dirnames, filenames in os.walk(start_path):
                for filename in filenames:
                    filepath = os.path.join(dirpath, filename)
                    try:
                        total_size += os.path.getsize(filepath)
                    except:
                        pass
            return total_size
        
        if os.path.exists(zarr_path):
            size_bytes = get_size(zarr_path)
            size_mb = size_bytes / (1024**2)
            print(f"Actual Zarr store size: {size_mb:.2f} MB")
            
            # Compare with expected
            expected_elements = sum(np.prod(var.shape) for var in test_ds.data_vars.values())
            expected_size_mb = expected_elements * 4 / (1024**2)  # float32 assumption
            print(f"Expected size (float32): {expected_size_mb:.2f} MB")
            print(f"Ratio: {size_mb/expected_size_mb:.2f}x actual/expected")
        else:
            print("Failed to create Zarr store")


def main():
    """Main diagnostic function."""
    # For now, let's create a simple test dataset to understand the issue
    print("Creating diagnostic test dataset...")
    
    # Simple 3D dataset
    time = pd.date_range("2000-01-01", periods=10, freq="D")
    lat = np.linspace(-90, 90, 50)
    lon = np.linspace(-180, 180, 100)
    
    # Create simple data
    temperature = 20 + 5 * np.random.randn(10, 50, 100)
    pressure = 101325 + 1000 * np.random.randn(10, 50, 100)
    
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
    
    # Add valid ranges for packing
    ds["temperature"].attrs["valid_min"] = -50.0
    ds["temperature"].attrs["valid_max"] = 50.0
    ds["pressure"].attrs["valid_min"] = 90000.0
    ds["pressure"].attrs["valid_max"] = 110000.0
    
    diagnose_dataset_structure(ds)
    
    # Create minimal subset
    test_ds = create_minimal_test_subset(ds, time_steps=5)
    
    # Test simple write
    test_simple_write_operation(test_ds)


if __name__ == "__main__":
    main()