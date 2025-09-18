"""
Example demonstrating CLI functionality of zarrio.
"""

import sys
import os
import tempfile
import subprocess
import numpy as np
import pandas as pd
import xarray as xr

# Add the zarrio directory to the path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '.'))


def create_sample_data(filename: str) -> str:
    """Create sample NetCDF data."""
    # Create sample data
    data = np.random.random([10, 5, 6])
    ds = xr.Dataset(
        {
            "temperature": (("time", "lat", "lon"), data),
            "pressure": (("time", "lat", "lon"), data * 1000),
        },
        coords={
            "time": pd.date_range("2023-01-01", periods=10),
            "lat": np.linspace(-10, 10, 5),
            "lon": np.linspace(20, 50, 6),
        },
    )
    
    ds.to_netcdf(filename)
    return filename


def test_cli_functionality():
    """Test CLI functionality."""
    with tempfile.TemporaryDirectory() as tmpdir:
        # Create sample data
        ncfile = os.path.join(tmpdir, "sample.nc")
        create_sample_data(ncfile)
        
        print(f"Created sample data: {ncfile}")
        
        # Test help command
        print("\n1. Testing help command:")
        result = subprocess.run([
            sys.executable, "-m", "zarrio.cli", "--help"
        ], capture_output=True, text=True)
        
        if result.returncode == 0:
            print("✓ Help command works")
        else:
            print(f"✗ Help command failed: {result.stderr}")
            return False
        
        # Test convert command
        print("\n2. Testing convert command:")
        zarrfile = os.path.join(tmpdir, "output.zarr")
        result = subprocess.run([
            sys.executable, "-m", "zarrio.cli", "convert",
            ncfile, zarrfile
        ], capture_output=True, text=True)
        
        if result.returncode == 0:
            print("✓ Convert command works")
            if os.path.exists(zarrfile):
                print("✓ Zarr file created successfully")
            else:
                print("✗ Zarr file not created")
                return False
        else:
            print(f"✗ Convert command failed: {result.stderr}")
            return False
        
        # Test convert with chunking
        print("\n3. Testing convert with chunking:")
        zarrfile2 = os.path.join(tmpdir, "output2.zarr")
        result = subprocess.run([
            sys.executable, "-m", "zarrio.cli", "convert",
            ncfile, zarrfile2,
            "--chunking", "time:5,lat:3"
        ], capture_output=True, text=True)
        
        if result.returncode == 0:
            print("✓ Convert with chunking works")
            if os.path.exists(zarrfile2):
                print("✓ Zarr file created successfully")
            else:
                print("✗ Zarr file not created")
                return False
        else:
            print(f"✗ Convert with chunking failed: {result.stderr}")
            return False
        
        # Test version command
        print("\n4. Testing version command:")
        result = subprocess.run([
            sys.executable, "-m", "zarrio.cli", "--version"
        ], capture_output=True, text=True)
        
        if result.returncode == 0:
            print("✓ Version command works")
            print(f"  Version: {result.stdout.strip()}")
        else:
            print(f"✗ Version command failed: {result.stderr}")
            return False
        
        return True


def main():
    """Run CLI tests."""
    print("Testing zarrio CLI functionality...")
    
    success = test_cli_functionality()
    
    if success:
        print("\nAll CLI tests passed!")
        return 0
    else:
        print("\nSome CLI tests failed!")
        return 1


if __name__ == "__main__":
    sys.exit(main())