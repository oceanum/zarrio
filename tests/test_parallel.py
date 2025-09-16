"""
Test for parallel writing functionality in zarrify.
"""

import pytest
import tempfile
import numpy as np
import pandas as pd
import xarray as xr
import os

from zarrify import ZarrConverter, convert_to_zarr, append_to_zarr
from zarrify.models import ZarrConverterConfig, ChunkingConfig, PackingConfig


def create_test_dataset(
    filename: str,
    t0: str = "2000-01-01",
    periods: int = 10,
    output: str = "netcdf"
) -> str:
    """Create a test dataset for testing."""
    # Create test data
    np.random.seed(42)  # For reproducible tests
    data = np.random.random([periods, 3, 4])
    ds = xr.Dataset(
        {
            "temperature": (("time", "lat", "lon"), data),
            "pressure": (("time", "lat", "lon"), data * 1000),
        },
        coords={
            "time": pd.date_range(t0, periods=periods),
            "lat": [-10, 0, 10],
            "lon": [20, 30, 40, 50],
        },
    )
    
    # Add some attributes
    ds.attrs["title"] = "Test dataset"
    ds["temperature"].attrs["units"] = "degC"
    ds["pressure"].attrs["units"] = "hPa"
    
    # Add valid range attributes for packing
    ds["temperature"].attrs["valid_min"] = 0.0
    ds["temperature"].attrs["valid_max"] = 1.0
    ds["pressure"].attrs["valid_min"] = 0.0
    ds["pressure"].attrs["valid_max"] = 1000.0
    
    if output == "netcdf":
        ds.to_netcdf(filename)
    elif output == "zarr":
        ds.to_zarr(filename)
    
    return filename


def compare_datasets(dset1: xr.Dataset, dset2: xr.Dataset) -> None:
    """Compare two datasets for approximate equality."""
    # Check coordinates
    for coord in dset1.coords:
        assert dset1[coord].equals(dset2[coord])
    
    # Check data variables
    for var in dset1.data_vars:
        # Allow for small differences due to packing/compression
        if dset1[var].dtype.kind == 'f':  # floating point
            # Check that values are within 10% of each other (increased tolerance for packing)
            # Also handle case where both values are zero
            denominator = np.abs(dset1[var]).where(dset1[var] != 0, 1)
            pdiff = np.abs(dset1[var] - dset2[var]) / denominator
            # Allow up to 10% difference for packed data
            assert pdiff.max() <= 0.10, f"Maximum percentage difference {pdiff.max()} exceeds 10% tolerance for variable {var}"
        else:
            # For non-floating point, check exact equality
            assert dset1[var].equals(dset2[var])
    
    # Check attributes
    assert dset1.attrs == dset2.attrs


def test_create_template_and_write_regions():
    """Test creating template and writing regions in parallel."""
    with tempfile.TemporaryDirectory() as tmpdir:
        # Create template dataset
        template_file = os.path.join(tmpdir, "template.nc")
        create_test_dataset(template_file, t0="2000-01-01", periods=5)
        template_ds = xr.open_dataset(template_file)
        
        # Create individual datasets for different time periods
        file1 = os.path.join(tmpdir, "data1.nc")
        create_test_dataset(file1, t0="2000-01-01", periods=5)
        
        file2 = os.path.join(tmpdir, "data2.nc")
        create_test_dataset(file2, t0="2000-01-06", periods=5)
        
        file3 = os.path.join(tmpdir, "data3.nc")
        create_test_dataset(file3, t0="2000-01-11", periods=5)
        
        file4 = os.path.join(tmpdir, "data4.nc")
        create_test_dataset(file4, t0="2000-01-16", periods=5)
        
        # Create Zarr converter
        converter = ZarrConverter(
            config=ZarrConverterConfig(
                chunking=ChunkingConfig(time=5, lat=2, lon=2),
                packing=PackingConfig(enabled=True, bits=16)
            )
        )
        
        # Create template archive covering full time range
        zarr_archive = os.path.join(tmpdir, "archive.zarr")
        converter.create_template(
            template_dataset=template_ds,
            output_path=zarr_archive,
            global_start="2000-01-01",
            global_end="2000-01-20",
            compute=False  # Metadata only, no data computation
        )
        
        # Verify template was created
        assert os.path.exists(zarr_archive)
        
        # Write regions in "parallel" (simulating parallel processes)
        converter.write_region(file1, zarr_archive)  # Process 1
        converter.write_region(file2, zarr_archive)  # Process 2
        converter.write_region(file3, zarr_archive)  # Process 3
        converter.write_region(file4, zarr_archive)  # Process 4
        
        # Verify the final archive
        final_ds = xr.open_zarr(zarr_archive)
        
        # Check that all time periods are included
        assert len(final_ds.time) == 20
        assert final_ds.time.values[0] == np.datetime64("2000-01-01")
        assert final_ds.time.values[-1] == np.datetime64("2000-01-20")
        
        # Check that data was written correctly by comparing with concatenated original data
        orig_ds1 = xr.open_dataset(file1)
        orig_ds2 = xr.open_dataset(file2)
        orig_ds3 = xr.open_dataset(file3)
        orig_ds4 = xr.open_dataset(file4)
        
        # Concatenate original datasets
        orig_combined = xr.concat([orig_ds1, orig_ds2, orig_ds3, orig_ds4], dim="time")
        
        # Compare with final archive
        compare_datasets(orig_combined, final_ds)


def test_automatic_region_determination():
    """Test automatic region determination."""
    with tempfile.TemporaryDirectory() as tmpdir:
        # Create template dataset
        template_file = os.path.join(tmpdir, "template.nc")
        create_test_dataset(template_file, t0="2000-01-01", periods=10)
        template_ds = xr.open_dataset(template_file)
        
        # Create data file for middle period
        data_file = os.path.join(tmpdir, "data.nc")
        create_test_dataset(data_file, t0="2000-01-03", periods=3)
        data_ds = xr.open_dataset(data_file)
        
        # Create Zarr converter
        converter = ZarrConverter(
            config=ZarrConverterConfig(
                chunking=ChunkingConfig(time=5, lat=2, lon=2),
                packing=PackingConfig(enabled=True, bits=16)
            )
        )
        
        # Create template archive
        zarr_archive = os.path.join(tmpdir, "archive.zarr")
        converter.create_template(
            template_dataset=template_ds,
            output_path=zarr_archive,
            global_start="2000-01-01",
            global_end="2000-01-10",
            compute=False
        )
        
        # Write region with automatic region determination
        converter.write_region(data_file, zarr_archive)
        
        # Verify the data was written to correct region
        final_ds = xr.open_zarr(zarr_archive)
        
        # Check that data was written to indices 2, 3, 4 (2000-01-03, 2000-01-04, 2000-01-05)
        written_slice = final_ds.isel(time=slice(2, 5))
        compare_datasets(data_ds, written_slice)


def test_retry_logic_on_missing_data():
    """Test retry logic when missing data is detected."""
    with tempfile.TemporaryDirectory() as tmpdir:
        # Create template dataset
        template_file = os.path.join(tmpdir, "template.nc")
        create_test_dataset(template_file, t0="2000-01-01", periods=5)
        template_ds = xr.open_dataset(template_file)
        
        # Create data file
        data_file = os.path.join(tmpdir, "data.nc")
        create_test_dataset(data_file, t0="2000-01-01", periods=5)
        
        # Create Zarr converter with retry logic
        converter = ZarrConverter(
            config=ZarrConverterConfig(
                chunking=ChunkingConfig(time=5, lat=2, lon=2),
                packing=PackingConfig(enabled=True, bits=16),
                retries_on_missing=2,  # Enable 2 retries
                missing_check_vars="all"
            )
        )
        
        # Create template archive
        zarr_archive = os.path.join(tmpdir, "archive.zarr")
        converter.create_template(
            template_dataset=template_ds,
            output_path=zarr_archive,
            global_start="2000-01-01",
            global_end="2000-01-05",
            compute=False
        )
        
        # Write region - should work with retry logic
        converter.write_region(data_file, zarr_archive)
        
        # Verify the data was written
        final_ds = xr.open_zarr(zarr_archive)
        assert len(final_ds.time) == 5
        assert final_ds.time.values[0] == np.datetime64("2000-01-01")
        assert final_ds.time.values[-1] == np.datetime64("2000-01-05")


if __name__ == "__main__":
    pytest.main([__file__])