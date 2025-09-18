"""
Tests for zarrio time functionality.
"""

import pytest
import numpy as np
import pandas as pd
import xarray as xr

from zarrio.time import TimeManager


def test_time_manager_initialization():
    """Test TimeManager initialization."""
    tm = TimeManager(time_dim="time")
    assert tm.time_dim == "time"
    
    tm = TimeManager(time_dim="forecast_time")
    assert tm.time_dim == "forecast_time"


def test_remove_duplicates():
    """Test removing duplicate times."""
    # Create dataset with duplicate times
    times = pd.to_datetime([
        "2000-01-01", "2000-01-02", "2000-01-02", "2000-01-03", "2000-01-03", "2000-01-04"
    ])
    data = np.random.random([6, 3])
    ds = xr.Dataset(
        {
            "temperature": (("time", "lat"), data),
        },
        coords={
            "time": times,
            "lat": [-10, 0, 10],
        },
    )
    
    # Remove duplicates
    tm = TimeManager()
    ds_unique = tm.remove_duplicates(ds)
    
    # Check that duplicates were removed
    assert len(ds_unique.time) == 4
    assert np.array_equal(ds_unique.time.values, pd.to_datetime([
        "2000-01-01", "2000-01-02", "2000-01-03", "2000-01-04"
    ]))


def test_get_time_bounds():
    """Test getting time bounds."""
    # Create test dataset
    times = pd.date_range("2000-01-01", periods=10)
    data = np.random.random([10, 3])
    ds = xr.Dataset(
        {
            "temperature": (("time", "lat"), data),
        },
        coords={
            "time": times,
            "lat": [-10, 0, 10],
        },
    )
    
    # Get time bounds
    tm = TimeManager()
    start, end = tm.get_time_bounds(ds)
    
    # Check bounds
    assert start == pd.Timestamp("2000-01-01")
    assert end == pd.Timestamp("2000-01-10")


def test_align_for_append_no_overlap():
    """Test aligning datasets with no overlap."""
    # Create existing dataset
    existing_times = pd.date_range("2000-01-01", periods=5)
    existing_data = np.random.random([5, 3])
    existing_ds = xr.Dataset(
        {
            "temperature": (("time", "lat"), existing_data),
        },
        coords={
            "time": existing_times,
            "lat": [-10, 0, 10],
        },
    )
    
    # Create new dataset with no overlap
    new_times = pd.date_range("2000-01-06", periods=5)
    new_data = np.random.random([5, 3])
    new_ds = xr.Dataset(
        {
            "temperature": (("time", "lat"), new_data),
        },
        coords={
            "time": new_times,
            "lat": [-10, 0, 10],
        },
    )
    
    # Align for append
    tm = TimeManager()
    aligned_ds = tm.align_for_append(existing_ds, new_ds)
    
    # Should return the new dataset unchanged
    assert aligned_ds.equals(new_ds)


def test_align_for_append_with_overlap():
    """Test aligning datasets with overlap."""
    # Create existing dataset (2000-01-01 to 2000-01-10)
    existing_times = pd.date_range("2000-01-01", periods=10)
    existing_data = np.random.random([10, 3])
    existing_ds = xr.Dataset(
        {
            "temperature": (("time", "lat"), existing_data),
        },
        coords={
            "time": existing_times,
            "lat": [-10, 0, 10],
        },
    )
    
    # Create new dataset with overlap (2000-01-06 to 2000-01-15)
    new_times = pd.date_range("2000-01-06", periods=10)
    new_data = np.random.random([10, 3])
    new_ds = xr.Dataset(
        {
            "temperature": (("time", "lat"), new_data),
        },
        coords={
            "time": new_times,
            "lat": [-10, 0, 10],
        },
    )
    
    # Align for append
    tm = TimeManager()
    aligned_ds = tm.align_for_append(existing_ds, new_ds)
    
    # Should return only the non-overlapping portion (2000-01-11 to 2000-01-15)
    # But since we're selecting from 2000-01-06 and existing ends at 2000-01-10,
    # we should get data from 2000-01-11 onwards
    expected_times = pd.date_range("2000-01-11", periods=5)
    assert len(aligned_ds.time) == 5
    # The actual times should be from 2000-01-11 onwards
    assert aligned_ds.time.values[0] >= pd.Timestamp("2000-01-11")


def test_add_time_attributes():
    """Test adding time attributes."""
    # Create test dataset
    times = pd.date_range("2000-01-01", periods=10)
    data = np.random.random([10, 3])
    ds = xr.Dataset(
        {
            "temperature": (("time", "lat"), data),
        },
        coords={
            "time": times,
            "lat": [-10, 0, 10],
        },
    )
    
    # Add time attributes
    tm = TimeManager()
    ds_with_attrs = tm.add_time_attributes(ds)
    
    # Check that attributes were added
    assert "time_coverage_start" in ds_with_attrs.attrs
    assert "time_coverage_end" in ds_with_attrs.attrs
    assert ds_with_attrs.attrs["time_coverage_start"] == "2000-01-01 00:00:00"
    assert ds_with_attrs.attrs["time_coverage_end"] == "2000-01-10 00:00:00"


if __name__ == "__main__":
    pytest.main([__file__])