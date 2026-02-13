#!/usr/bin/env python3
"""
Integration test script for hierarchical Zarr groups with zarrio.

Creates 3 cycle groups with:
- Same lat/lon coordinates
- Time coordinate varying by cycle (6 hours apart)
- Each cycle: 1 day of hourly data
- Groups named with ISO timestamps: cycle/20240101T000000, cycle/20240101T060000, etc.
"""

import tempfile
import xarray as xr
import numpy as np
from pathlib import Path
from datetime import datetime, timedelta
from zarrio import ZarrConverter


def create_cycle_dataset(cycle_time: datetime) -> xr.Dataset:
    """Create a dataset for a specific cycle."""
    lat = np.linspace(-90, 90, 18)
    lon = np.linspace(0, 355, 72)
    time = [cycle_time + timedelta(hours=i) for i in range(24)]

    np.random.seed(int(cycle_time.timestamp()))
    temperature = np.random.randn(24, len(lat), len(lon)) * 10 + 15
    precipitation = np.random.rand(24, len(lat), len(lon)) * 10

    ds = xr.Dataset(
        {
            "temperature": (["time", "lat", "lon"], temperature),
            "precipitation": (["time", "lat", "lon"], precipitation),
        },
        coords={
            "time": time,
            "lat": lat,
            "lon": lon,
        },
        attrs={
            "cycle_time": cycle_time.isoformat(),
            "description": f"Cycle {cycle_time.strftime('%Y-%m-%d %H:%M')} forecast data",
        },
    )

    ds["temperature"].attrs = {"units": "celsius", "long_name": "Air Temperature"}
    ds["precipitation"].attrs = {"units": "mm", "long_name": "Precipitation"}
    ds["lat"].attrs = {"units": "degrees_north", "long_name": "Latitude"}
    ds["lon"].attrs = {"units": "degrees_east", "long_name": "Longitude"}

    return ds


def cycle_time_to_group_name(cycle_time: datetime) -> str:
    """Convert cycle datetime to group name."""
    # Format: cycle/20240101T000000 (compact ISO-like format safe for paths)
    return f"cycle/{cycle_time.strftime('%Y%m%dT%H%M%S')}"


def test_hierarchical_groups():
    """Test writing and reading hierarchical Zarr groups."""

    print("=" * 70)
    print("Testing Hierarchical Zarr Groups with zarrio")
    print("=" * 70)

    with tempfile.TemporaryDirectory() as tmpdir:
        tmpdir = Path(tmpdir)
        zarr_path = tmpdir / "test_cycles.zarr"

        base_cycle_time = datetime(2024, 1, 1, 0, 0, 0)
        cycle_interval = timedelta(hours=6)
        num_cycles = 3

        converter = ZarrConverter()

        print(f"\nCreating {num_cycles} cycles:")
        print(f"  Base time: {base_cycle_time}")
        print(f"  Interval: {cycle_interval}")
        print(f"  Output: {zarr_path}")
        print()

        # Generate cycle times and group names
        cycle_times = [base_cycle_time + i * cycle_interval for i in range(num_cycles)]
        group_names = [cycle_time_to_group_name(t) for t in cycle_times]

        # Create and write each cycle
        for i, (cycle_time, group_name) in enumerate(zip(cycle_times, group_names), 1):
            temp_zarr = tmpdir / f"temp_cycle_{i}.zarr"

            print(f"Cycle {i}:")
            print(f"  Time range: {cycle_time} to {cycle_time + timedelta(days=1)}")
            print(f"  Group: {group_name}")

            ds = create_cycle_dataset(cycle_time)
            ds.to_zarr(temp_zarr, mode="w")
            converter.convert(str(temp_zarr), str(zarr_path), group=group_name)

            print(f"  Written to {group_name}")
            print()

        # Verification
        print("=" * 70)
        print("Verification - Reading back data")
        print("=" * 70)
        print()

        for cycle_time, group_name in zip(cycle_times, group_names):
            ds_read = xr.open_zarr(str(zarr_path), group=group_name)

            print(f"Group: {group_name}")
            print(f"  Cycle time: {cycle_time}")
            print(f"  Variables: {list(ds_read.data_vars)}")
            print(
                f"  Time range: {ds_read.time.values[0]} to {ds_read.time.values[-1]}"
            )
            print(
                f"  Lat range: {ds_read.lat.values.min():.1f} to {ds_read.lat.values.max():.1f}"
            )
            print(
                f"  Lon range: {ds_read.lon.values.min():.1f} to {ds_read.lon.values.max():.1f}"
            )
            print(f"  Data shape: {ds_read.temperature.shape}")
            print(f"  Successfully read")
            print()

        # Test group isolation
        print("=" * 70)
        print("Checking group isolation")
        print("=" * 70)
        print()

        ds_first = xr.open_zarr(str(zarr_path), group=group_names[0])
        ds_second = xr.open_zarr(str(zarr_path), group=group_names[1])
        ds_third = xr.open_zarr(str(zarr_path), group=group_names[2])

        time_first = ds_first.time.values[0]
        time_second = ds_second.time.values[0]
        time_third = ds_third.time.values[0]

        print(f"First cycle ({group_names[0]}): {time_first}")
        print(f"Second cycle ({group_names[1]}): {time_second}")
        print(f"Third cycle ({group_names[2]}): {time_third}")
        print()

        diff_1_2 = (time_second - time_first).astype(
            "timedelta64[h]"
        ).item().total_seconds() / 3600
        diff_2_3 = (time_third - time_second).astype(
            "timedelta64[h]"
        ).item().total_seconds() / 3600

        print(f"Time difference 1->2: {diff_1_2:.1f} hours")
        print(f"Time difference 2->3: {diff_2_3:.1f} hours")

        assert abs(diff_1_2 - 6) < 0.1, "Expected 6 hour difference"
        assert abs(diff_2_3 - 6) < 0.1, "Expected 6 hour difference"

        print()
        print("All cycles correctly spaced by 6 hours")
        print()

        # Test append functionality to first group
        print("=" * 70)
        print("Testing append to existing group")
        print("=" * 70)
        print()

        next_day_time = cycle_times[0] + timedelta(days=1)
        ds_append = create_cycle_dataset(next_day_time)
        temp_append_zarr = tmpdir / "temp_append.zarr"
        ds_append.to_zarr(temp_append_zarr, mode="w")

        converter.append(str(temp_append_zarr), str(zarr_path), group=group_names[0])

        ds_appended = xr.open_zarr(str(zarr_path), group=group_names[0])
        print(f"Group {group_names[0]} after append:")
        print(
            f"  Time range: {ds_appended.time.values[0]} to {ds_appended.time.values[-1]}"
        )
        print(f"  Total timesteps: {len(ds_appended.time)}")
        print(f"  Successfully appended (now has {len(ds_appended.time)} hours)")
        print()

        print("=" * 70)
        print("SUCCESS: All tests passed!")
        print("=" * 70)
        print()
        print(f"Zarr store location: {zarr_path}")
        print()
        print("Groups created:")
        for group_name in group_names:
            print(f"  - {group_name}")
        print()
        print("You can explore the data with:")
        print(f"  import xarray as xr")
        print(f"  ds = xr.open_zarr('{zarr_path}', group='{group_names[0]}')")


if __name__ == "__main__":
    test_hierarchical_groups()
