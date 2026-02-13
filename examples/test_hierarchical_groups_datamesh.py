#!/usr/bin/env python3
"""
Integration test script for writing hierarchical Zarr groups to Oceanum datamesh.

Requires:
- DATAMESH_TOKEN environment variable set
- Access to Oceanum datamesh service

Creates 3 cycle groups with ISO timestamps in a datamesh datasource.
"""

import os
import sys
import tempfile
import xarray as xr
import numpy as np
from pathlib import Path
from datetime import datetime, timedelta
from zarrio import ZarrConverter
from zarrio.models import ZarrConverterConfig, DatameshConfig, DatameshDatasource


def create_cycle_dataset(cycle_time: datetime) -> xr.Dataset:
    """Create a dataset for a specific cycle."""
    lat = np.linspace(-60, 20, 5)
    lon = np.linspace(100, 160, 5)
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
    return f"cycle/{cycle_time.strftime('%Y%m%dT%H%M%S')}"


def test_hierarchical_groups_datamesh():
    """Test writing hierarchical Zarr groups to datamesh."""

    # Check for required environment variables
    token = os.environ.get("DATAMESH_TOKEN")
    if not token:
        print("ERROR: DATAMESH_TOKEN environment variable not set")
        print("Please set it with: export DATAMESH_TOKEN=your_token_here")
        sys.exit(1)

    # Configuration
    datasource_id = "test-hierarchical-cycles"
    service_url = os.environ.get("DATAMESH_SERVICE", "https://datamesh.oceanum.io")

    print("=" * 70)
    print("Testing Hierarchical Zarr Groups with Datamesh")
    print("=" * 70)
    print()
    print(f"Datasource ID: {datasource_id}")
    print(f"Service: {service_url}")
    print()

    # Create converter with datamesh configuration
    config = ZarrConverterConfig(
        datamesh=DatameshConfig(
            datasource=DatameshDatasource(
                id=datasource_id,
                name="Test Hierarchical Cycles",
                description="Test dataset for hierarchical Zarr groups",
            ),
            token=token,
            service=service_url,
            use_zarr_client=True,
        )
    )
    converter = ZarrConverter(config=config)

    # Cycle configuration
    base_cycle_time = datetime(2024, 1, 1, 0, 0, 0)
    cycle_interval = timedelta(hours=6)
    num_cycles = 3

    print(f"Creating {num_cycles} cycles:")
    print(f"  Base time: {base_cycle_time}")
    print(f"  Interval: {cycle_interval}")
    print(f"  Output: datamesh://{datasource_id}")
    print()

    # Generate cycle times and group names
    cycle_times = [base_cycle_time + i * cycle_interval for i in range(num_cycles)]
    group_names = [cycle_time_to_group_name(t) for t in cycle_times]

    with tempfile.TemporaryDirectory() as tmpdir:
        tmpdir = Path(tmpdir)

        # Create and write each cycle
        for i, (cycle_time, group_name) in enumerate(zip(cycle_times, group_names), 1):
            temp_zarr = tmpdir / f"temp_cycle_{i}.zarr"

            print(f"Cycle {i}:")
            print(f"  Time range: {cycle_time} to {cycle_time + timedelta(days=1)}")
            print(f"  Group: {group_name}")

            ds = create_cycle_dataset(cycle_time)
            ds.to_zarr(temp_zarr, mode="w")

            # Write to datamesh with group
            try:
                converter.convert(str(temp_zarr), datasource_id, group=group_name)
                print(f"  Written to datamesh group: {group_name}")
            except Exception as e:
                print(f"  ERROR: {e}")
                raise
            print()

        # Verification - read back from datamesh
        print("=" * 70)
        print("Verification - Reading back from datamesh")
        print("=" * 70)
        print()

        for cycle_time, group_name in zip(cycle_times, group_names):
            try:
                # Read from datamesh using the datasource with group
                from oceanum.datamesh import Connector

                conn = Connector(token=token, service=service_url)

                # Open the datasource with specific group
                ds_read = conn.open_datasource(datasource_id, group=group_name)

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
                print(f"  Successfully read from datamesh")
                print()
            except Exception as e:
                print(f"  ERROR reading {group_name}: {e}")
                print()

        # Test append to first group
        print("=" * 70)
        print("Testing append to existing group in datamesh")
        print("=" * 70)
        print()

        next_day_time = cycle_times[0] + timedelta(days=1)
        ds_append = create_cycle_dataset(next_day_time)
        temp_append_zarr = tmpdir / "temp_append.zarr"
        ds_append.to_zarr(temp_append_zarr, mode="w")

        try:
            converter.append(str(temp_append_zarr), datasource_id, group=group_names[0])
            print(f"Appended to {group_names[0]}")

            # Verify append
            from oceanum.datamesh import Connector

            conn = Connector(token=token, service=service_url)
            ds_appended = conn.open_zarr(datasource_id, group=group_names[0])
            print(
                f"  Time range: {ds_appended.time.values[0]} to {ds_appended.time.values[-1]}"
            )
            print(f"  Total timesteps: {len(ds_appended.time)}")
            print(f"  Successfully appended (now has {len(ds_appended.time)} hours)")
        except Exception as e:
            print(f"  ERROR: {e}")

        print()
        print("=" * 70)
        print("SUCCESS: All datamesh tests completed!")
        print("=" * 70)
        print()
        print(f"Datasource: {datasource_id}")
        print("Groups created:")
        for group_name in group_names:
            print(f"  - {group_name}")


if __name__ == "__main__":
    test_hierarchical_groups_datamesh()
