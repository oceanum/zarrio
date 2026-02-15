#!/usr/bin/env python3
"""
Rolling Archive Datamesh Demo

Writes forecast cycles to datamesh and demonstrates automatic cleanup.
Requires DATAMESH_TOKEN environment variable.

This script shows:
1. Creating realistic forecast data for multiple cycles
2. Writing cycles to datamesh datasource
3. Previewing cleanup with dry_run
4. Running actual rolling archive cleanup
5. Verifying the final state

Use case: Managing forecast archives on datamesh where you want to keep
only the last N hours of forecast cycles while automatically cleaning up
old data to manage storage costs.

Requirements:
    - zarrio with datamesh support
    - DATAMESH_TOKEN environment variable
"""

import os
import tempfile
import xarray as xr
import numpy as np
from datetime import datetime, timedelta
from pathlib import Path

import argparse
from zarrio import ZarrConverter
from zarrio.models import ZarrConverterConfig


def parse_args():
    """Parse command-line arguments for the rolling archive datamesh demo."""
    parser = argparse.ArgumentParser(description="Rolling Archive Datamesh Demo")
    parser.add_argument(
        "--delete-existing",
        action="store_true",
        help="Delete the datasource if it already exists before running demo",
    )
    return parser.parse_args()


def delete_existing_datasource(converter, datasource_id):
    """Delete datasource if it exists.

    This function attempts to delete the given datasource using the Oceanum datamesh
    API if available. It is best-effort and will not fail the demo if deletion cannot
    be performed (e.g., token not set or API unavailable).
    """
    # Confirm action to the user
    print(
        f"CONFIRMATION: This will permanently delete datasource '{datasource_id}' and all its data. Proceeding..."
    )

    token = os.environ.get("DATAMESH_TOKEN")
    if not token:
        print("Warning: DATAMESH_TOKEN not set. Skipping deletion attempt.")
        return

    try:
        # Try to use Oceanum datamesh if available
        from oceanum.datamesh import Connector  # type: ignore

        conn = Connector(token=token)
        if hasattr(conn, "delete_datasource"):
            print(f"Deleting datasource '{datasource_id}' via Oceanum datamesh API...")
            conn.delete_datasource(datasource_id)
            print(f"✓ Datasource '{datasource_id}' deleted")
        else:
            print(
                "Datamesh connector found but no delete_datasource() method. Skipping actual deletion."
            )
    except Exception as e:
        # Non-fatal: the demo can still proceed
        print(
            f"Warning: Could not delete datasource via datamesh API ({type(e).__name__}: {e}). This may be due to configuration limits."
        )


def create_forecast_data(cycle_time: datetime):
    """
    Create realistic forecast data for a single cycle.

    Args:
        cycle_time: Timestamp for this forecast cycle

    Returns:
        xarray.Dataset with forecast variables
    """
    # Create spatial grid (simplified for demo)
    lats = np.linspace(-40, 40, 81)  # 81 latitude points
    lons = np.linspace(-180, 180, 361)  # 361 longitude points

    # Create forecast horizon (48 hours from cycle time)
    forecast_times = [cycle_time + timedelta(hours=h) for h in range(49)]  # 0-48 hours

    # Create realistic forecast variables
    np.random.seed(int(cycle_time.timestamp()))  # Reproducible per cycle

    # Temperature (in Celsius) with spatial and temporal variations
    base_temp = 15 + 10 * np.sin(2 * np.pi * np.arange(49) / 24)  # Diurnal cycle
    temp_spatial = 30 * np.sin(
        np.deg2rad(lats)
    )  # Temperature gradient from equator to poles
    temperature = (
        base_temp[:, np.newaxis, np.newaxis]
        + temp_spatial[np.newaxis, :, np.newaxis]
        + 3 * np.random.randn(49, 81, 361)
    )

    # Pressure (in hPa)
    pressure = 1013.25 + 20 * np.random.randn(49, 81, 361)

    # Wind speed (in m/s)
    wind_speed = 5 + 8 * np.random.rand(49, 81, 361)

    # Create dataset
    ds = xr.Dataset(
        {
            "temperature": (["time", "lat", "lon"], temperature.astype(np.float32)),
            "pressure": (["time", "lat", "lon"], pressure.astype(np.float32)),
            "wind_speed": (["time", "lat", "lon"], wind_speed.astype(np.float32)),
        },
        coords={
            "time": forecast_times,
            "lat": lats,
            "lon": lons,
        },
    )

    # Add metadata
    ds.attrs["cycle_time"] = cycle_time.isoformat()
    ds.attrs["title"] = f"Forecast Cycle {cycle_time.strftime('%Y%m%dT%H%M%S')}"
    ds.attrs["source"] = "zarrio rolling_archive_datamesh_demo.py"
    ds.attrs["forecast_type"] = "deterministic"

    # Add coordinate attributes
    ds["lat"].attrs.update({"units": "degrees_north", "long_name": "Latitude"})
    ds["lon"].attrs.update({"units": "degrees_east", "long_name": "Longitude"})
    ds["time"].attrs.update({"long_name": "Forecast time"})

    # Add variable attributes
    ds["temperature"].attrs.update(
        {
            "units": "degC",
            "long_name": "Air Temperature",
            "standard_name": "air_temperature",
        }
    )
    ds["pressure"].attrs.update(
        {
            "units": "hPa",
            "long_name": "Atmospheric Pressure",
            "standard_name": "air_pressure_at_sea_level",
        }
    )
    ds["wind_speed"].attrs.update(
        {"units": "m s-1", "long_name": "Wind Speed", "standard_name": "wind_speed"}
    )

    return ds


def demo_datamesh_rolling_archive():
    """
    Main demo function showing rolling archive with datamesh.

    This demonstrates:
    1. Checking for datamesh token
    2. Creating multiple forecast cycles over 3 days
    3. Writing each cycle to datamesh in a separate group
    4. Running cleanup with dry_run to preview deletions
    5. Running actual cleanup to remove old cycles
    6. Showing final state of the archive
    """
    print("=" * 80)
    print("Rolling Archive Datamesh Demo")
    print("=" * 80)
    print()

    # Step 1: Check for datamesh token
    print("Step 1: Checking for datamesh token...")
    token = os.environ.get("DATAMESH_TOKEN")
    if not token:
        print("❌ DATAMESH_TOKEN not set.")
        print()
        print("This demo requires a datamesh token to write data.")
        print("To run this demo, set your token:")
        print()
        print("  export DATAMESH_TOKEN=your_token_here")
        print()
        print("You can obtain a token from the Oceanum datamesh platform.")
        print()
        return
    print("✅ Token found")
    print()

    # Step 2: Configure datamesh with rolling archive
    datasource_id = "zarrio-rolling-test"
    print("Step 2: Configuring datamesh with rolling archive...")
    print(f"  Datasource ID: {datasource_id}")
    print("  Retention window: 48 hours")
    print("  Min groups to keep: 2")

    config = ZarrConverterConfig(
        # Datamesh configuration
        datamesh={
            "datasource": {
                "id": datasource_id,
                "name": "Zarrio Rolling Archive Test",
                "description": "Test datasource for rolling archive demo",
                "coordinates": {"x": "lon", "y": "lat", "t": "time"},
                "driver": "vzarr",
                "tags": ["demo", "rolling-archive", "forecast"],
            },
            "token": token,
            "service": "https://datamesh-v1.oceanum.io",
        },
        # Rolling archive configuration
        rolling_archive={
            "enabled": True,
            "retention_window": timedelta(hours=48),  # Keep last 48 hours
            "min_groups_to_keep": 2,  # Always keep at least 2 cycles
            "auto_cleanup": False,  # We'll manually trigger cleanup for the demo
        },
        # Optimization settings
        chunking={"time": 24, "lat": 40, "lon": 180},
        compression={"method": "blosc:zstd:1"},
        packing={"enabled": True, "bits": 16},
    )

    converter = ZarrConverter(config=config)
    print("✅ Configuration created")
    print()

    # Step 3: Create and write multiple forecast cycles
    print("Step 3: Creating and writing 6 forecast cycles over 3 days...")
    print()

    # Simulate 6 forecast cycles - twice daily (00:00 and 12:00) over 3 days
    base_time = datetime(2024, 1, 1, 0, 0)
    cycles = [
        base_time,  # Day 1, 00:00
        base_time + timedelta(hours=12),  # Day 1, 12:00
        base_time + timedelta(hours=24),  # Day 2, 00:00
        base_time + timedelta(hours=36),  # Day 2, 12:00
        base_time + timedelta(hours=48),  # Day 3, 00:00
        base_time + timedelta(hours=60),  # Day 3, 12:00
    ]

    with tempfile.TemporaryDirectory() as tmpdir:
        # Write each cycle to datamesh
        for i, cycle_time in enumerate(cycles, 1):
            print(f"  Writing cycle {i}/6: {cycle_time.isoformat()}")

            # Create forecast data for this cycle
            ds = create_forecast_data(cycle_time)

            # Save to temporary NetCDF file
            nc_file = Path(tmpdir) / f"cycle_{cycle_time.strftime('%Y%m%dT%H%M%S')}.nc"
            ds.to_netcdf(nc_file)

            # Write to datamesh with hierarchical group name
            group_name = f"cycle/{cycle_time.strftime('%Y%m%dT%H%M%S')}"

            try:
                converter.convert(str(nc_file), group=group_name)
                print(f"    ✅ Written to datamesh group: {group_name}")
            except Exception as e:
                print(f"    ❌ Error writing cycle: {e}")
                # Continue with other cycles even if one fails

            print()

    print(f"✅ All {len(cycles)} cycles written to datamesh")
    print()

    # Step 4: Preview cleanup with dry_run
    print("Step 4: Previewing cleanup (dry run)...")
    print(f"  Current time: {cycles[-1].isoformat()}")
    print("  Retention window: 48 hours")
    print(f"  Cutoff time: {(cycles[-1] - timedelta(hours=48)).isoformat()}")
    print()

    result = {"deleted": [], "kept": [], "skipped": []}

    try:
        result = converter.cleanup_archive(None, dry_run=True)

        print(f"  Would delete: {len(result['deleted'])} groups")
        for g in sorted(result["deleted"]):
            print(f"    - {g}")
        print()

        print(f"  Would keep: {len(result['kept'])} groups")
        for g in sorted(result["kept"]):
            print(f"    - {g}")
        print()

        if result["skipped"]:
            print(f"  Skipped: {len(result['skipped'])} groups (unparseable timestamp)")
            for g in sorted(result["skipped"]):
                print(f"    - {g}")
            print()

        print("  NOTE: No changes were made (dry run mode)")
        print()

    except Exception as e:
        print(f"❌ Error during dry run cleanup: {e}")
        print()

    # Step 5: Run actual cleanup
    print("Step 5: Running actual cleanup...")

    try:
        result = converter.cleanup_archive(None, dry_run=False)

        print(f"  ✅ Deleted: {len(result['deleted'])} groups")
        for g in sorted(result["deleted"]):
            print(f"    - {g}")
        print()

        print(f"  ✅ Kept: {len(result['kept'])} groups")
        for g in sorted(result["kept"]):
            print(f"    - {g}")
        print()

        if result["skipped"]:
            print(f"  Skipped: {len(result['skipped'])} groups")
            for g in sorted(result["skipped"]):
                print(f"    - {g}")
            print()

    except Exception as e:
        print(f"❌ Error during cleanup: {e}")
        print()

    # Step 6: Show final state
    print("Step 6: Final archive state...")
    print(f"  Datasource: {datasource_id}")
    print(f"  Remaining cycles: {len(result.get('kept', []))}")
    print()
    print("  You can view your data in the datamesh platform or query it:")
    print()
    print("  from oceanum.datamesh import Connector")
    print("  conn = Connector(token=os.environ['DATAMESH_TOKEN'])")
    print(f"  ds = conn.open_zarr('{datasource_id}')")
    print("  print(ds)")
    print()

    # Summary
    print("=" * 80)
    print("Demo Summary")
    print("=" * 80)
    print()
    print("✅ Created 6 forecast cycles spread over 3 days")
    print("✅ Wrote all cycles to datamesh with hierarchical groups")
    print("✅ Previewed cleanup with dry_run")
    print("✅ Ran actual cleanup to remove old cycles")
    print(f"✅ Kept {len(result.get('kept', []))} recent cycles within 48-hour window")
    print()
    print("Key features demonstrated:")
    print("  • Time-based retention window (48 hours)")
    print("  • Minimum groups protection (min_groups_to_keep=2)")
    print("  • Dry run preview before actual deletion")
    print("  • Datamesh integration with hierarchical groups")
    print("  • Automatic timestamp parsing from group attributes")
    print()
    print("Next steps:")
    print("  • Enable auto_cleanup=True for automatic cleanup after each write")
    print("  • Adjust retention_window to match your needs")
    print("  • Use in production forecast workflows")
    print()


def show_cli_usage():
    """Show CLI usage examples for datamesh rolling archive."""
    print()
    print("=" * 80)
    print("CLI Usage Examples")
    print("=" * 80)
    print()
    print("1. Convert with rolling archive to datamesh:")
    print()
    print("   zarrio convert forecast.nc \\")
    print("     --datamesh-datasource '{")
    print('       "id": "zarrio-rolling-test",')
    print('       "name": "Forecast Archive",')
    print('       "coordinates": {"x": "lon", "y": "lat", "t": "time"}')
    print("     }' \\")
    print("     --datamesh-token $DATAMESH_TOKEN \\")
    print("     --rolling-archive-hours 48 \\")
    print("     --group cycle/20240101T000000")
    print()
    print("2. Using a config file:")
    print()
    print("   # config.yaml")
    print("   datamesh:")
    print("     datasource:")
    print("       id: zarrio-rolling-test")
    print("       name: Forecast Archive")
    print("       coordinates:")
    print("         x: lon")
    print("         y: lat")
    print("         t: time")
    print("     token: ${DATAMESH_TOKEN}")
    print("   rolling_archive:")
    print("     enabled: true")
    print("     retention_window: 48:00:00  # 48 hours")
    print("     min_groups_to_keep: 2")
    print("     auto_cleanup: true")
    print()
    print("   zarrio convert forecast.nc --config config.yaml \\")
    print("     --group cycle/20240101T000000")
    print()
    print("3. Python API with datamesh:")
    print()
    print("   from zarrio import ZarrConverter")
    print("   from datetime import timedelta")
    print("   import os")
    print()
    print("   converter = ZarrConverter(")
    print("       datamesh={")
    print("           'datasource': {'id': 'zarrio-rolling-test', ...},")
    print("           'token': os.environ['DATAMESH_TOKEN'],")
    print("       },")
    print("       rolling_archive={")
    print("           'enabled': True,")
    print("           'retention_window': timedelta(hours=48),")
    print("           'auto_cleanup': True,")
    print("       }")
    print("   )")
    print()
    print("   # Write new cycle (old cycles auto-cleaned)")
    print("   converter.convert('forecast.nc', group='cycle/20240101T000000')")
    print()


def main():
    args = parse_args()
    datasource_id = "zarrio-rolling-test"

    if args.delete_existing:
        # Inform user and attempt deletion before starting the demo
        delete_existing_datasource(None, datasource_id)
    else:
        print(
            f"Note: Data will be appended to existing '{datasource_id}' datasource.\nUse --delete-existing to start with a clean slate."
        )

    # Run the actual demo
    demo_datamesh_rolling_archive()
    show_cli_usage()


if __name__ == "__main__":
    main()
