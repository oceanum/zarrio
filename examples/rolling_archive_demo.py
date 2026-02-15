#!/usr/bin/env python3
"""
Rolling Archive Demo

This script demonstrates the rolling archive feature which automatically
cleans up old forecast cycles from a Zarr store based on time-based retention.

Use case: Managing forecast archives where you want to keep only the
last N hours of forecast cycles (e.g., last 24 hours).

Requirements:
    - zarrio
    - zarr
    - tempfile
"""

import tempfile
import zarr
from datetime import datetime, timedelta
from pathlib import Path

from zarrio import ZarrConverter
from zarrio.models import ZarrConverterConfig


def create_forecast_store(store_path: Path):
    """Create a Zarr store with simulated forecast cycles.

    This creates a store structure similar to what you might get from
    a weather forecasting system that runs twice daily (00:00 and 12:00).
    """
    store = zarr.open_group(str(store_path), mode="w")

    # Simulate 6 forecast cycles over 3 days
    # Each cycle has a group with a cycle_time attribute
    # Create the parent 'cycle' group first, then create each forecast cycle as a subgroup
    cycles = [
        ("cycle/20240101T000000", datetime(2024, 1, 1, 0, 0)),
        ("cycle/20240101T120000", datetime(2024, 1, 1, 12, 0)),
        ("cycle/20240102T000000", datetime(2024, 1, 2, 0, 0)),
        ("cycle/20240102T120000", datetime(2024, 1, 2, 12, 0)),
        ("cycle/20240103T000000", datetime(2024, 1, 3, 0, 0)),
        ("cycle/20240103T120000", datetime(2024, 1, 3, 12, 0)),
    ]

    # Create the parent 'cycle' group first
    cycle_group = store.create_group("cycle")

    for group_name, cycle_time in cycles:
        # group_name includes the 'cycle/' prefix, so extract just the timestamp part
        timestamp_part = group_name.split("/")[-1]
        group = cycle_group.create_group(timestamp_part)
        # The cycle_time attribute is used by rolling archive to determine age
        group.attrs["cycle_time"] = cycle_time.isoformat()
        # Add some dummy data arrays
        group.array("temperature", data=[20.0 + i * 0.5 for i in range(3)], dtype="f4")
        group.array("pressure", data=[1013.0 + i for i in range(3)], dtype="f4")

    return cycles


def demo_rolling_archive():
    """Demonstrate rolling archive functionality.

    This demo shows:
    1. Creating a forecast archive with multiple cycles
    2. Configuring rolling archive with time-based retention
    3. Running cleanup with dry_run to preview deletions
    4. Running actual cleanup
    5. Verifying the final state
    """
    print("=" * 70)
    print("Rolling Archive Demo")
    print("=" * 70)
    print()

    with tempfile.TemporaryDirectory() as tmpdir:
        store_path = Path(tmpdir) / "forecast_archive.zarr"

        # Step 1: Create forecast store
        print("Step 1: Creating forecast archive with 6 cycles...")
        cycles = create_forecast_store(store_path)

        # Show initial state
        store = zarr.open_group(str(store_path), mode="r")
        # We now create a parent 'cycle' group and 6 subgroups under it.
        # Report the number of subgroups (cycles) for clarity.
        initial_cycle_count = len(cycles)
        print(f"  Created {initial_cycle_count} groups:")
        for g_name, g_time in cycles:
            print(f"    - {g_name} (cycle_time: {g_time.isoformat()})")
        print()

        # Step 2: Configure rolling archive (48 hour retention)
        print("Step 2: Configuring rolling archive (48 hour retention)...")
        config = ZarrConverterConfig(
            rolling_archive={
                "enabled": True,
                "retention_window": timedelta(hours=48),
                "min_groups_to_keep": 2,  # Always keep at least 2 cycles
                "auto_cleanup": False,  # We'll manually trigger cleanup
            }
        )
        converter = ZarrConverter(config=config)
        print("  Configuration:")
        print(f"    - Enabled: {config.rolling_archive.enabled}")
        print(f"    - Retention window: {config.rolling_archive.retention_window}")
        print(f"    - Min groups to keep: {config.rolling_archive.min_groups_to_keep}")
        print(f"    - Auto cleanup: {config.rolling_archive.auto_cleanup}")
        print()

        # Step 3: Dry run - preview what would be deleted
        print("Step 3: Running cleanup (dry run) to preview deletions...")
        result = converter.cleanup_archive(store_path, dry_run=True)
        print(f"  Would delete: {len(result['deleted'])} groups")
        for g in result["deleted"]:
            print(f"    - {g}")
        print(f"  Would keep: {len(result['kept'])} groups")
        for g in result["kept"]:
            print(f"    - {g}")
        if result["skipped"]:
            print(f"  Skipped: {len(result['skipped'])} groups (unparseable timestamp)")
            for g in result["skipped"]:
                print(f"    - {g}")
        print()
        print("  NOTE: No changes were made (dry run mode)")
        print()

        # Step 4: Run actual cleanup
        print("Step 4: Running actual cleanup...")
        result = converter.cleanup_archive(store_path, dry_run=False)
        print(f"  Deleted: {len(result['deleted'])} groups")
        for g in result["deleted"]:
            print(f"    - {g}")
        print(f"  Kept: {len(result['kept'])} groups")
        for g in result["kept"]:
            print(f"    - {g}")
        if result["skipped"]:
            print(f"  Skipped: {len(result['skipped'])} groups (unparseable timestamp)")
        print()

        # Step 5: Show final state
        print("Step 5: Final archive state...")
        store = zarr.open_group(str(store_path), mode="r")
        # Show final state: enumerate nested 'cycle' subgroup contents as well
        final_cycle_group = store.get("cycle") if hasattr(store, "get") else None
        if final_cycle_group is not None:
            subgroups = sorted(final_cycle_group.group_keys())
            print(f"  Remaining cycle groups: {len(subgroups)}")
            for name in subgroups:
                grp = final_cycle_group[name]
                cycle_time = grp.attrs.get("cycle_time", "unknown")
                print(f"    - cycle/{name} (cycle_time: {cycle_time})")
        else:
            final_groups = list(store.group_keys())
            print(f"  Remaining groups: {len(final_groups)}")
            for g in sorted(final_groups):
                group = store[g]
                cycle_time = group.attrs.get("cycle_time", "unknown")
                print(f"    - {g} (cycle_time: {cycle_time})")
        print()

        print("=" * 70)
        print("Demo complete!")
        print("=" * 70)


def demo_auto_cleanup():
    """Demonstrate automatic cleanup on write operations.

    This shows how rolling archive can automatically clean up old cycles
    after each write operation when auto_cleanup=True.
    """
    print()
    print("=" * 70)
    print("Auto Cleanup Demo")
    print("=" * 70)
    print()

    with tempfile.TemporaryDirectory() as tmpdir:
        store_path = Path(tmpdir) / "forecast_archive_autoclean.zarr"

        # Create initial archive
        print("Step 1: Creating initial forecast archive...")
        cycles = create_forecast_store(store_path)
        store = zarr.open_group(str(store_path), mode="r")
        print(f"  Created {len(list(store.group_keys()))} groups")
        print()

        # Configure with auto_cleanup enabled
        print("Step 2: Configuring with auto_cleanup=True...")
        config = ZarrConverterConfig(
            rolling_archive={
                "enabled": True,
                "retention_window": timedelta(hours=48),
                "min_groups_to_keep": 2,
                "auto_cleanup": True,  # Cleanup after each write
            }
        )
        converter = ZarrConverter(config=config)
        print("  Auto cleanup is enabled - cleanup runs after each write")
        print()

        # Note: In a real scenario, after calling convert(), old cycles
        # would be automatically cleaned up based on the retention window.
        # Here we just demonstrate the configuration.
        print("Step 3: In a real conversion, cleanup would run automatically...")
        print("  Example:")
        print(
            "    converter.convert('new_forecast.nc', 'archive.zarr', group='cycle/new'"
        )
        print("  After this call, old cycles outside the retention window are removed")
        print()

        print("=" * 70)


def demo_cli_usage():
    """Show CLI usage examples for rolling archive."""
    print()
    print("=" * 70)
    print("CLI Usage Examples")
    print("=" * 70)
    print()
    print("1. Convert with 24-hour rolling archive retention:")
    print("   $ zarrio convert forecast.nc archive.zarr --rolling-archive-hours 24")
    print()
    print("2. Convert with 48-hour retention and custom settings (via config file):")
    print("   $ zarrio convert forecast.nc archive.zarr --config config.yaml")
    print()
    print("3. Where config.yaml contains:")
    print("   rolling_archive:")
    print("     enabled: true")
    print("     retention_window: 48:00:00  # 48 hours")
    print("     min_groups_to_keep: 4")
    print("     auto_cleanup: true")
    print()
    print("4. Manual cleanup via Python API:")
    print("   from zarrio import ZarrConverter")
    print("   from datetime import timedelta")
    print()
    print("   converter = ZarrConverter(")
    print("       rolling_archive={")
    print("           'enabled': True,")
    print("           'retention_window': timedelta(hours=24),")
    print("       }")
    print("   )")
    print("   ")
    print("   # Preview what would be deleted")
    print("   result = converter.cleanup_archive('archive.zarr', dry_run=True)")
    print("   print(f\"Would delete: {result['deleted']}\")")
    print("   ")
    print("   # Actually delete")
    print("   result = converter.cleanup_archive('archive.zarr')")
    print("   print(f\"Deleted: {result['deleted']}\")")
    print()


def demo_configuration_options():
    """Show all configuration options for rolling archive."""
    print()
    print("=" * 70)
    print("Configuration Options")
    print("=" * 70)
    print()
    print("RollingArchiveConfig options:")
    print()
    print("  enabled (bool, default: False)")
    print("      Whether to enable rolling archive cleanup")
    print()
    print("  retention_window (timedelta, default: None)")
    print("      How long to keep data (e.g., timedelta(hours=24))")
    print("      Minimum 1 hour required")
    print()
    print("  time_reference_attr (str, default: 'cycle_time')")
    print("      Attribute name containing the timestamp for each group")
    print("      Can be any ISO format datetime string")
    print()
    print("  auto_cleanup (bool, default: True)")
    print("      If True, cleanup runs automatically after each write")
    print("      If False, you must call cleanup_archive() manually")
    print()
    print("  min_groups_to_keep (int, default: 1)")
    print("      Minimum number of groups to always preserve")
    print("      Useful to prevent accidental total deletion")
    print("      Must be >= 0")
    print()


if __name__ == "__main__":
    demo_rolling_archive()
    demo_auto_cleanup()
    demo_configuration_options()
    demo_cli_usage()
