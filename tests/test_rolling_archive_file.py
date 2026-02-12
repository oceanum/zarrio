"""
Integration tests for FileRollingArchiveBackend using real Zarr stores.

These tests create real Zarr stores on the filesystem and test actual
cleanup operations, including:
- Realistic forecast cycle scenarios
- Multiple groups with timestamps
- Cleanup with different retention windows
- Dry run verification
- End-to-end ZarrConverter integration
"""

import pytest
from datetime import datetime, timedelta
from pathlib import Path
import tempfile
import zarr
import xarray as xr
import numpy as np
import pandas as pd

from zarrio.rolling_archive import FileRollingArchiveBackend


class TestFileBackendIntegration:
    """Integration tests with real Zarr stores."""

    @pytest.fixture
    def forecast_zarr_store(self):
        """Create a Zarr store with forecast cycles."""
        with tempfile.TemporaryDirectory() as tmpdir:
            store = zarr.open_group(tmpdir, mode="w")

            # Create forecast cycle groups with timestamps
            cycles = [
                ("20240101T000000", "2024-01-01T00:00:00"),
                ("20240101T060000", "2024-01-01T06:00:00"),
                ("20240101T120000", "2024-01-01T12:00:00"),
                ("20240101T180000", "2024-01-01T18:00:00"),
            ]

            for group_name, timestamp in cycles:
                group = store.create_group(f"cycle/{group_name}")
                group.attrs["cycle_time"] = timestamp
                # Add some dummy data to make it realistic
                group.create_dataset("temperature", data=np.random.rand(10, 10))

            yield tmpdir

    @pytest.fixture
    def multi_level_zarr_store(self):
        """Create a Zarr store with multiple nested levels."""
        with tempfile.TemporaryDirectory() as tmpdir:
            store = zarr.open_group(tmpdir, mode="w")

            # Create nested structure: data/forecast/cycle/timestamp
            cycles = [
                ("20240101T000000", "2024-01-01T00:00:00"),
                ("20240101T060000", "2024-01-01T06:00:00"),
            ]

            for group_name, timestamp in cycles:
                group = store.create_group(f"data/forecast/cycle/{group_name}")
                group.attrs["cycle_time"] = timestamp

            yield tmpdir

    def test_enumerate_groups_real_store(self, forecast_zarr_store):
        """Test enumerating groups from real Zarr store."""
        backend = FileRollingArchiveBackend(forecast_zarr_store)

        groups = backend.enumerate_groups()

        assert len(groups) >= 4  # At least the 4 cycle groups
        cycle_groups = [g for g in groups if g.startswith("cycle/")]
        assert len(cycle_groups) == 4
        assert any("20240101T000000" in g for g in groups)
        assert any("20240101T180000" in g for g in groups)

    def test_enumerate_groups_nested_structure(self, multi_level_zarr_store):
        """Test enumerating groups with nested structure."""
        backend = FileRollingArchiveBackend(multi_level_zarr_store)

        groups = backend.enumerate_groups()

        # Should include all levels of nesting
        assert any("data" in g for g in groups)
        assert any("data/forecast" in g for g in groups)
        assert any("data/forecast/cycle" in g for g in groups)
        assert any("data/forecast/cycle/20240101T000000" in g for g in groups)

    def test_get_timestamp_from_name(self, forecast_zarr_store):
        """Test extracting timestamp from group name."""
        backend = FileRollingArchiveBackend(forecast_zarr_store)

        timestamp = backend.get_group_timestamp("cycle/20240101T120000")

        assert timestamp == datetime(2024, 1, 1, 12, 0, 0)

    def test_get_timestamp_from_attrs(self, forecast_zarr_store):
        """Test extracting timestamp from group attributes."""
        backend = FileRollingArchiveBackend(forecast_zarr_store)

        # Create group with non-standard name
        store = zarr.open_group(forecast_zarr_store, mode="a")
        group = store.create_group("cycle/special_forecast")
        group.attrs["cycle_time"] = "20240102T000000"

        timestamp = backend.get_group_timestamp("cycle/special_forecast")

        assert timestamp == datetime(2024, 1, 2, 0, 0, 0)

    def test_delete_groups_real_store(self, forecast_zarr_store):
        """Test actually deleting groups from store."""
        backend = FileRollingArchiveBackend(forecast_zarr_store)

        store = zarr.open_group(forecast_zarr_store, mode="r")
        assert "cycle/20240101T000000" in store

        result = backend.delete_groups(["cycle/20240101T000000"])

        assert "cycle/20240101T000000" in result["deleted"]
        assert len(result["failed"]) == 0

        store = zarr.open_group(forecast_zarr_store, mode="r")
        assert "cycle/20240101T000000" not in store
        assert "cycle/20240101T060000" in store
        assert "cycle/20240101T120000" in store

    def test_delete_multiple_groups(self, forecast_zarr_store):
        """Test deleting multiple groups at once."""
        backend = FileRollingArchiveBackend(forecast_zarr_store)

        groups_to_delete = [
            "cycle/20240101T000000",
            "cycle/20240101T060000",
        ]
        result = backend.delete_groups(groups_to_delete)

        assert len(result["deleted"]) == 2
        assert "cycle/20240101T000000" in result["deleted"]
        assert "cycle/20240101T060000" in result["deleted"]

        store = zarr.open_group(forecast_zarr_store, mode="r")
        assert "cycle/20240101T000000" not in store
        assert "cycle/20240101T060000" not in store
        assert "cycle/20240101T120000" in store

    def test_dry_run_no_actual_deletion(self, forecast_zarr_store):
        """Test dry run doesn't actually delete."""
        backend = FileRollingArchiveBackend(forecast_zarr_store)

        groups_before = backend.enumerate_groups()

        result = backend.delete_groups(["cycle/20240101T000000"], dry_run=True)

        assert "cycle/20240101T000000" in result["deleted"]

        groups_after = backend.enumerate_groups()
        assert len(groups_after) == len(groups_before)

        store = zarr.open_group(forecast_zarr_store, mode="r")
        assert "cycle/20240101T000000" in store

    def test_dry_run_multiple_groups(self, forecast_zarr_store):
        """Test dry run with multiple groups."""
        backend = FileRollingArchiveBackend(forecast_zarr_store)

        groups_to_delete = [
            "cycle/20240101T000000",
            "cycle/20240101T060000",
            "cycle/20240101T120000",
        ]

        result = backend.delete_groups(groups_to_delete, dry_run=True)

        assert len(result["deleted"]) == 3
        for group in groups_to_delete:
            assert group in result["deleted"]

        store = zarr.open_group(forecast_zarr_store, mode="r")
        for group in groups_to_delete:
            assert group in store

    def test_delete_nonexistent_group(self, forecast_zarr_store):
        """Test deleting a group that doesn't exist."""
        backend = FileRollingArchiveBackend(forecast_zarr_store)

        result = backend.delete_groups(["cycle/nonexistent"])

        assert "cycle/nonexistent" in result["failed"]
        assert len(result["deleted"]) == 0

    def test_partial_deletion_failure(self, forecast_zarr_store):
        """Test deletion with some successes and some failures."""
        backend = FileRollingArchiveBackend(forecast_zarr_store)

        groups_to_delete = [
            "cycle/20240101T000000",
            "cycle/nonexistent",
            "cycle/20240101T060000",
        ]

        result = backend.delete_groups(groups_to_delete)

        assert "cycle/20240101T000000" in result["deleted"]
        assert "cycle/20240101T060000" in result["deleted"]
        assert "cycle/nonexistent" in result["failed"]

        store = zarr.open_group(forecast_zarr_store, mode="r")
        assert "cycle/20240101T000000" not in store
        assert "cycle/20240101T060000" not in store
        assert "cycle/20240101T120000" in store


class TestRealisticForecastScenarios:
    """Test realistic forecast cycle scenarios."""

    @pytest.fixture
    def operational_forecast_store(self):
        """Create a realistic operational forecast archive."""
        with tempfile.TemporaryDirectory() as tmpdir:
            store = zarr.open_group(tmpdir, mode="w")

            base_time = datetime(2024, 1, 1, 0, 0, 0)
            for day in range(7):
                for hour in [0, 6, 12, 18]:
                    cycle_time = base_time + timedelta(days=day, hours=hour)
                    group_name = cycle_time.strftime("%Y%m%dT%H%M%S")
                    group = store.create_group(f"cycle/{group_name}")
                    group.attrs["cycle_time"] = cycle_time.isoformat()
                    group.create_dataset("temperature", data=np.random.rand(24, 10, 10))
                    group.create_dataset("pressure", data=np.random.rand(24, 10, 10))

            yield tmpdir

    def test_enumerate_all_forecast_cycles(self, operational_forecast_store):
        """Test enumerating all cycles in operational archive."""
        backend = FileRollingArchiveBackend(operational_forecast_store)

        groups = backend.enumerate_groups()

        cycle_groups = [g for g in groups if g.startswith("cycle/")]
        assert len(cycle_groups) == 28

    def test_filter_old_forecasts(self, operational_forecast_store):
        """Test filtering forecasts older than retention window."""
        backend = FileRollingArchiveBackend(operational_forecast_store)

        all_groups = backend.enumerate_groups()
        cycle_groups = [g for g in all_groups if g.startswith("cycle/")]

        cutoff = datetime(2024, 1, 3, 0, 0, 0)
        old_groups = []

        for group in cycle_groups:
            timestamp = backend.get_group_timestamp(group)
            if timestamp and timestamp < cutoff:
                old_groups.append(group)

        assert len(old_groups) == 8

        result = backend.delete_groups(old_groups)
        assert len(result["deleted"]) == 8

        remaining = backend.enumerate_groups()
        remaining_cycles = [g for g in remaining if g.startswith("cycle/")]
        assert len(remaining_cycles) == 20

    def test_keep_minimum_groups(self, operational_forecast_store):
        """Test keeping minimum number of groups regardless of age."""
        backend = FileRollingArchiveBackend(operational_forecast_store)

        all_groups = backend.enumerate_groups()
        cycle_groups = sorted([g for g in all_groups if g.startswith("cycle/")])

        min_groups_to_keep = 5
        groups_to_delete = cycle_groups[:-min_groups_to_keep]

        result = backend.delete_groups(groups_to_delete)
        assert len(result["deleted"]) == len(groups_to_delete)

        remaining = backend.enumerate_groups()
        remaining_cycles = [g for g in remaining if g.startswith("cycle/")]
        assert len(remaining_cycles) == min_groups_to_keep


class TestZarrConverterIntegration:
    """Test integration with ZarrConverter."""

    @pytest.fixture
    def forecast_zarr_store(self):
        """Create a Zarr store with forecast cycles for integration tests."""
        with tempfile.TemporaryDirectory() as tmpdir:
            store = zarr.open_group(tmpdir, mode="w")

            cycles = [
                ("20240101T000000", "2024-01-01T00:00:00"),
                ("20240101T060000", "2024-01-01T06:00:00"),
                ("20240101T120000", "2024-01-01T12:00:00"),
                ("20240101T180000", "2024-01-01T18:00:00"),
            ]

            for group_name, timestamp in cycles:
                group = store.create_group(f"cycle/{group_name}")
                group.attrs["cycle_time"] = timestamp
                group.create_dataset("temperature", data=np.random.rand(10, 10))

            yield tmpdir

    @pytest.fixture
    def operational_forecast_store(self):
        """Create a realistic operational forecast archive."""
        with tempfile.TemporaryDirectory() as tmpdir:
            store = zarr.open_group(tmpdir, mode="w")

            base_time = datetime(2024, 1, 1, 0, 0, 0)
            for day in range(7):
                for hour in [0, 6, 12, 18]:
                    cycle_time = base_time + timedelta(days=day, hours=hour)
                    group_name = cycle_time.strftime("%Y%m%dT%H%M%S")
                    group = store.create_group(f"cycle/{group_name}")
                    group.attrs["cycle_time"] = cycle_time.isoformat()
                    group.create_dataset("temperature", data=np.random.rand(24, 10, 10))
                    group.create_dataset("pressure", data=np.random.rand(24, 10, 10))

            yield tmpdir

    @pytest.fixture
    def sample_netcdf_dataset(self):
        """Create a sample xarray dataset for testing."""
        time = pd.date_range("2024-01-01", periods=24, freq="h")
        lat = np.linspace(-90, 90, 10)
        lon = np.linspace(-180, 180, 20)

        temp = np.random.rand(24, 10, 20)

        ds = xr.Dataset(
            {"temperature": (["time", "lat", "lon"], temp)},
            coords={"time": time, "lat": lat, "lon": lon},
        )
        return ds

    @pytest.fixture
    def populated_zarr_store(self, sample_netcdf_dataset):
        """Create Zarr store with old and new cycles."""
        with tempfile.TemporaryDirectory() as tmpdir:
            store = zarr.open_group(tmpdir, mode="w")

            old_time = datetime.now() - timedelta(hours=48)
            old_group = store.create_group("cycle/old_forecast")
            old_group.attrs["cycle_time"] = old_time.isoformat()
            old_group.create_dataset("temperature", data=np.random.rand(24, 10, 20))

            recent_time = datetime.now() - timedelta(hours=6)
            recent_group = store.create_group("cycle/recent_forecast")
            recent_group.attrs["cycle_time"] = recent_time.isoformat()
            recent_group.create_dataset("temperature", data=np.random.rand(24, 10, 20))

            yield tmpdir

    def test_backend_with_zarr_path(self, forecast_zarr_store):
        """Test creating backend from file path."""
        backend = FileRollingArchiveBackend(forecast_zarr_store)
        assert backend.zarr_path == Path(forecast_zarr_store)

        backend2 = FileRollingArchiveBackend(Path(forecast_zarr_store))
        assert backend2.zarr_path == Path(forecast_zarr_store)

    def test_cleanup_with_converter_config(self, populated_zarr_store):
        """Test cleanup using ZarrConverter with rolling archive config."""
        backend = FileRollingArchiveBackend(populated_zarr_store)

        all_groups = backend.enumerate_groups()
        cycle_groups = [g for g in all_groups if "cycle/" in g]

        cutoff = datetime.now() - timedelta(hours=24)
        expired_groups = []

        for group in cycle_groups:
            timestamp = backend.get_group_timestamp(group)
            if timestamp and timestamp < cutoff:
                expired_groups.append(group)

        assert len(expired_groups) > 0

        result = backend.delete_groups(expired_groups)
        assert len(result["deleted"]) > 0

    def test_dry_run_with_config(self, populated_zarr_store):
        """Test dry run cleanup with configuration."""
        backend = FileRollingArchiveBackend(populated_zarr_store)

        groups_before = set(backend.enumerate_groups())

        all_cycle_groups = [g for g in groups_before if "cycle/" in g]
        result = backend.delete_groups(all_cycle_groups, dry_run=True)

        assert len(result["deleted"]) == len(all_cycle_groups)

        groups_after = set(backend.enumerate_groups())
        assert groups_before == groups_after

    def test_incremental_cleanup(self, operational_forecast_store):
        """Test incremental cleanup over multiple runs."""
        backend = FileRollingArchiveBackend(operational_forecast_store)

        initial_count = len(
            [g for g in backend.enumerate_groups() if g.startswith("cycle/")]
        )
        assert initial_count == 28

        all_groups = sorted(
            [g for g in backend.enumerate_groups() if g.startswith("cycle/")]
        )
        result1 = backend.delete_groups(all_groups[:10])
        assert len(result1["deleted"]) == 10

        remaining_count = len(
            [g for g in backend.enumerate_groups() if g.startswith("cycle/")]
        )
        assert remaining_count == 18

        all_groups = sorted(
            [g for g in backend.enumerate_groups() if g.startswith("cycle/")]
        )
        result2 = backend.delete_groups(all_groups[:5])
        assert len(result2["deleted"]) == 5

        final_count = len(
            [g for g in backend.enumerate_groups() if g.startswith("cycle/")]
        )
        assert final_count == 13


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
