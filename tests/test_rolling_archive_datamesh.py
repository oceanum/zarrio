"""Integration tests for DatameshRollingArchiveBackend with mocked ZarrClient."""

import pytest
from datetime import datetime, timedelta
from unittest.mock import Mock, patch, MagicMock
import sys
from pathlib import Path

# Ensure project root is on PYTHONPATH
ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from zarrio.rolling_archive import DatameshRollingArchiveBackend


class MockZarrClient:
    """Mock ZarrClient implementing MutableMapping interface for testing.

    Simulates the behavior of ZarrClient which iterates over keys including
    .zgroup and .zattrs markers that indicate Zarr group structure.
    """

    def __init__(
        self, groups_with_data=None, delitem_side_effect=None, iter_side_effect=None
    ):
        self._groups = groups_with_data or {}
        self.deleted = []
        self.accessed = []
        self._delitem_side_effect = delitem_side_effect
        self._iter_side_effect = iter_side_effect

    def __iter__(self):
        if self._iter_side_effect:
            raise self._iter_side_effect
        for group_name in self._groups.keys():
            yield f"{group_name}/.zgroup"

    def __delitem__(self, key):
        if self._delitem_side_effect:
            if callable(self._delitem_side_effect):
                self._delitem_side_effect(key)
            else:
                raise self._delitem_side_effect
        if key not in self._groups:
            raise KeyError(f"Group not found: {key}")
        del self._groups[key]
        self.deleted.append(key)

    def __contains__(self, key):
        return key in self._groups

    def __getitem__(self, key):
        self.accessed.append(key)
        if key not in self._groups:
            raise KeyError(f"Group not found: {key}")
        return self._groups[key]

    def keys(self):
        return self._groups.keys()


class TestDatameshEndToEnd:
    """End-to-end workflow tests with mocked ZarrClient."""

    @pytest.fixture
    def forecast_groups(self):
        """Create mock forecast cycle groups with realistic naming."""
        return {
            "cycle/20240101T000000": {"cycle_time": "2024-01-01T00:00:00"},
            "cycle/20240101T060000": {"cycle_time": "2024-01-01T06:00:00"},
            "cycle/20240101T120000": {"cycle_time": "2024-01-01T12:00:00"},
            "cycle/20240101T180000": {"cycle_time": "2024-01-01T18:00:00"},
            "cycle/20240102T000000": {"cycle_time": "2024-01-02T00:00:00"},
        }

    @pytest.fixture
    def mixed_groups(self):
        """Create mixed group types (forecast cycles + metadata)."""
        return {
            "cycle/20240101T000000": {"cycle_time": "2024-01-01T00:00:00"},
            "cycle/20240101T120000": {"cycle_time": "2024-01-01T12:00:00"},
            "metadata/v1": {"version": "1.0"},
            "config/settings": {"config": "data"},
        }

    def test_enumerate_all_groups(self, forecast_groups):
        """Test enumerating all groups in datasource."""
        client = MockZarrClient(forecast_groups)
        backend = DatameshRollingArchiveBackend(client)

        groups = backend.enumerate_groups()

        assert len(groups) == 5
        assert "cycle/20240101T000000" in groups
        assert "cycle/20240101T180000" in groups
        assert "cycle/20240102T000000" in groups

    def test_enumerate_empty_datasource(self):
        """Test enumerating groups from empty datasource."""
        client = MockZarrClient({})
        backend = DatameshRollingArchiveBackend(client)

        groups = backend.enumerate_groups()

        assert groups == []

    def test_delete_expired_groups(self, forecast_groups):
        """Test deleting expired forecast cycles."""
        client = MockZarrClient(forecast_groups)
        backend = DatameshRollingArchiveBackend(client)

        # Delete first two cycles
        to_delete = ["cycle/20240101T000000", "cycle/20240101T060000"]
        result = backend.delete_groups(to_delete)

        assert len(result["deleted"]) == 2
        assert "cycle/20240101T000000" in result["deleted"]
        assert "cycle/20240101T060000" in result["deleted"]
        assert len(result["failed"]) == 0

        # Verify actually deleted
        assert "cycle/20240101T000000" in client.deleted
        assert "cycle/20240101T060000" in client.deleted
        assert "cycle/20240101T000000" not in client
        assert "cycle/20240101T060000" not in client

        # Verify remaining groups still exist
        assert "cycle/20240101T120000" in client
        assert "cycle/20240102T000000" in client

    def test_delete_all_groups(self, forecast_groups):
        """Test deleting all groups."""
        client = MockZarrClient(forecast_groups)
        backend = DatameshRollingArchiveBackend(client)

        all_groups = list(forecast_groups.keys())
        result = backend.delete_groups(all_groups)

        assert len(result["deleted"]) == 5
        assert len(result["failed"]) == 0
        assert len(client.deleted) == 5

        # Verify all deleted
        for group in all_groups:
            assert group not in client

    def test_dry_run_no_deletion(self, forecast_groups):
        """Test dry run doesn't actually delete groups."""
        client = MockZarrClient(forecast_groups)
        backend = DatameshRollingArchiveBackend(client)

        result = backend.delete_groups(
            ["cycle/20240101T000000", "cycle/20240101T060000"], dry_run=True
        )

        # Should report as deleted
        assert len(result["deleted"]) == 2
        assert "cycle/20240101T000000" in result["deleted"]

        # But not actually deleted
        assert len(client.deleted) == 0
        assert "cycle/20240101T000000" in client
        assert "cycle/20240101T060000" in client

    def test_dry_run_all_groups(self, forecast_groups):
        """Test dry run with all groups."""
        client = MockZarrClient(forecast_groups)
        backend = DatameshRollingArchiveBackend(client)

        all_groups = list(forecast_groups.keys())
        result = backend.delete_groups(all_groups, dry_run=True)

        assert len(result["deleted"]) == 5
        assert len(client.deleted) == 0
        assert len(list(client)) == 5

    def test_handle_network_error(self):
        """Test that network errors during deletion propagate as expected."""
        forecast_groups = {
            "cycle/20240101T000000": {"cycle_time": "2024-01-01T00:00:00"},
        }
        client = MockZarrClient(
            forecast_groups, delitem_side_effect=ConnectionError("Network failure")
        )
        backend = DatameshRollingArchiveBackend(client)

        with pytest.raises(ConnectionError, match="Network failure"):
            backend.delete_groups(["cycle/20240101T000000"])

    def test_handle_partial_network_failure(self):
        """Test that first error during batch deletion stops the operation."""
        forecast_groups = {
            "cycle/20240101T000000": {"cycle_time": "2024-01-01T00:00:00"},
            "cycle/20240101T060000": {"cycle_time": "2024-01-01T06:00:00"},
            "cycle/20240101T120000": {"cycle_time": "2024-01-01T12:00:00"},
        }
        client = MockZarrClient(forecast_groups)

        fail_group = "cycle/20240101T060000"

        class SelectiveFailure:
            def __init__(self, fail_key):
                self.fail_key = fail_key
                self.call_count = 0

            def __call__(self, key):
                self.call_count += 1
                if key == self.fail_key:
                    raise ConnectionError("Network failure")

        selective_error = SelectiveFailure(fail_group)
        client._delitem_side_effect = selective_error

        backend = DatameshRollingArchiveBackend(client)

        to_delete = [
            "cycle/20240101T000000",
            "cycle/20240101T060000",
            "cycle/20240101T120000",
        ]

        with pytest.raises(ConnectionError, match="Network failure"):
            backend.delete_groups(to_delete)

        assert selective_error.call_count == 2
        assert len(client.deleted) == 1
        assert "cycle/20240101T000000" in client.deleted

    def test_handle_auth_error(self):
        """Test that authentication errors during deletion propagate."""
        forecast_groups = {
            "cycle/20240101T000000": {"cycle_time": "2024-01-01T00:00:00"},
        }
        client = MockZarrClient(
            forecast_groups,
            delitem_side_effect=PermissionError("Authentication failed"),
        )
        backend = DatameshRollingArchiveBackend(client)

        with pytest.raises(PermissionError, match="Authentication failed"):
            backend.delete_groups(["cycle/20240101T000000"])

    def test_delete_nonexistent_group(self, forecast_groups):
        """Test deleting groups that don't exist."""
        client = MockZarrClient(forecast_groups)
        backend = DatameshRollingArchiveBackend(client)

        result = backend.delete_groups(["cycle/20240103T000000"])

        assert len(result["deleted"]) == 0
        assert len(result["failed"]) == 1
        assert "cycle/20240103T000000" in result["failed"]

    def test_delete_mixed_existing_nonexistent(self, forecast_groups):
        """Test deleting mix of existing and non-existing groups."""
        client = MockZarrClient(forecast_groups)
        backend = DatameshRollingArchiveBackend(client)

        to_delete = [
            "cycle/20240101T000000",  # exists
            "cycle/20240103T000000",  # doesn't exist
            "cycle/20240101T120000",  # exists
        ]
        result = backend.delete_groups(to_delete)

        assert len(result["deleted"]) == 2
        assert len(result["failed"]) == 1
        assert "cycle/20240101T000000" in result["deleted"]
        assert "cycle/20240101T120000" in result["deleted"]
        assert "cycle/20240103T000000" in result["failed"]

    def test_backend_type(self):
        """Test backend_type property returns 'datamesh'."""
        client = MockZarrClient({})
        backend = DatameshRollingArchiveBackend(client)

        assert backend.backend_type == "datamesh"

    def test_realistic_forecast_workflow(self):
        """Test realistic forecast cycle management workflow."""
        # Simulate 7 days of 6-hourly forecasts
        groups = {}
        base_date = datetime(2024, 1, 1)
        for day in range(7):
            for hour in [0, 6, 12, 18]:
                dt = base_date + timedelta(days=day, hours=hour)
                group_name = f"cycle/{dt.strftime('%Y%m%dT%H%M%S')}"
                groups[group_name] = {"cycle_time": dt.isoformat()}

        client = MockZarrClient(groups)
        backend = DatameshRollingArchiveBackend(client)

        # Enumerate all cycles
        all_groups = backend.enumerate_groups()
        assert len(all_groups) == 28  # 7 days * 4 cycles/day

        # Delete first 2 days (8 cycles)
        cutoff = base_date + timedelta(days=2)
        to_delete = [g for g in all_groups if backend.get_group_timestamp(g) < cutoff]
        assert len(to_delete) == 8

        result = backend.delete_groups(to_delete)
        assert len(result["deleted"]) == 8
        assert len(result["failed"]) == 0

        # Verify remaining cycles
        remaining = backend.enumerate_groups()
        assert len(remaining) == 20


class TestDatameshTimestampExtraction:
    """Test timestamp extraction from group names and attributes."""

    def test_extract_from_compact_format(self):
        """Test extracting timestamp from compact format (20240101T120000)."""
        client = MockZarrClient({"cycle/20240101T120000": {}})
        backend = DatameshRollingArchiveBackend(client)

        timestamp = backend.get_group_timestamp("cycle/20240101T120000")

        assert timestamp == datetime(2024, 1, 1, 12, 0, 0)

    def test_extract_from_iso_format(self):
        """Test extracting timestamp from ISO format (2024-01-01T12:00:00)."""
        client = MockZarrClient({"cycle/2024-01-01T12:00:00": {}})
        backend = DatameshRollingArchiveBackend(client)

        timestamp = backend.get_group_timestamp("cycle/2024-01-01T12:00:00")

        assert timestamp == datetime(2024, 1, 1, 12, 0, 0)

    def test_extract_from_nested_path(self):
        """Test extracting timestamp from nested group path."""
        client = MockZarrClient({"forecasts/wave/cycle/20240101T060000": {}})
        backend = DatameshRollingArchiveBackend(client)

        timestamp = backend.get_group_timestamp("forecasts/wave/cycle/20240101T060000")

        assert timestamp == datetime(2024, 1, 1, 6, 0, 0)

    def test_extract_from_unparseable_name(self):
        """Test handling unparseable group names."""
        client = MockZarrClient({"metadata/v1": {}})
        backend = DatameshRollingArchiveBackend(client)

        timestamp = backend.get_group_timestamp("metadata/v1")

        assert timestamp is None

    def test_extract_from_multiple_formats(self):
        """Test extracting timestamps from various group name formats."""
        test_cases = [
            ("cycle/20240101T000000", datetime(2024, 1, 1, 0, 0, 0)),
            ("cycle/20240115T183045", datetime(2024, 1, 15, 18, 30, 45)),
            ("cycle/2024-03-01T12:00:00", datetime(2024, 3, 1, 12, 0, 0)),
            ("forecast/20240201T120000", datetime(2024, 2, 1, 12, 0, 0)),
        ]

        for group_name, expected_dt in test_cases:
            client = MockZarrClient({group_name: {}})
            backend = DatameshRollingArchiveBackend(client)

            timestamp = backend.get_group_timestamp(group_name)

            assert timestamp == expected_dt, (
                f"Failed for {group_name}: got {timestamp}, expected {expected_dt}"
            )

    def test_extract_handles_none_timestamp(self):
        """Test graceful handling when timestamp extraction returns None."""
        client = MockZarrClient({"invalid/group/name": {}})
        backend = DatameshRollingArchiveBackend(client)

        timestamp = backend.get_group_timestamp("invalid/group/name")

        assert timestamp is None

    def test_custom_time_reference_attr(self):
        """Test get_group_timestamp with custom time_reference_attr parameter."""
        client = MockZarrClient({"cycle/unparseable": {"init_time": "2024-01-01"}})
        backend = DatameshRollingArchiveBackend(client)

        # Should still try name parsing first (which returns None)
        # Then try attribute lookup (not implemented yet, returns None)
        timestamp = backend.get_group_timestamp("cycle/unparseable", "init_time")

        # Currently returns None since attribute reading not implemented
        assert timestamp is None


class TestDatameshGroupEnumeration:
    """Test group enumeration edge cases."""

    def test_enumerate_preserves_order(self):
        """Test that group enumeration preserves insertion order."""
        groups = {
            "cycle/20240101T000000": {},
            "cycle/20240101T060000": {},
            "cycle/20240101T120000": {},
        }
        client = MockZarrClient(groups)
        backend = DatameshRollingArchiveBackend(client)

        enumerated = backend.enumerate_groups()

        # Python 3.7+ dicts maintain insertion order
        assert enumerated == list(groups.keys())

    def test_enumerate_with_special_characters(self):
        """Test enumeration with special characters in group names."""
        groups = {
            "cycle/test-forecast_v2": {},
            "cycle/prod.backup": {},
            "cycle/20240101T000000+metadata": {},
        }
        client = MockZarrClient(groups)
        backend = DatameshRollingArchiveBackend(client)

        enumerated = backend.enumerate_groups()

        assert len(enumerated) == 3
        assert all(g in enumerated for g in groups.keys())

    def test_enumerate_deeply_nested(self):
        """Test enumeration with deeply nested group paths."""
        groups = {
            "ocean/wave/model/gfs/cycle/20240101T000000": {},
            "atmosphere/forecast/ecmwf/cycle/20240101T000000": {},
        }
        client = MockZarrClient(groups)
        backend = DatameshRollingArchiveBackend(client)

        enumerated = backend.enumerate_groups()

        assert len(enumerated) == 2
        assert "ocean/wave/model/gfs/cycle/20240101T000000" in enumerated


class TestDatameshErrorHandling:
    """Test error handling scenarios."""

    def test_handle_client_iteration_error(self):
        """Test that errors during client iteration propagate."""
        client = MockZarrClient(
            {"cycle/20240101T000000": {}},
            iter_side_effect=RuntimeError("Client connection lost"),
        )
        backend = DatameshRollingArchiveBackend(client)

        with pytest.raises(RuntimeError, match="Client connection lost"):
            backend.enumerate_groups()

    def test_handle_deletion_keyerror(self):
        """Test handling KeyError during deletion."""
        client = MockZarrClient({})
        backend = DatameshRollingArchiveBackend(client)

        result = backend.delete_groups(["nonexistent/group"])

        assert len(result["failed"]) == 1
        assert "nonexistent/group" in result["failed"]

    def test_handle_generic_exception_during_deletion(self):
        """Test that unexpected exceptions during deletion propagate."""
        client = MockZarrClient(
            {"cycle/20240101T000000": {}},
            delitem_side_effect=ValueError("Unexpected internal error"),
        )
        backend = DatameshRollingArchiveBackend(client)

        with pytest.raises(ValueError, match="Unexpected internal error"):
            backend.delete_groups(["cycle/20240101T000000"])

    def test_empty_group_list_deletion(self):
        """Test deletion with empty group list."""
        client = MockZarrClient({"cycle/20240101T000000": {}})
        backend = DatameshRollingArchiveBackend(client)

        result = backend.delete_groups([])

        assert len(result["deleted"]) == 0
        assert len(result["failed"]) == 0
        # Original groups still exist
        assert len(backend.enumerate_groups()) == 1


class TestDatameshPagination:
    """Test pagination scenarios (if applicable to future implementation)."""

    def test_enumerate_large_number_of_groups(self):
        """Test enumerating large number of groups (simulating pagination)."""
        # Create 1000 groups
        groups = {f"cycle/group_{i:05d}": {} for i in range(1000)}
        client = MockZarrClient(groups)
        backend = DatameshRollingArchiveBackend(client)

        enumerated = backend.enumerate_groups()

        assert len(enumerated) == 1000
        assert "cycle/group_00000" in enumerated
        assert "cycle/group_00999" in enumerated

    def test_batch_deletion_large_group_set(self):
        """Test deleting large batch of groups."""
        groups = {f"cycle/group_{i:05d}": {} for i in range(100)}
        client = MockZarrClient(groups)
        backend = DatameshRollingArchiveBackend(client)

        # Delete first 50
        to_delete = [f"cycle/group_{i:05d}" for i in range(50)]
        result = backend.delete_groups(to_delete)

        assert len(result["deleted"]) == 50
        assert len(result["failed"]) == 0

        # Verify remaining
        remaining = backend.enumerate_groups()
        assert len(remaining) == 50
