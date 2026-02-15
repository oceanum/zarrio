"""
Comprehensive unit tests for rolling archive functionality.

Tests all components with mocked dependencies:
- Config validation (RollingArchiveConfig)
- Time parsing utilities
- Backend interface compliance
- Datamesh backend (mocked ZarrClient)
- File backend (mocked zarr)
- Core integration
"""

import pytest
from datetime import datetime, timedelta
from unittest.mock import Mock, MagicMock
import tempfile
from pathlib import Path
import zarr

from zarrio.models import RollingArchiveConfig, ZarrConverterConfig
from zarrio.time_parsing import (
    parse_timestamp_from_string,
    extract_timestamp_from_group_name,
    DEFAULT_TIMESTAMP_FORMATS,
)
from zarrio.rolling_archive import (
    RollingArchiveBackend,
    DatameshRollingArchiveBackend,
    FileRollingArchiveBackend,
)


# =============================================================================
# Config Validation Tests
# =============================================================================


class TestRollingArchiveConfig:
    """Test RollingArchiveConfig validation and defaults."""

    def test_default_config_disabled(self):
        """Test that config defaults to disabled."""
        config = RollingArchiveConfig()
        assert config.enabled is False
        assert config.retention_window is None

    def test_enabled_config_basic(self):
        """Test enabled config with retention window."""
        config = RollingArchiveConfig(
            enabled=True, retention_window=timedelta(hours=24)
        )
        assert config.enabled is True
        assert config.retention_window == timedelta(hours=24)

    def test_retention_window_validation_minimum(self):
        """Test retention window must be at least 1 hour."""
        with pytest.raises(
            ValueError, match="retention_window must be at least 1 hour"
        ):
            RollingArchiveConfig(enabled=True, retention_window=timedelta(minutes=30))

    def test_retention_window_validation_exactly_one_hour(self):
        """Test retention window accepts exactly 1 hour."""
        config = RollingArchiveConfig(enabled=True, retention_window=timedelta(hours=1))
        assert config.retention_window == timedelta(hours=1)

    def test_retention_window_validation_large_values(self):
        """Test retention window accepts large values."""
        config = RollingArchiveConfig(
            enabled=True, retention_window=timedelta(days=365)
        )
        assert config.retention_window == timedelta(days=365)

    def test_min_groups_to_keep_validation_zero(self):
        """Test min_groups_to_keep accepts 0."""
        config = RollingArchiveConfig(min_groups_to_keep=0)
        assert config.min_groups_to_keep == 0

    def test_min_groups_to_keep_validation_positive(self):
        """Test min_groups_to_keep accepts positive values."""
        config = RollingArchiveConfig(min_groups_to_keep=5)
        assert config.min_groups_to_keep == 5

    def test_min_groups_to_keep_default(self):
        """Test min_groups_to_keep defaults to 1."""
        config = RollingArchiveConfig()
        assert config.min_groups_to_keep == 1

    def test_time_reference_attr_default(self):
        """Test time_reference_attr defaults to 'cycle_time'."""
        config = RollingArchiveConfig()
        assert config.time_reference_attr == "cycle_time"

    def test_time_reference_attr_custom(self):
        """Test custom time_reference_attr."""
        config = RollingArchiveConfig(time_reference_attr="timestamp")
        assert config.time_reference_attr == "timestamp"

    def test_auto_cleanup_default(self):
        """Test auto_cleanup defaults to True."""
        config = RollingArchiveConfig()
        assert config.auto_cleanup is True

    def test_auto_cleanup_disabled(self):
        """Test disabling auto_cleanup."""
        config = RollingArchiveConfig(auto_cleanup=False)
        assert config.auto_cleanup is False

    def test_full_config(self):
        """Test complete configuration with all options."""
        config = RollingArchiveConfig(
            enabled=True,
            retention_window=timedelta(hours=48),
            time_reference_attr="forecast_time",
            auto_cleanup=False,
            min_groups_to_keep=3,
        )
        assert config.enabled is True
        assert config.retention_window == timedelta(hours=48)
        assert config.time_reference_attr == "forecast_time"
        assert config.auto_cleanup is False
        assert config.min_groups_to_keep == 3


# =============================================================================
# Time Parsing Tests
# =============================================================================


class TestTimeParsing:
    """Test time parsing utilities."""

    def test_parse_compact_iso8601(self):
        """Test parsing 20240101T000000 format."""
        result = parse_timestamp_from_string("20240101T000000")
        assert result == datetime(2024, 1, 1, 0, 0, 0)

    def test_parse_compact_iso8601_noon(self):
        """Test parsing 20240101T120000 format."""
        result = parse_timestamp_from_string("20240101T120000")
        assert result == datetime(2024, 1, 1, 12, 0, 0)

    def test_parse_standard_iso8601(self):
        """Test parsing 2024-01-01T00:00:00 format."""
        result = parse_timestamp_from_string("2024-01-01T00:00:00")
        assert result == datetime(2024, 1, 1, 0, 0, 0)

    def test_parse_standard_iso8601_with_time(self):
        """Test parsing 2024-01-01T15:30:45 format."""
        result = parse_timestamp_from_string("2024-01-01T15:30:45")
        assert result == datetime(2024, 1, 1, 15, 30, 45)

    def test_parse_compact_no_separator(self):
        """Test parsing 20240101000000 format."""
        result = parse_timestamp_from_string("20240101000000")
        assert result == datetime(2024, 1, 1, 0, 0, 0)

    def test_parse_unparseable_returns_none(self):
        """Test unparseable strings return None."""
        result = parse_timestamp_from_string("invalid")
        assert result is None

    def test_parse_empty_string(self):
        """Test empty string returns None."""
        result = parse_timestamp_from_string("")
        assert result is None

    def test_parse_partial_date(self):
        """Test partial date string returns None or parsed via dateutil."""
        result = parse_timestamp_from_string("2024-01-01")
        # dateutil fallback might parse this
        if result is not None:
            assert result.year == 2024
            assert result.month == 1
            assert result.day == 1

    def test_parse_custom_formats(self):
        """Test parsing with custom format list."""
        formats = ["%Y/%m/%d %H:%M:%S"]
        result = parse_timestamp_from_string("2024/01/01 12:00:00", formats=formats)
        assert result == datetime(2024, 1, 1, 12, 0, 0)

    def test_extract_from_group_name_single_segment(self):
        """Test extracting timestamp from single segment group name."""
        result = extract_timestamp_from_group_name("20240101T120000")
        assert result == datetime(2024, 1, 1, 12, 0, 0)

    def test_extract_from_group_name_nested(self):
        """Test extracting timestamp from nested group path."""
        result = extract_timestamp_from_group_name("cycle/20240101T120000")
        assert result == datetime(2024, 1, 1, 12, 0, 0)

    def test_extract_from_group_name_deeply_nested(self):
        """Test extracting timestamp from deeply nested path."""
        result = extract_timestamp_from_group_name(
            "data/cycle/forecast/20240101T120000"
        )
        assert result == datetime(2024, 1, 1, 12, 0, 0)

    def test_extract_from_group_name_standard_iso(self):
        """Test extracting standard ISO8601 from group name."""
        result = extract_timestamp_from_group_name("cycle/2024-01-01T12:00:00")
        assert result == datetime(2024, 1, 1, 12, 0, 0)

    def test_extract_from_group_name_invalid(self):
        """Test extracting from invalid group name returns None."""
        result = extract_timestamp_from_group_name("cycle/invalid")
        assert result is None

    def test_default_formats_order(self):
        """Test that default formats are tried in order."""
        # Compact format should match first
        assert DEFAULT_TIMESTAMP_FORMATS[0] == "%Y%m%dT%H%M%S"
        assert DEFAULT_TIMESTAMP_FORMATS[1] == "%Y-%m-%dT%H:%M:%S"


# =============================================================================
# Datamesh Backend Tests
# =============================================================================


class TestDatameshBackend:
    """Test DatameshRollingArchiveBackend with mocked ZarrClient."""

    @pytest.fixture
    def mock_zarr_client(self):
        """Create a mock ZarrClient that behaves like a MutableMapping."""
        client = MagicMock()
        keys = [
            "cycle/20240101T000000/.zgroup",
            "cycle/20240101T060000/.zgroup",
            "cycle/20240101T120000/.zgroup",
        ]
        client.__iter__ = Mock(return_value=iter(keys))
        client.__contains__ = Mock(
            side_effect=lambda k: k.rstrip("/")
            in [
                "cycle/20240101T000000",
                "cycle/20240101T060000",
                "cycle/20240101T120000",
            ]
        )
        return client

    def test_backend_type(self, mock_zarr_client):
        """Test backend_type property returns 'datamesh'."""
        backend = DatameshRollingArchiveBackend(mock_zarr_client)
        assert backend.backend_type == "datamesh"

    def test_enumerate_groups(self, mock_zarr_client):
        """Test group enumeration."""
        backend = DatameshRollingArchiveBackend(mock_zarr_client)
        groups = backend.enumerate_groups()
        assert len(groups) == 3
        assert "cycle/20240101T000000" in groups
        assert "cycle/20240101T060000" in groups
        assert "cycle/20240101T120000" in groups

    def test_enumerate_groups_empty(self):
        """Test enumeration with empty client."""
        client = Mock()
        client.__iter__ = Mock(return_value=iter([]))
        backend = DatameshRollingArchiveBackend(client)
        groups = backend.enumerate_groups()
        assert len(groups) == 0

    def test_get_group_timestamp_from_name(self, mock_zarr_client):
        """Test timestamp extraction from group name."""
        backend = DatameshRollingArchiveBackend(mock_zarr_client)
        timestamp = backend.get_group_timestamp("cycle/20240101T120000")
        assert timestamp == datetime(2024, 1, 1, 12, 0, 0)

    def test_get_group_timestamp_standard_iso(self, mock_zarr_client):
        """Test timestamp extraction with standard ISO8601."""
        backend = DatameshRollingArchiveBackend(mock_zarr_client)
        timestamp = backend.get_group_timestamp("cycle/2024-01-01T12:00:00")
        assert timestamp == datetime(2024, 1, 1, 12, 0, 0)

    def test_get_group_timestamp_invalid_name(self, mock_zarr_client):
        """Test timestamp extraction with invalid group name."""
        mock_zarr_client.__getitem__ = Mock(side_effect=KeyError("Not found"))
        backend = DatameshRollingArchiveBackend(mock_zarr_client)
        timestamp = backend.get_group_timestamp("cycle/invalid")
        assert timestamp is None

    def test_get_group_timestamp_custom_attr(self, mock_zarr_client):
        """Test timestamp extraction with custom time_reference_attr."""
        backend = DatameshRollingArchiveBackend(mock_zarr_client)
        # Should still fall back to name parsing
        timestamp = backend.get_group_timestamp(
            "cycle/20240101T120000", time_reference_attr="forecast_time"
        )
        assert timestamp == datetime(2024, 1, 1, 12, 0, 0)

    def test_delete_groups_single(self, mock_zarr_client):
        """Test deleting a single group."""
        backend = DatameshRollingArchiveBackend(mock_zarr_client)
        result = backend.delete_groups(["cycle/20240101T000000"])
        assert "cycle/20240101T000000" in result["deleted"]
        assert len(result["deleted"]) == 1
        assert len(result.get("failed", [])) == 0
        mock_zarr_client.__delitem__.assert_called_once_with("cycle/20240101T000000")

    def test_delete_groups_multiple(self, mock_zarr_client):
        """Test deleting multiple groups."""
        backend = DatameshRollingArchiveBackend(mock_zarr_client)
        result = backend.delete_groups(
            ["cycle/20240101T000000", "cycle/20240101T060000"]
        )
        assert len(result["deleted"]) == 2
        assert "cycle/20240101T000000" in result["deleted"]
        assert "cycle/20240101T060000" in result["deleted"]
        assert mock_zarr_client.__delitem__.call_count == 2

    def test_delete_groups_dry_run(self, mock_zarr_client):
        """Test dry run doesn't delete."""
        backend = DatameshRollingArchiveBackend(mock_zarr_client)
        result = backend.delete_groups(["cycle/20240101T000000"], dry_run=True)
        assert "cycle/20240101T000000" in result["deleted"]
        mock_zarr_client.__delitem__.assert_not_called()

    def test_delete_groups_dry_run_multiple(self, mock_zarr_client):
        """Test dry run with multiple groups."""
        backend = DatameshRollingArchiveBackend(mock_zarr_client)
        result = backend.delete_groups(
            ["cycle/20240101T000000", "cycle/20240101T060000", "cycle/20240101T120000"],
            dry_run=True,
        )
        assert len(result["deleted"]) == 3
        mock_zarr_client.__delitem__.assert_not_called()

    def test_delete_groups_with_failure(self):
        """Test deletion with failures raises exception."""
        client = MagicMock()
        client.__contains__ = Mock(return_value=True)
        client.__delitem__ = Mock(side_effect=Exception("Delete failed"))
        backend = DatameshRollingArchiveBackend(client)
        with pytest.raises(Exception, match="Delete failed"):
            backend.delete_groups(["cycle/20240101T000000"])

    def test_delete_groups_partial_failure(self):
        """Test deletion with partial failures raises exception."""
        client = MagicMock()
        client.__contains__ = Mock(return_value=True)

        def delete_side_effect(key):
            if "060000" in key:
                raise Exception("Delete failed")

        client.__delitem__ = Mock(side_effect=delete_side_effect)
        backend = DatameshRollingArchiveBackend(client)
        with pytest.raises(Exception, match="Delete failed"):
            backend.delete_groups(["cycle/20240101T000000", "cycle/20240101T060000"])

    def test_delete_groups_empty_list(self, mock_zarr_client):
        """Test deletion with empty group list."""
        backend = DatameshRollingArchiveBackend(mock_zarr_client)
        result = backend.delete_groups([])
        assert len(result["deleted"]) == 0
        assert len(result.get("failed", [])) == 0
        mock_zarr_client.__delitem__.assert_not_called()


# =============================================================================
# File Backend Tests
# =============================================================================


class TestFileBackend:
    """Test FileRollingArchiveBackend with temp Zarr stores."""

    @pytest.fixture
    def temp_zarr_store(self):
        """Create a temporary Zarr store with test groups."""
        with tempfile.TemporaryDirectory() as tmpdir:
            store = zarr.open_group(tmpdir, mode="w")
            # Create test groups
            g1 = store.create_group("cycle/20240101T000000")
            g1.attrs["cycle_time"] = "2024-01-01T00:00:00"
            g2 = store.create_group("cycle/20240101T060000")
            g2.attrs["cycle_time"] = "2024-01-01T06:00:00"
            g3 = store.create_group("cycle/20240101T120000")
            g3.attrs["cycle_time"] = "2024-01-01T12:00:00"
            yield tmpdir

    def test_backend_type(self, temp_zarr_store):
        """Test backend_type property returns 'file'."""
        backend = FileRollingArchiveBackend(temp_zarr_store)
        assert backend.backend_type == "file"

    def test_enumerate_groups(self, temp_zarr_store):
        """Test group enumeration."""
        backend = FileRollingArchiveBackend(temp_zarr_store)
        groups = backend.enumerate_groups()
        group_set = set(groups)
        assert "cycle/20240101T000000" in group_set
        assert "cycle/20240101T060000" in group_set
        assert "cycle/20240101T120000" in group_set

    def test_enumerate_groups_nonexistent_path(self):
        """Test enumeration with non-existent path."""
        with tempfile.TemporaryDirectory() as tmpdir:
            nonexistent = Path(tmpdir) / "does_not_exist"
            backend = FileRollingArchiveBackend(nonexistent)
            groups = backend.enumerate_groups()
            assert len(groups) == 0

    def test_enumerate_groups_nested(self):
        """Test enumeration with nested groups."""
        with tempfile.TemporaryDirectory() as tmpdir:
            store = zarr.open_group(tmpdir, mode="w")
            store.create_group("data/cycle/20240101T000000")
            store.create_group("data/cycle/20240101T060000")
            backend = FileRollingArchiveBackend(tmpdir)
            groups = backend.enumerate_groups()
            assert len(groups) >= 3  # data, data/cycle, and the timestamped groups

    def test_get_group_timestamp_from_name(self, temp_zarr_store):
        """Test timestamp extraction from group name."""
        backend = FileRollingArchiveBackend(temp_zarr_store)
        timestamp = backend.get_group_timestamp("cycle/20240101T120000")
        assert timestamp == datetime(2024, 1, 1, 12, 0, 0)

    def test_get_group_timestamp_from_attrs(self, temp_zarr_store):
        """Test timestamp extraction from group attributes."""
        backend = FileRollingArchiveBackend(temp_zarr_store)
        # Create group with non-parseable name but valid attribute
        store = zarr.open_group(temp_zarr_store, mode="a")
        g = store.create_group("cycle/custom_group")
        g.attrs["cycle_time"] = "20240101T180000"

        timestamp = backend.get_group_timestamp("cycle/custom_group")
        assert timestamp == datetime(2024, 1, 1, 18, 0, 0)

    def test_get_group_timestamp_custom_attr(self, temp_zarr_store):
        """Test timestamp extraction with custom time_reference_attr."""
        backend = FileRollingArchiveBackend(temp_zarr_store)
        # Create group with custom attribute
        store = zarr.open_group(temp_zarr_store, mode="a")
        g = store.create_group("cycle/custom")
        g.attrs["forecast_time"] = "20240101T180000"

        timestamp = backend.get_group_timestamp(
            "cycle/custom", time_reference_attr="forecast_time"
        )
        assert timestamp == datetime(2024, 1, 1, 18, 0, 0)

    def test_get_group_timestamp_invalid(self, temp_zarr_store):
        """Test timestamp extraction with invalid group."""
        backend = FileRollingArchiveBackend(temp_zarr_store)
        timestamp = backend.get_group_timestamp("nonexistent/group")
        assert timestamp is None

    def test_delete_groups_single(self, temp_zarr_store):
        """Test deleting a single group."""
        backend = FileRollingArchiveBackend(temp_zarr_store)
        result = backend.delete_groups(["cycle/20240101T000000"])
        assert "cycle/20240101T000000" in result["deleted"]
        assert len(result["deleted"]) == 1

        # Verify deletion
        groups = backend.enumerate_groups()
        assert "cycle/20240101T000000" not in groups
        assert "cycle/20240101T060000" in groups

    def test_delete_groups_multiple(self, temp_zarr_store):
        """Test deleting multiple groups."""
        backend = FileRollingArchiveBackend(temp_zarr_store)
        result = backend.delete_groups(
            ["cycle/20240101T000000", "cycle/20240101T060000"]
        )
        assert len(result["deleted"]) == 2

        # Verify deletion
        groups = backend.enumerate_groups()
        assert "cycle/20240101T000000" not in groups
        assert "cycle/20240101T060000" not in groups
        assert "cycle/20240101T120000" in groups

    def test_delete_groups_dry_run(self, temp_zarr_store):
        """Test dry run doesn't delete."""
        backend = FileRollingArchiveBackend(temp_zarr_store)
        result = backend.delete_groups(["cycle/20240101T000000"], dry_run=True)
        assert "cycle/20240101T000000" in result["deleted"]

        # Verify no deletion
        groups = backend.enumerate_groups()
        assert "cycle/20240101T000000" in groups

    def test_delete_groups_nonexistent(self, temp_zarr_store):
        """Test deleting non-existent group."""
        backend = FileRollingArchiveBackend(temp_zarr_store)
        result = backend.delete_groups(["cycle/nonexistent"])
        assert "cycle/nonexistent" in result["failed"]

    def test_delete_groups_nonexistent_store(self):
        """Test deletion when store doesn't exist."""
        with tempfile.TemporaryDirectory() as tmpdir:
            nonexistent = Path(tmpdir) / "does_not_exist"
            backend = FileRollingArchiveBackend(nonexistent)
            result = backend.delete_groups(["cycle/20240101T000000"])
            assert len(result["deleted"]) == 0
            assert len(result["failed"]) == 0

    def test_delete_groups_empty_list(self, temp_zarr_store):
        """Test deletion with empty group list."""
        backend = FileRollingArchiveBackend(temp_zarr_store)
        result = backend.delete_groups([])
        assert len(result["deleted"]) == 0
        assert len(result.get("failed", [])) == 0


# =============================================================================
# Backend Interface Compliance Tests
# =============================================================================


class TestBackendInterface:
    """Test that backends comply with abstract interface."""

    def test_datamesh_backend_implements_interface(self):
        """Test DatameshRollingArchiveBackend implements all abstract methods."""
        client = Mock()
        backend = DatameshRollingArchiveBackend(client)
        assert isinstance(backend, RollingArchiveBackend)
        assert hasattr(backend, "enumerate_groups")
        assert hasattr(backend, "get_group_timestamp")
        assert hasattr(backend, "delete_groups")
        assert hasattr(backend, "backend_type")

    def test_file_backend_implements_interface(self):
        """Test FileRollingArchiveBackend implements all abstract methods."""
        with tempfile.TemporaryDirectory() as tmpdir:
            backend = FileRollingArchiveBackend(tmpdir)
            assert isinstance(backend, RollingArchiveBackend)
            assert hasattr(backend, "enumerate_groups")
            assert hasattr(backend, "get_group_timestamp")
            assert hasattr(backend, "delete_groups")
            assert hasattr(backend, "backend_type")

    def test_backend_method_signatures_datamesh(self):
        """Test DatameshRollingArchiveBackend method signatures."""
        client = MagicMock()
        client.__iter__ = Mock(return_value=iter([]))
        client.__getitem__ = Mock(side_effect=KeyError("Not found"))
        backend = DatameshRollingArchiveBackend(client)

        groups = backend.enumerate_groups()
        assert isinstance(groups, list)

        timestamp = backend.get_group_timestamp("test")
        assert timestamp is None or isinstance(timestamp, datetime)

        result = backend.delete_groups([], dry_run=True)
        assert isinstance(result, dict)
        assert "deleted" in result

        backend_type = backend.backend_type
        assert isinstance(backend_type, str)

    def test_backend_method_signatures_file(self):
        """Test FileRollingArchiveBackend method signatures."""
        with tempfile.TemporaryDirectory() as tmpdir:
            backend = FileRollingArchiveBackend(tmpdir)

            # Test method signatures
            groups = backend.enumerate_groups()
            assert isinstance(groups, list)

            timestamp = backend.get_group_timestamp("test")
            assert timestamp is None or isinstance(timestamp, datetime)

            result = backend.delete_groups([], dry_run=True)
            assert isinstance(result, dict)
            assert "deleted" in result

            backend_type = backend.backend_type
            assert isinstance(backend_type, str)


# =============================================================================
# Integration with ZarrConverterConfig Tests
# =============================================================================


class TestZarrConverterConfigIntegration:
    """Test RollingArchiveConfig integration with ZarrConverterConfig."""

    def test_default_rolling_archive_config(self):
        """Test ZarrConverterConfig has default rolling archive config."""
        config = ZarrConverterConfig()
        assert hasattr(config, "rolling_archive")
        assert isinstance(config.rolling_archive, RollingArchiveConfig)
        assert config.rolling_archive.enabled is False

    def test_custom_rolling_archive_config(self):
        """Test ZarrConverterConfig with custom rolling archive config."""
        config = ZarrConverterConfig(
            rolling_archive=RollingArchiveConfig(
                enabled=True,
                retention_window=timedelta(hours=24),
                min_groups_to_keep=2,
            )
        )
        assert config.rolling_archive.enabled is True
        assert config.rolling_archive.retention_window == timedelta(hours=24)
        assert config.rolling_archive.min_groups_to_keep == 2

    def test_rolling_archive_config_from_dict(self):
        """Test creating ZarrConverterConfig from dict with rolling archive."""
        config_dict = {
            "rolling_archive": {
                "enabled": True,
                "retention_window": timedelta(hours=48),
                "min_groups_to_keep": 3,
            }
        }
        config = ZarrConverterConfig(**config_dict)
        assert config.rolling_archive.enabled is True
        assert config.rolling_archive.retention_window == timedelta(hours=48)
        assert config.rolling_archive.min_groups_to_keep == 3


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
