"""
Tests for remote file support in zarrio.
"""

import pytest
from unittest.mock import patch, MagicMock
from zarrio import ZarrConverter, ZarrConverterConfig, RemoteFileConfig
from zarrio.core import FSSPEC_AVAILABLE


class TestRemoteURLDetection:
    """Test remote URL detection."""

    def test_is_remote_url_gs(self):
        """Test detection of gs:// URLs."""
        converter = ZarrConverter()
        assert converter._is_remote_url("gs://bucket/file.nc") is True

    def test_is_remote_url_s3(self):
        """Test detection of s3:// URLs."""
        converter = ZarrConverter()
        assert converter._is_remote_url("s3://bucket/file.nc") is True

    def test_is_remote_url_https(self):
        """Test detection of https:// URLs."""
        converter = ZarrConverter()
        assert converter._is_remote_url("https://example.com/file.nc") is True

    def test_is_remote_url_azure(self):
        """Test detection of Azure blob storage URLs."""
        converter = ZarrConverter()
        assert converter._is_remote_url("az://container/file.nc") is True

    def test_is_remote_url_local_file(self):
        """Test that local file paths are not detected as remote."""
        converter = ZarrConverter()
        assert converter._is_remote_url("/local/path/file.nc") is False
        assert converter._is_remote_url("./relative/path/file.nc") is False
        assert converter._is_remote_url("file.nc") is False

    def test_is_remote_url_file_protocol(self):
        """Test that file:// protocol is not detected as remote."""
        converter = ZarrConverter()
        assert converter._is_remote_url("file:///local/path/file.nc") is False


class TestRemoteFileConfig:
    """Test remote file configuration."""

    def test_default_config(self):
        """Test default remote file configuration."""
        config = ZarrConverterConfig()
        assert config.remote_files.engine == "h5netcdf"
        assert config.remote_files.cache_local is False
        assert config.remote_files.cache_dir is None

    def test_custom_config(self):
        """Test custom remote file configuration."""
        config = ZarrConverterConfig(
            remote_files=RemoteFileConfig(
                engine="netcdf4",
                cache_local=True,
                cache_dir="/tmp/cache",
            )
        )
        assert config.remote_files.engine == "netcdf4"
        assert config.remote_files.cache_local is True
        assert config.remote_files.cache_dir == "/tmp/cache"

    def test_invalid_engine_raises_error(self):
        """Test that invalid engine raises ValueError."""
        with pytest.raises(ValueError, match="engine must be 'h5netcdf' or 'netcdf4'"):
            RemoteFileConfig(engine="invalid")


class TestRemoteDatasetOpening:
    """Test remote dataset opening."""

    @pytest.mark.skipif(not FSSPEC_AVAILABLE, reason="fsspec not installed")
    @patch("zarrio.core.xr.open_dataset")
    def test_open_remote_netcdf_with_h5netcdf(self, mock_open_dataset):
        """Test opening remote NetCDF file with h5netcdf engine."""
        mock_ds = MagicMock()
        mock_open_dataset.return_value = mock_ds

        converter = ZarrConverter()
        result = converter._open_remote_dataset("gs://bucket/file.nc")

        mock_open_dataset.assert_called_once_with(
            "gs://bucket/file.nc", engine="h5netcdf"
        )
        assert result == mock_ds

    @pytest.mark.skipif(not FSSPEC_AVAILABLE, reason="fsspec not installed")
    @patch("zarrio.core.xr.open_dataset")
    def test_open_remote_netcdf_with_netcdf4_engine(self, mock_open_dataset):
        """Test opening remote NetCDF file with netcdf4 engine."""
        mock_ds = MagicMock()
        mock_open_dataset.return_value = mock_ds

        config = ZarrConverterConfig(remote_files=RemoteFileConfig(engine="netcdf4"))
        converter = ZarrConverter(config=config)
        result = converter._open_remote_dataset("s3://bucket/file.nc")

        mock_open_dataset.assert_called_once_with(
            "s3://bucket/file.nc", engine="netcdf4"
        )
        assert result == mock_ds

    @pytest.mark.skipif(not FSSPEC_AVAILABLE, reason="fsspec not installed")
    @patch("zarrio.core.xr.open_zarr")
    def test_open_remote_zarr(self, mock_open_zarr):
        """Test opening remote Zarr file."""
        mock_ds = MagicMock()
        mock_open_zarr.return_value = mock_ds

        converter = ZarrConverter()
        result = converter._open_remote_dataset("gs://bucket/data.zarr")

        mock_open_zarr.assert_called_once_with("gs://bucket/data.zarr")
        assert result == mock_ds

    def test_open_remote_without_fsspec_raises_import_error(self):
        """Test that ImportError is raised when fsspec is not available."""
        with patch("zarrio.core.FSSPEC_AVAILABLE", False):
            converter = ZarrConverter()
            with pytest.raises(
                ImportError, match="Remote file support requires fsspec"
            ):
                converter._open_remote_dataset("gs://bucket/file.nc")

    @pytest.mark.skipif(not FSSPEC_AVAILABLE, reason="fsspec not installed")
    @patch("zarrio.core.xr.open_dataset")
    def test_open_remote_nc4_extension(self, mock_open_dataset):
        """Test opening remote .nc4 file."""
        mock_ds = MagicMock()
        mock_open_dataset.return_value = mock_ds

        converter = ZarrConverter()
        result = converter._open_remote_dataset("gs://bucket/file.nc4")

        mock_open_dataset.assert_called_once_with(
            "gs://bucket/file.nc4", engine="h5netcdf"
        )
        assert result == mock_ds


class TestOpenDatasetIntegration:
    """Test the _open_dataset method with remote URL detection."""

    @patch.object(ZarrConverter, "_open_remote_dataset")
    def test_open_dataset_routes_to_remote_handler(self, mock_remote_open):
        """Test that remote URLs are routed to _open_remote_dataset."""
        mock_ds = MagicMock()
        mock_remote_open.return_value = mock_ds

        converter = ZarrConverter()
        result = converter._open_dataset("gs://bucket/file.nc")

        mock_remote_open.assert_called_once_with("gs://bucket/file.nc")
        assert result == mock_ds

    @patch("zarrio.core.xr.open_dataset")
    def test_open_dataset_handles_local_files(self, mock_open_dataset):
        """Test that local files are opened normally."""
        mock_ds = MagicMock()
        mock_open_dataset.return_value = mock_ds

        converter = ZarrConverter()
        result = converter._open_dataset("/local/path/file.nc")

        mock_open_dataset.assert_called_once_with("/local/path/file.nc")
        assert result == mock_ds

    @patch("zarrio.core.xr.open_zarr")
    def test_open_dataset_handles_local_zarr(self, mock_open_zarr):
        """Test that local Zarr files are opened with open_zarr."""
        mock_ds = MagicMock()
        mock_open_zarr.return_value = mock_ds

        converter = ZarrConverter()
        result = converter._open_dataset("/local/path/data.zarr")

        mock_open_zarr.assert_called_once_with("/local/path/data.zarr")
        assert result == mock_ds
