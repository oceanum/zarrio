import json
import pytest
from unittest.mock import Mock, patch
import sys
import tempfile
from pathlib import Path

# Ensure project root is on PYTHONPATH for imports like 'import zarrio'
ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
import xarray as xr
import zarr

# Mock datamesh-related modules at import time to avoid real connections
sys.modules["oceanum"] = Mock()
sys.modules["oceanum.datamesh"] = Mock()
sys.modules["oceanum.datamesh.zarr"] = Mock()
sys.modules["oceanum.datamesh.session"] = Mock()
sys.modules["oceanum.datamesh.exceptions"] = Mock()

# Attempt to import the package, fallback to loading from file paths if not on PYTHONPATH
try:
    from zarrio.core import ZarrConverter
    from zarrio.models import ZarrConverterConfig, DatameshConfig
    from oceanum.datamesh.datasource import Datasource
except ModuleNotFoundError:
    import importlib.util

    ROOT = Path(__file__).resolve().parents[2]
    core_path = ROOT / "zarrio" / "core.py"
    spec = importlib.util.spec_from_file_location("zarrio.core", str(core_path))
    core = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(core)
    ZarrConverter = core.ZarrConverter

    models_path = ROOT / "zarrio" / "models.py"
    spec2 = importlib.util.spec_from_file_location("zarrio.models", str(models_path))
    mod2 = importlib.util.module_from_spec(spec2)
    spec2.loader.exec_module(mod2)
    ZarrConverterConfig = mod2.ZarrConverterConfig
    DatameshConfig = mod2.DatameshConfig


@pytest.fixture
def converter_with_group():
    """Yield a ZarrConverter configured to use a datamesh datasource."""
    config = ZarrConverterConfig(
        datamesh=DatameshConfig(
            datasource=Datasource(id="test-ds", name="Test Datasource", driver="vzarr"),
            token="fake-token",
            service="http://test",
            use_zarr_client=True,
        )
    )
    return ZarrConverter(config=config)


def test_convert_with_group(converter_with_group):
    converter = converter_with_group
    with (
        patch.object(converter, "_get_store") as mock_get_store,
        patch.object(converter, "_open_dataset") as mock_open,
        patch.object(converter, "_process_dataset", return_value=Mock()),
        patch.object(converter, "_setup_encoding", return_value={}),
        patch("xarray.Dataset.to_zarr") as mock_to_zarr,
    ):
        mock_store = Mock()
        mock_get_store.return_value = mock_store
        mock_ds = Mock()
        mock_open.return_value = mock_ds
        converter.convert("input.nc", "output.zarr", group="cycle/001")
        mock_get_store.assert_called_once()
        kwargs = mock_get_store.call_args.kwargs
        # Ensure the group was propagated to store creation
        assert kwargs.get("group") == "cycle/001"
        # Ensure to_zarr received the group argument as well (when used)
        if mock_to_zarr.call_args:
            to_zarr_kwargs = mock_to_zarr.call_args.kwargs
            assert to_zarr_kwargs.get("group") == "cycle/001"


def test_append_accepts_group_parameter():
    """Test that append method accepts group parameter in signature."""
    config = ZarrConverterConfig()
    converter = ZarrConverter(config=config)
    import inspect

    sig = inspect.signature(converter.append)
    assert "group" in sig.parameters
    assert sig.parameters["group"].default is None


def test_create_template_with_group(converter_with_group):
    """Test that create_template accepts group parameter in signature.

    Note: Full integration testing requires complex setup. This test verifies
    the parameter is properly accepted and would be passed to _get_store.
    """
    converter = converter_with_group
    # Just verify the method accepts the group parameter without error
    # The actual _get_store call happens internally
    import inspect

    sig = inspect.signature(converter.create_template)
    assert "group" in sig.parameters
    assert sig.parameters["group"].default is None


def test_write_region_with_group(converter_with_group):
    """Test that write_region accepts group parameter in signature."""
    converter = converter_with_group
    import inspect

    sig = inspect.signature(converter.write_region)
    assert "group" in sig.parameters
    assert sig.parameters["group"].default is None


def test_backward_compatibility():
    config = ZarrConverterConfig()
    converter = ZarrConverter(config=config)
    with tempfile.TemporaryDirectory() as tmpdir:
        input_path = Path(tmpdir) / "input.nc"
        output_path = Path(tmpdir) / "output.zarr"
        ds = xr.Dataset({"temp": (["time"], [1.0, 2.0, 3.0])})
        ds.to_netcdf(input_path)
        converter.convert(str(input_path), str(output_path))
        assert Path(output_path).exists()


def test_end_to_end_file_based_with_group():
    config = ZarrConverterConfig()
    converter = ZarrConverter(config=config)
    with tempfile.TemporaryDirectory() as tmpdir:
        input_path = Path(tmpdir) / "input.nc"
        output_path = Path(tmpdir) / "output.zarr"
        ds = xr.Dataset({"temp": (["time"], [1.0, 2.0, 3.0])})
        ds.to_netcdf(input_path)
        converter.convert(str(input_path), str(output_path), group="cycle/001")
        # Verify group exists in the resulting Zarr store
        store = zarr.open(str(output_path), mode="r")
        has_cycle = "cycle" in store or any("cycle" in k for k in store.keys())
        assert has_cycle
        ds_read = xr.open_zarr(str(output_path), group="cycle/001")
        assert "temp" in ds_read.data_vars


def test_nested_group_path(converter_with_group):
    """Test nested group path propagation through mocks."""
    converter = converter_with_group
    with (
        patch.object(converter, "_get_store") as mock_get_store,
        patch.object(converter, "_open_dataset") as mock_open,
        patch.object(converter, "_process_dataset", return_value=Mock()),
        patch.object(converter, "_setup_encoding", return_value={}),
        patch("xarray.Dataset.to_zarr") as mock_to_zarr,
    ):
        mock_store = Mock()
        mock_get_store.return_value = mock_store
        mock_ds = Mock()
        mock_open.return_value = mock_ds
        converter.convert("input.nc", "output.zarr", group="cycle/001/sub1")
        mock_get_store.assert_called_once()
        kwargs = mock_get_store.call_args.kwargs
        assert kwargs.get("group") == "cycle/001/sub1"
        if mock_to_zarr.call_args:
            to_zarr_kwargs = mock_to_zarr.call_args.kwargs
            assert to_zarr_kwargs.get("group") == "cycle/001/sub1"
