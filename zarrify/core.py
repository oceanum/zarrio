"""
Enhanced core functionality for zarrify with retry logic for missing data.
"""

import logging
import time
from typing import Dict, Optional, Union, Any, List
from pathlib import Path

import xarray as xr
import numpy as np
import dask.array as da

from .packing import Packer
from .time import TimeManager
from .exceptions import ConversionError, RetryLimitExceededError
from .models import (
    ZarrConverterConfig, 
    ChunkingConfig, 
    PackingConfig, 
    CompressionConfig,
    TimeConfig,
    VariableConfig,
    MissingDataConfig
)

logger = logging.getLogger(__name__)


class ZarrConverter:
    """Main class for converting data to Zarr format with retry logic."""
    
    def __init__(
        self,
        config: Optional[ZarrConverterConfig] = None,
        **kwargs
    ):
        """
        Initialize the ZarrConverter.
        
        Args:
            config: Pydantic configuration object
            **kwargs: Backward compatibility parameters
        """
        if config is None:
            # Create config from kwargs for backward compatibility
            config_dict = {}
            
            # Map old parameter names to new ones
            if 'chunking' in kwargs:
                config_dict['chunking'] = kwargs['chunking']
            if 'compression' in kwargs:
                config_dict['compression'] = {'method': kwargs['compression']}
            if 'packing' in kwargs:
                config_dict['packing'] = {'enabled': kwargs['packing']}
            if 'packing_bits' in kwargs:
                if 'packing' not in config_dict:
                    config_dict['packing'] = {}
                config_dict['packing']['bits'] = kwargs['packing_bits']
            if 'time_dim' in kwargs:
                config_dict['time'] = {'dim': kwargs['time_dim']}
            if 'append_dim' in kwargs:
                if 'time' not in config_dict:
                    config_dict['time'] = {}
                config_dict['time']['append_dim'] = kwargs['append_dim']
            if 'retries_on_missing' in kwargs:
                if 'missing_data' not in config_dict:
                    config_dict['missing_data'] = {}
                config_dict['missing_data']['retries_on_missing'] = kwargs['retries_on_missing']
            if 'missing_check_vars' in kwargs:
                if 'missing_data' not in config_dict:
                    config_dict['missing_data'] = {}
                config_dict['missing_data']['missing_check_vars'] = kwargs['missing_check_vars']
            
            config = ZarrConverterConfig(**config_dict)
        
        self.config = config
        
        # Initialize components
        self.packer = Packer(nbits=config.packing.bits) if config.packing.enabled else None
        self.time_manager = TimeManager(time_dim=config.time.dim)
        
        # Initialize missing data handler counters
        self.retried_on_missing = 0
        
        # Internal state
        self._current_dataset = None
        self._region = None
    
    @classmethod
    def from_config_file(cls, config_path: Union[str, Path]) -> "ZarrConverter":
        """
        Create ZarrConverter from configuration file.
        
        Args:
            config_path: Path to configuration file (YAML or JSON)
            
        Returns:
            ZarrConverter instance
        """
        from .models import load_config_from_file
        config = load_config_from_file(config_path)
        return cls(config=config)
    
    def create_template(
        self,
        template_dataset: xr.Dataset,
        output_path: Union[str, Path],
        global_start: Optional[Any] = None,
        global_end: Optional[Any] = None,
        freq: Optional[str] = None,
        compute: bool = False
    ) -> None:
        """
        Create a template Zarr archive for parallel writing.
        
        Args:
            template_dataset: Dataset to use as template for structure and metadata
            output_path: Path to output Zarr store
            global_start: Start time for the full archive
            global_end: End time for the full archive
            freq: Frequency for time coordinate (inferred from template if not provided)
            compute: Whether to compute immediately (False for template only)
        """
        try:
            # Use config values if not provided
            if global_start is None:
                global_start = self.config.time.global_start
            if global_end is None:
                global_end = self.config.time.global_end
            if freq is None:
                freq = self.config.time.freq
            
            # Create the full archive dataset
            archive_ds = self._create_hindcast_template(
                template_dataset, global_start, global_end, freq
            )
            
            # Setup encoding
            encoding = self._setup_encoding(archive_ds)
            
            # Apply chunking
            chunking_dict = self._chunking_config_to_dict()
            if chunking_dict:
                archive_ds = archive_ds.chunk(chunking_dict)
            
            # Write template (compute=False means metadata only)
            archive_ds.to_zarr(
                str(output_path), 
                mode="w", 
                encoding=encoding, 
                compute=compute
            )
            
            logger.info(f"Created template Zarr archive at {output_path}")
            
        except Exception as e:
            logger.error(f"Template creation failed: {e}")
            raise ConversionError(f"Failed to create template: {e}") from e
    
    def _chunking_config_to_dict(self) -> Dict[str, int]:
        """Convert ChunkingConfig to dictionary."""
        chunking_dict = {}
        if self.config.chunking.time is not None:
            chunking_dict['time'] = self.config.chunking.time
        if self.config.chunking.lat is not None:
            chunking_dict['lat'] = self.config.chunking.lat
        if self.config.chunking.lon is not None:
            chunking_dict['lon'] = self.config.chunking.lon
        return chunking_dict
    
    def _create_hindcast_template(
        self,
        template_ds: xr.Dataset,
        global_start: Optional[Any] = None,
        global_end: Optional[Any] = None,
        freq: Optional[str] = None
    ) -> xr.Dataset:
        """
        Create a hindcast template dataset with full time range.
        
        Args:
            template_ds: Template dataset to base structure on
            global_start: Start time for the full archive
            global_end: End time for the full archive
            freq: Frequency for time coordinate (inferred from template if not provided)
            
        Returns:
            Template dataset with full time range
        """
        # Determine time range
        if global_start is None:
            global_start = template_ds[self.config.time.dim].to_index()[0]
        if global_end is None:
            global_end = template_ds[self.config.time.dim].to_index()[-1]
        if freq is None:
            if len(template_ds[self.config.time.dim]) >= 2:
                times = template_ds[self.config.time.dim].to_index()
                freq = times[1] - times[0]
            else:
                # Default to daily if we can't infer
                freq = "1D"
        
        # Create full time coordinate
        import pandas as pd
        full_time = pd.date_range(start=global_start, end=global_end, freq=freq)
        
        # Create template dataset with dask arrays
        template_archive = xr.Dataset()
        
        # Copy global attributes
        template_archive.attrs.update(template_ds.attrs)
        
        # First, copy coordinate variables
        for coord_name in template_ds.coords:
            if coord_name == self.config.time.dim:
                # Replace time coordinate with full range
                template_archive.coords[coord_name] = full_time
            else:
                # Keep other coordinates as they are
                template_archive.coords[coord_name] = template_ds.coords[coord_name]
        
        # Then create variables with full time dimension
        for var_name, var in template_ds.data_vars.items():
            # Get dimensions and coordinates
            dims = var.dims
            coords = {}
            shape = []
            chunks = []
            
            # Process each dimension
            for i, dim in enumerate(dims):
                if dim == self.config.time.dim:
                    # Use full time range
                    coords[dim] = full_time
                    shape.append(len(full_time))
                else:
                    # Use original coordinate
                    coords[dim] = var.coords[dim]
                    shape.append(len(var.coords[dim]))
                
                # Determine chunking
                chunking_dict = self._chunking_config_to_dict()
                if dim in chunking_dict:
                    chunks.append(chunking_dict[dim])
                else:
                    # Use variable's original chunking or full dimension size
                    if hasattr(var.data, 'chunks') and var.data.chunks and i < len(var.data.chunks):
                        chunks.append(var.data.chunks[i])
                    else:
                        chunks.append(shape[-1])
            
            # Create empty dask array
            data = da.zeros(shape, chunks=chunks, dtype=var.dtype)
            
            # Add variable to template
            template_archive[var_name] = xr.DataArray(
                data=data, 
                coords=coords, 
                dims=dims, 
                attrs=var.attrs
            )
        
        return template_archive
    
    def write_region(
        self,
        input_path: Union[str, Path],
        zarr_path: Union[str, Path],
        region: Optional[Dict[str, slice]] = None,
        variables: Optional[list] = None,
        drop_variables: Optional[list] = None
    ) -> None:
        """
        Write data to a specific region of an existing Zarr store with retry logic.
        
        Args:
            input_path: Path to input file
            zarr_path: Path to existing Zarr store
            region: Dictionary specifying the region to write to
            variables: List of variables to include (None for all)
            drop_variables: List of variables to exclude
        """
        try:
            # Reset retry counter for new operation
            self.retried_on_missing = 0
            
            # Perform the actual write operation with retry logic
            self._write_region_with_retry(
                input_path, zarr_path, region, variables, drop_variables
            )
            
        except Exception as e:
            logger.error(f"Region writing failed: {e}")
            raise ConversionError(f"Failed to write region: {e}") from e
    
    def _write_region_with_retry(
        self,
        input_path: Union[str, Path],
        zarr_path: Union[str, Path],
        region: Optional[Dict[str, slice]] = None,
        variables: Optional[list] = None,
        drop_variables: Optional[list] = None
    ) -> None:
        """
        Write data to a specific region with retry logic for missing data.
        
        Args:
            input_path: Path to input file
            zarr_path: Path to existing Zarr store
            region: Dictionary specifying the region to write to
            variables: List of variables to include (None for all)
            drop_variables: List of variables to exclude
        """
        max_retries = self.config.missing_data.retries_on_missing
        
        while True:
            try:
                # Open dataset
                ds = self._open_dataset(input_path)
                
                # Process dataset
                ds = self._process_dataset(ds, variables, drop_variables)
                
                # Store current dataset for missing data check
                self._current_dataset = ds
                
                # If no region specified, determine automatically
                if region is None:
                    region = self._determine_region(ds, zarr_path)
                
                # Store region for missing data check
                self._region = region
                
                # Setup encoding (minimal for region writing)
                encoding = {}
                
                # Apply chunking
                chunking_dict = self._chunking_config_to_dict()
                if chunking_dict:
                    ds = ds.chunk(chunking_dict)
                
                # Write to region
                ds.to_zarr(str(zarr_path), region=region, encoding=encoding, safe_chunks=False)
                
                logger.info(f"Successfully wrote region {region} from {input_path} to {zarr_path}")
                
                # Check for missing data if configured
                if self.config.missing_data.missing_check_vars and self._has_missing(zarr_path, ds, region):
                    logger.info("Missing data detected - rewriting region")
                    if self.retried_on_missing < max_retries:
                        self.retried_on_missing += 1
                        logger.info(f"Retry {self.retried_on_missing}/{max_retries}")
                        # Wait a bit before retry to allow system to stabilize
                        time.sleep(0.1 * self.retried_on_missing)
                        continue
                    else:
                        raise RetryLimitExceededError(
                            f"Missing data present, retry limit exceeded after "
                            f"{self.retried_on_missing} retries"
                        )
                else:
                    # Success - no missing data or missing data check disabled
                    break
                    
            except RetryLimitExceededError:
                raise
            except Exception as e:
                logger.error(f"Region writing attempt failed: {e}")
                if self.retried_on_missing < max_retries:
                    self.retried_on_missing += 1
                    logger.info(f"Retry {self.retried_on_missing}/{max_retries}")
                    # Wait a bit before retry
                    time.sleep(0.1 * self.retried_on_missing)
                    continue
                else:
                    raise ConversionError(f"Region writing failed after {self.retried_on_missing} retries: {e}") from e
    
    def _has_missing(
        self,
        zarr_path: Union[str, Path],
        input_dataset: xr.Dataset,
        region: Optional[Dict[str, slice]] = None
    ) -> bool:
        """
        Check data just written for missing values.
        
        Args:
            zarr_path: Path to Zarr store
            input_dataset: Input dataset that was written
            region: Region that was written to
            
        Returns:
            True if missing data is detected, False otherwise
        """
        if not self.config.missing_data.missing_check_vars:
            logger.warning("No vars specified for checking for missing values")
            return False
        
        try:
            # Open existing Zarr store
            with xr.open_zarr(str(zarr_path), consolidated=True) as store_dset:
                # Determine variables to check
                missing_check_vars = self.config.missing_data.missing_check_vars
                if missing_check_vars == "all":
                    missing_check_vars = list(store_dset.data_vars.keys())
                elif not isinstance(missing_check_vars, (list, tuple)):
                    logger.warning(
                        "`missing_check_vars` must be one of 'all', None or a list of data"
                        f" vars to check for missing values, got {missing_check_vars}"
                    )
                    return False
                
                # Datasets to compare
                dset_out = store_dset[missing_check_vars]
                dset_in = input_dataset[missing_check_vars]
                
                # Missing values from input and output datasets
                if region is not None:
                    region_filtered = {k: v for k, v in region.items() if k in dset_in.dims}
                    dsmiss_in = dset_in.isnull()
                    dsmiss_out = dset_out.isel(region_filtered).isnull()
                else:
                    dsmiss_in = dset_in.isnull()
                    dsmiss_out = dset_out.isnull()
                
                # Check missing values are equal for each variable
                _has_missing = False
                for var in dsmiss_out.data_vars:
                    if not (dsmiss_out[var] == dsmiss_in[var]).all():
                        logger.warning(
                            f"Variable '{var}' has missing values in store {zarr_path} "
                            f"that are not present in input dataset {input_dataset.encoding.get('source', 'unknown')}"
                        )
                        _has_missing = True
                
                return _has_missing
                
        except Exception as e:
            logger.warning(f"Could not check for missing data: {e}")
            return False
    
    def _determine_region(
        self, 
        ds: xr.Dataset, 
        zarr_path: Union[str, Path]
    ) -> Dict[str, slice]:
        """
        Automatically determine the region for writing based on time coordinates.
        
        Args:
            ds: Dataset to write
            zarr_path: Path to existing Zarr store
            
        Returns:
            Dictionary specifying the region to write to
        """
        # Open existing Zarr store
        existing_ds = xr.open_zarr(str(zarr_path))
        
        # Get time ranges
        ds_start = ds[self.config.time.dim].to_index()[0]
        ds_end = ds[self.config.time.dim].to_index()[-1]
        existing_times = existing_ds[self.config.time.dim].to_index()
        
        # Find indices in existing dataset
        start_idx = np.searchsorted(existing_times, ds_start, side='left')
        end_idx = np.searchsorted(existing_times, ds_end, side='right')
        
        # Create region dictionary
        region = {self.config.time.dim: slice(start_idx, end_idx)}
        
        # Add full slices for other dimensions
        for dim in existing_ds.dims:
            if dim != self.config.time.dim:
                region[dim] = slice(None)
        
        return region
    
    def convert(
        self,
        input_path: Union[str, Path],
        output_path: Union[str, Path],
        variables: Optional[list] = None,
        drop_variables: Optional[list] = None,
        attrs: Optional[Dict[str, Any]] = None
    ) -> None:
        """
        Convert input data to Zarr format with retry logic.
        
        Args:
            input_path: Path to input file
            output_path: Path to output Zarr store
            variables: List of variables to include (None for all)
            drop_variables: List of variables to exclude
            attrs: Additional global attributes to add
        """
        try:
            # Reset retry counter for new operation
            self.retried_on_missing = 0
            
            # Perform conversion with retry logic
            self._convert_with_retry(
                input_path, output_path, variables, drop_variables, attrs
            )
            
        except Exception as e:
            logger.error(f"Conversion failed: {e}")
            raise ConversionError(f"Failed to convert {input_path} to Zarr: {e}") from e
    
    def _convert_with_retry(
        self,
        input_path: Union[str, Path],
        output_path: Union[str, Path],
        variables: Optional[list] = None,
        drop_variables: Optional[list] = None,
        attrs: Optional[Dict[str, Any]] = None
    ) -> None:
        """
        Convert input data to Zarr format with retry logic.
        
        Args:
            input_path: Path to input file
            output_path: Path to output Zarr store
            variables: List of variables to include (None for all)
            drop_variables: List of variables to exclude
            attrs: Additional global attributes to add
        """
        max_retries = self.config.missing_data.retries_on_missing
        
        while True:
            try:
                # Open dataset
                ds = self._open_dataset(input_path)
                
                # Process dataset
                ds = self._process_dataset(ds, variables, drop_variables, attrs)
                
                # Store current dataset for missing data check
                self._current_dataset = ds
                
                # Setup encoding
                encoding = self._setup_encoding(ds)
                
                # Apply chunking
                chunking_dict = self._chunking_config_to_dict()
                if chunking_dict:
                    ds = ds.chunk(chunking_dict)
                
                # Write to Zarr
                ds.to_zarr(str(output_path), mode="w", encoding=encoding)
                
                logger.info(f"Successfully converted {input_path} to {output_path}")
                
                # Check for missing data if configured
                if self.config.missing_data.missing_check_vars and self._has_missing(output_path, ds):
                    logger.info("Missing data detected - rewriting")
                    if self.retried_on_missing < max_retries:
                        self.retried_on_missing += 1
                        logger.info(f"Retry {self.retried_on_missing}/{max_retries}")
                        # Wait a bit before retry to allow system to stabilize
                        time.sleep(0.1 * self.retried_on_missing)
                        continue
                    else:
                        raise RetryLimitExceededError(
                            f"Missing data present, retry limit exceeded after "
                            f"{self.retried_on_missing} retries"
                        )
                else:
                    # Success - no missing data or missing data check disabled
                    break
                    
            except RetryLimitExceededError:
                raise
            except Exception as e:
                logger.error(f"Conversion attempt failed: {e}")
                if self.retried_on_missing < max_retries:
                    self.retried_on_missing += 1
                    logger.info(f"Retry {self.retried_on_missing}/{max_retries}")
                    # Wait a bit before retry
                    time.sleep(0.1 * self.retried_on_missing)
                    continue
                else:
                    raise ConversionError(f"Conversion failed after {self.retried_on_missing} retries: {e}") from e
    
    def append(
        self,
        input_path: Union[str, Path],
        zarr_path: Union[str, Path],
        variables: Optional[list] = None,
        drop_variables: Optional[list] = None
    ) -> None:
        """
        Append data to an existing Zarr store with retry logic.
        
        Args:
            input_path: Path to input file
            zarr_path: Path to existing Zarr store
            variables: List of variables to include (None for all)
            drop_variables: List of variables to exclude
        """
        try:
            # Reset retry counter for new operation
            self.retried_on_missing = 0
            
            # Perform append with retry logic
            self._append_with_retry(
                input_path, zarr_path, variables, drop_variables
            )
            
        except Exception as e:
            logger.error(f"Append failed: {e}")
            raise ConversionError(f"Failed to append {input_path} to {zarr_path}: {e}") from e
    
    def _append_with_retry(
        self,
        input_path: Union[str, Path],
        zarr_path: Union[str, Path],
        variables: Optional[list] = None,
        drop_variables: Optional[list] = None
    ) -> None:
        """
        Append data to an existing Zarr store with retry logic.
        
        Args:
            input_path: Path to input file
            zarr_path: Path to existing Zarr store
            variables: List of variables to include (None for all)
            drop_variables: List of variables to exclude
        """
        max_retries = self.config.missing_data.retries_on_missing
        
        while True:
            try:
                # Open datasets
                new_ds = self._open_dataset(input_path)
                existing_ds = xr.open_zarr(str(zarr_path))
                
                # Process new dataset
                new_ds = self._process_dataset(new_ds, variables, drop_variables)
                
                # Store current dataset for missing data check
                self._current_dataset = new_ds
                
                # Align time dimensions
                new_ds = self.time_manager.align_for_append(existing_ds, new_ds)
                
                # Setup encoding (minimal for append)
                encoding = {}
                
                # Apply chunking
                chunking_dict = self._chunking_config_to_dict()
                if chunking_dict:
                    new_ds = new_ds.chunk(chunking_dict)
                
                # Append to Zarr
                new_ds.to_zarr(str(zarr_path), append_dim=self.config.time.append_dim, encoding=encoding)
                
                logger.info(f"Successfully appended {input_path} to {zarr_path}")
                
                # Check for missing data if configured
                if self.config.missing_data.missing_check_vars and self._has_missing(zarr_path, new_ds):
                    logger.info("Missing data detected - rewriting")
                    if self.retried_on_missing < max_retries:
                        self.retried_on_missing += 1
                        logger.info(f"Retry {self.retried_on_missing}/{max_retries}")
                        # Wait a bit before retry to allow system to stabilize
                        time.sleep(0.1 * self.retried_on_missing)
                        continue
                    else:
                        raise RetryLimitExceededError(
                            f"Missing data present, retry limit exceeded after "
                            f"{self.retried_on_missing} retries"
                        )
                else:
                    # Success - no missing data or missing data check disabled
                    break
                    
            except RetryLimitExceededError:
                raise
            except Exception as e:
                logger.error(f"Append attempt failed: {e}")
                if self.retried_on_missing < max_retries:
                    self.retried_on_missing += 1
                    logger.info(f"Retry {self.retried_on_missing}/{max_retries}")
                    # Wait a bit before retry
                    time.sleep(0.1 * self.retried_on_missing)
                    continue
                else:
                    raise ConversionError(f"Append failed after {self.retried_on_missing} retries: {e}") from e
    
    def _open_dataset(self, path: Union[str, Path]) -> xr.Dataset:
        """Open dataset from file."""
        path = str(path)
        if path.endswith('.nc') or path.endswith('.nc4'):
            return xr.open_dataset(path)
        elif path.endswith('.zarr'):
            return xr.open_zarr(path)
        else:
            # Try to infer from file content
            return xr.open_dataset(path)
    
    def _process_dataset(
        self,
        ds: xr.Dataset,
        variables: Optional[list] = None,
        drop_variables: Optional[list] = None,
        attrs: Optional[Dict[str, Any]] = None
    ) -> xr.Dataset:
        """Process dataset with standard operations."""
        # Select variables
        if variables is not None:
            ds = ds[variables]
        elif self.config.variables.include:
            ds = ds[self.config.variables.include]
        
        # Drop variables
        if drop_variables is not None:
            ds = ds.drop_vars(drop_variables, errors="ignore")
        elif self.config.variables.exclude:
            ds = ds.drop_vars(self.config.variables.exclude, errors="ignore")
        
        # Remove duplicate times
        ds = self.time_manager.remove_duplicates(ds)
        
        # Add attributes
        if attrs is not None:
            ds.attrs.update(attrs)
        elif self.config.attrs:
            ds.attrs.update(self.config.attrs)
            
        return ds
    
    def _setup_encoding(self, ds: xr.Dataset) -> Dict[str, Any]:
        """Setup encoding for Zarr storage."""
        encoding = {}
        
        # Setup compression
        if self.config.compression:
            compressor = self._create_compressor()
            for var in ds.data_vars:
                encoding[var] = {"compressor": compressor}
        
        # Setup packing
        if self.config.packing.enabled and self.packer:
            packing_encoding = self.packer.setup_encoding(ds)
            encoding.update(packing_encoding)
            
        # Setup coordinate chunking
        for coord_name in ds.coords:
            if coord_name == self.config.time.append_dim:
                encoding[coord_name] = {"chunks": (int(1e6),)}  # Large chunk for append dim
            else:
                encoding[coord_name] = {"chunks": (int(ds[coord_name].size),)}
        
        return encoding
    
    def _create_compressor(self):
        """Create compressor from configuration."""
        try:
            import zarr
            from zarr.codecs import BloscCodec
            
            if self.config.compression and self.config.compression.method:
                method = self.config.compression.method
                if method.startswith("blosc:"):
                    parts = method.split(":")
                    cname = parts[1] if len(parts) > 1 else "zstd"
                    clevel = int(parts[2]) if len(parts) > 2 else 1
                    return BloscCodec(cname=cname, clevel=clevel, shuffle="shuffle")
            
            # Default compressor
            return BloscCodec(cname="zstd", clevel=1, shuffle="shuffle")
        except ImportError:
            logger.warning("zarr not available, compression disabled")
            return None


# Convenience functions
def convert_to_zarr(
    input_path: Union[str, Path],
    output_path: Union[str, Path],
    chunking: Optional[Dict[str, int]] = None,
    compression: Optional[str] = None,
    packing: bool = False,
    packing_bits: int = 16,
    variables: Optional[list] = None,
    drop_variables: Optional[list] = None,
    attrs: Optional[Dict[str, Any]] = None,
    time_dim: str = "time",
    retries_on_missing: int = 0,
    missing_check_vars: Optional[Union[str, List[str]]] = "all"
) -> None:
    """
    Convert data to Zarr format using default settings with retry logic.
    
    Args:
        input_path: Path to input file
        output_path: Path to output Zarr store
        chunking: Dictionary specifying chunk sizes for dimensions
        compression: Compression specification
        packing: Whether to enable data packing
        packing_bits: Number of bits for packing
        variables: List of variables to include
        drop_variables: List of variables to exclude
        attrs: Additional global attributes
        time_dim: Name of the time dimension
        retries_on_missing: Number of retries if missing values are encountered
        missing_check_vars: Data variables to check for missing values
    """
    # Create config from parameters
    config_dict = {}
    if chunking:
        config_dict['chunking'] = chunking
    if compression:
        config_dict['compression'] = {'method': compression}
    if packing or packing_bits:
        config_dict['packing'] = {'enabled': packing, 'bits': packing_bits}
    if time_dim:
        config_dict['time'] = {'dim': time_dim}
    if retries_on_missing or missing_check_vars:
        config_dict['missing_data'] = {
            'retries_on_missing': retries_on_missing,
            'missing_check_vars': missing_check_vars
        }
    
    config = ZarrConverterConfig(**config_dict)
    converter = ZarrConverter(config=config)
    converter.convert(input_path, output_path, variables, drop_variables, attrs)


def append_to_zarr(
    input_path: Union[str, Path],
    zarr_path: Union[str, Path],
    chunking: Optional[Dict[str, int]] = None,
    variables: Optional[list] = None,
    drop_variables: Optional[list] = None,
    append_dim: str = "time",
    time_dim: str = "time",
    retries_on_missing: int = 0,
    missing_check_vars: Optional[Union[str, List[str]]] = "all"
) -> None:
    """
    Append data to an existing Zarr store with retry logic.
    
    Args:
        input_path: Path to input file
        zarr_path: Path to existing Zarr store
        chunking: Dictionary specifying chunk sizes for dimensions
        variables: List of variables to include
        drop_variables: List of variables to exclude
        append_dim: Dimension to append along
        time_dim: Name of the time dimension
        retries_on_missing: Number of retries if missing values are encountered
        missing_check_vars: Data variables to check for missing values
    """
    # Create config from parameters
    config_dict = {}
    if chunking:
        config_dict['chunking'] = chunking
    if append_dim or time_dim:
        config_dict['time'] = {'append_dim': append_dim, 'dim': time_dim}
    if retries_on_missing or missing_check_vars:
        config_dict['missing_data'] = {
            'retries_on_missing': retries_on_missing,
            'missing_check_vars': missing_check_vars
        }
    
    config = ZarrConverterConfig(**config_dict)
    converter = ZarrConverter(config=config)
    converter.append(input_path, zarr_path, variables, drop_variables)