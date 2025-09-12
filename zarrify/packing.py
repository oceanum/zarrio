"""
Data packing functionality for zarrify.
"""

import logging
from typing import Dict, Any, Optional, Union, List

import xarray as xr
import numpy as np

try:
    from zarr.codecs import FixedScaleOffset
    ZARR_AVAILABLE = True
except ImportError:
    ZARR_AVAILABLE = False
    FixedScaleOffset = None

logger = logging.getLogger(__name__)


class Packer:
    """Handles data packing using fixed-scale offset encoding."""
    
    def __init__(self, nbits: int = 16):
        """
        Initialize the Packer.
        
        Args:
            nbits: Number of bits for packing (8, 16, 32)
        """
        if nbits not in [8, 16, 32]:
            raise ValueError("nbits must be one of 8, 16, or 32")
        
        self.nbits = nbits
        self.dtype_map = {8: "int8", 16: "int16", 32: "int32"}
        self.float_dtype = "float32"
    
    def compute_scale_and_offset(self, vmin: float, vmax: float) -> tuple:
        """
        Compute scale and offset for fixed-scale offset encoding.
        
        Args:
            vmin: Minimum value
            vmax: Maximum value
            
        Returns:
            Tuple of (scale_factor, offset)
        """
        if vmax == vmin:
            scale_factor = 1.0
        else:
            scale_factor = (vmax - vmin) / (2**self.nbits - 1)
        offset = vmin + 2 ** (self.nbits - 1) * scale_factor
        return scale_factor, offset
    
    def setup_encoding(
        self, 
        ds: xr.Dataset, 
        variables: Optional[List[str]] = None,
        manual_ranges: Optional[Dict[str, Dict[str, float]]] = None,
        auto_buffer_factor: float = 0.01,
        check_range_exceeded: bool = True,
        range_exceeded_action: str = "warn"
    ) -> Dict[str, Any]:
        """
        Setup encoding for dataset variables with enhanced packing options.
        
        Priority order for determining min/max values:
        1. Manual ranges (if provided)
        2. Variable attributes (valid_min/valid_max)
        3. Automatic calculation from data
        
        Args:
            ds: Dataset to setup encoding for
            variables: List of variables to pack (None for all numeric variables)
            manual_ranges: Dictionary specifying manual min/max values 
                          e.g., {"temperature": {"min": 0, "max": 100}}
            auto_buffer_factor: Buffer factor for automatically calculated ranges
            check_range_exceeded: Whether to check if data exceeds specified ranges
            range_exceeded_action: Action when data exceeds range ("warn", "error", "ignore")
            
        Returns:
            Dictionary of encoding specifications
        """
        if not ZARR_AVAILABLE:
            logger.warning("zarr not available, packing disabled")
            return {}
        
        encoding = {}
        manual_ranges = manual_ranges or {}
        
        # Determine which variables to pack
        if variables is None:
            # Pack all numeric variables
            variables = [var for var in ds.data_vars 
                        if np.issubdtype(ds[var].dtype, np.number)]
        
        for var in variables:
            if var in ds.data_vars:
                # Determine min/max values based on priority order
                vmin, vmax = self._get_variable_range(ds, var, manual_ranges, auto_buffer_factor)
                
                if vmin is not None and vmax is not None:
                    # Check if data exceeds specified range
                    if check_range_exceeded:
                        self._check_range_exceeded(ds, var, vmin, vmax, range_exceeded_action)
                    
                    # Compute scale and offset
                    scale_factor, offset = self.compute_scale_and_offset(vmin, vmax)
                    
                    # Create FixedScaleOffset filter
                    filt = FixedScaleOffset(
                        offset=offset,
                        scale=1 / scale_factor,
                        dtype=self.float_dtype,
                        astype=self.dtype_map[self.nbits]
                    )
                    
                    encoding[var] = {
                        "filters": [filt],
                        "_FillValue": vmax,
                        "dtype": self.float_dtype
                    }
                    
                    logger.debug(f"Setup packing for variable {var} with scale={scale_factor}, offset={offset}")
                else:
                    logger.debug(f"Could not determine valid range for variable {var}, skipping packing")
        
        return encoding
    
    def _get_variable_range(
        self, 
        ds: xr.Dataset, 
        var: str, 
        manual_ranges: Dict[str, Dict[str, float]], 
        auto_buffer_factor: float
    ) -> tuple:
        """
        Determine min/max values for a variable based on priority order.
        
        Args:
            ds: Dataset containing the variable
            var: Variable name
            manual_ranges: Manual ranges dictionary
            auto_buffer_factor: Buffer factor for automatic calculation
            
        Returns:
            Tuple of (vmin, vmax) or (None, None) if unable to determine
        """
        # 1. Check for manual ranges
        if var in manual_ranges:
            manual_min = manual_ranges[var].get("min")
            manual_max = manual_ranges[var].get("max")
            
            if manual_min is not None and manual_max is not None:
                # Warn if manual ranges override existing attributes
                attr_min = ds[var].attrs.get("valid_min")
                attr_max = ds[var].attrs.get("valid_max")
                if attr_min is not None and attr_max is not None:
                    logger.warning(
                        f"Using manually specified range [{manual_min}, {manual_max}] for variable {var} "
                        f"instead of attributes [{attr_min}, {attr_max}]"
                    )
                return float(manual_min), float(manual_max)
        
        # 2. Check for variable attributes
        vmin = ds[var].attrs.get("valid_min")
        vmax = ds[var].attrs.get("valid_max")
        if vmin is not None and vmax is not None:
            # Add small buffer to vmax to avoid masking valid data
            vmax = vmax + (vmax - vmin) * 0.001
            return float(vmin), float(vmax)
        
        # 3. Automatically calculate from data with warning
        logger.warning(
            f"Variable {var} missing valid_min/valid_max attributes. "
            f"Automatically calculating range from data. "
            f"Note: These values may be inaccurate for archives written a region at a time."
        )
        return self._calculate_range_from_data(ds, var, auto_buffer_factor)
    
    def _calculate_range_from_data(
        self, 
        ds: xr.Dataset, 
        var: str, 
        buffer_factor: float
    ) -> tuple:
        """
        Calculate min/max values from data with buffer.
        
        Args:
            ds: Dataset containing the variable
            var: Variable name
            buffer_factor: Buffer factor to extend range
            
        Returns:
            Tuple of (vmin, vmax)
        """
        # Compute min and max values
        vmin = float(ds[var].min().values)
        vmax = float(ds[var].max().values)
        
        # Apply buffer
        if vmin != vmax:
            range_size = vmax - vmin
            buffer = range_size * buffer_factor
            vmin -= buffer
            vmax += buffer
        else:
            # For constant fields, add a small buffer
            if vmin == 0:
                vmin = -0.01
                vmax = 0.01
            else:
                buffer = abs(vmin) * buffer_factor
                vmin -= buffer
                vmax += buffer
        
        logger.debug(f"Calculated range for {var}: [{vmin}, {vmax}] with buffer factor {buffer_factor}")
        return vmin, vmax
    
    def _check_range_exceeded(
        self, 
        ds: xr.Dataset, 
        var: str, 
        vmin: float, 
        vmax: float, 
        action: str
    ) -> None:
        """
        Check if data exceeds specified range and take appropriate action.
        
        Args:
            ds: Dataset containing the variable
            var: Variable name
            vmin: Specified minimum value
            vmax: Specified maximum value
            action: Action to take ("warn", "error", "ignore")
        """
        if action == "ignore":
            return
            
        # Compute actual min/max from data
        data_min = float(ds[var].min().values)
        data_max = float(ds[var].max().values)
        
        # Check if data exceeds range
        if data_min < vmin or data_max > vmax:
            message = (
                f"Data for variable {var} exceeds specified range [{vmin}, {vmax}]. "
                f"Actual range is [{data_min}, {data_max}]."
            )
            
            if action == "warn":
                logger.warning(message)
            elif action == "error":
                raise ValueError(message)
    
    def add_valid_range_attributes(
        self, 
        ds: xr.Dataset, 
        buffer_factor: float = 0.01,
        variables: Optional[List[str]] = None
    ) -> xr.Dataset:
        """
        Add valid_min and valid_max attributes to variables based on their data range.
        
        Args:
            ds: Dataset to add attributes to
            buffer_factor: Factor to extend range by (e.g., 0.01 = 1% buffer)
            variables: List of variables to process (None for all numeric variables)
            
        Returns:
            Dataset with added attributes
        """
        ds = ds.copy()
        
        # Determine which variables to process
        if variables is None:
            # Process all numeric variables
            variables = [var for var in ds.data_vars 
                        if np.issubdtype(ds[var].dtype, np.number)]
        
        for var in variables:
            if var in ds.data_vars and np.issubdtype(ds[var].dtype, np.number):
                # Compute min and max values
                vmin = float(ds[var].min().values)
                vmax = float(ds[var].max().values)
                
                # Apply buffer
                if vmin != vmax:
                    range_size = vmax - vmin
                    buffer = range_size * buffer_factor
                    vmin -= buffer
                    vmax += buffer
                else:
                    # For constant fields, add a small buffer
                    if vmin == 0:
                        vmin = -0.01
                        vmax = 0.01
                    else:
                        buffer = abs(vmin) * buffer_factor
                        vmin -= buffer
                        vmax += buffer
                
                # Add attributes
                ds[var].attrs["valid_min"] = vmin
                ds[var].attrs["valid_max"] = vmax
                
                logger.debug(f"Added valid range for {var}: [{vmin}, {vmax}]")
        
        return ds