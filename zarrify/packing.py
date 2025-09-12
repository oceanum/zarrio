"""
Data packing functionality for zarrify.
"""

import logging
from typing import Dict, Any

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
    
    def setup_encoding(self, ds: xr.Dataset, variables: list = None) -> Dict[str, Any]:
        """
        Setup encoding for dataset variables.
        
        Args:
            ds: Dataset to setup encoding for
            variables: List of variables to pack (None for all numeric variables)
            
        Returns:
            Dictionary of encoding specifications
        """
        if not ZARR_AVAILABLE:
            logger.warning("zarr not available, packing disabled")
            return {}
        
        encoding = {}
        
        # Determine which variables to pack
        if variables is None:
            # Pack all numeric variables
            variables = [var for var in ds.data_vars 
                        if np.issubdtype(ds[var].dtype, np.number)]
        
        for var in variables:
            if var in ds.data_vars:
                # Check if variable has valid range attributes
                vmin = ds[var].attrs.get("valid_min")
                vmax = ds[var].attrs.get("valid_max")
                
                if vmin is not None and vmax is not None:
                    # Add small buffer to vmax to avoid masking valid data
                    vmax = vmax + (vmax - vmin) * 0.001
                    
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
                    logger.debug(f"Variable {var} missing valid_min/valid_max attributes, skipping packing")
        
        return encoding
    
    def add_valid_range_attributes(self, ds: xr.Dataset, buffer_factor: float = 0.01) -> xr.Dataset:
        """
        Add valid_min and valid_max attributes to variables based on their data range.
        
        Args:
            ds: Dataset to add attributes to
            buffer_factor: Factor to extend range by (e.g., 0.01 = 1% buffer)
            
        Returns:
            Dataset with added attributes
        """
        ds = ds.copy()
        
        for var in ds.data_vars:
            if np.issubdtype(ds[var].dtype, np.number):
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