"""
Missing data detection and retry logic for zarrio.
"""

import logging
from typing import Dict, Optional, Union, Any, List, Callable
import xarray as xr
import numpy as np

logger = logging.getLogger(__name__)


class MissingDataHandler:
    """Handles missing data detection and retry logic."""
    
    def __init__(
        self,
        missing_check_vars: Optional[Union[str, List[str]]] = "all",
        retries_on_missing: int = 0,
        time_dim: str = "time"
    ):
        """
        Initialize the MissingDataHandler.
        
        Args:
            missing_check_vars: Variables to check for missing values ("all", None, or list)
            retries_on_missing: Number of retries if missing values are encountered
            time_dim: Name of the time dimension
        """
        self.missing_check_vars = missing_check_vars
        self.retries_on_missing = retries_on_missing
        self.time_dim = time_dim
        self.retried_on_missing = 0
        
    def has_missing(
        self,
        zarr_path: Union[str, Any],
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
        if not self.missing_check_vars:
            logger.warning("No vars specified for checking for missing values")
            return False
        
        try:
            # Open existing Zarr store
            with xr.open_zarr(str(zarr_path), consolidated=True) as store_dset:
                # Determine variables to check
                missing_check_vars = self.missing_check_vars
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
    
    def handle_missing_data(
        self,
        zarr_path: Union[str, Any],
        input_dataset: xr.Dataset,
        region: Optional[Dict[str, slice]] = None,
        write_func: Optional[Callable] = None,
        **kwargs
    ) -> bool:
        """
        Handle missing data with retry logic.
        
        Args:
            zarr_path: Path to Zarr store
            input_dataset: Input dataset that was written
            region: Region that was written to
            write_func: Function to call for retry
            **kwargs: Additional arguments for write_func
            
        Returns:
            True if successful, False if retry limit exceeded
        """
        if self.has_missing(zarr_path, input_dataset, region):
            logger.info("Missing data detected - rewriting region")
            if self.retried_on_missing < self.retries_on_missing:
                self.retried_on_missing += 1
                logger.info(f"Retry {self.retried_on_missing}/{self.retries_on_missing}")
                
                # Wait a bit before retry to allow system to stabilize
                import time
                time.sleep(0.1 * self.retried_on_missing)
                
                # Retry the write operation
                if write_func is not None:
                    try:
                        write_func(**kwargs)
                        # Recursively check for missing data after retry
                        return self.handle_missing_data(
                            zarr_path, input_dataset, region, write_func, **kwargs
                        )
                    except Exception as e:
                        logger.error(f"Retry failed: {e}")
                        return False
                else:
                    return False
            else:
                logger.error(
                    f"Missing data present, retry limit exceeded after "
                    f"{self.retried_on_missing} retries"
                )
                return False
        else:
            # Success - no missing data
            self.retried_on_missing = 0  # Reset for next operation
            return True
    
    def reset_retry_count(self) -> None:
        """Reset the retry counter."""
        self.retried_on_missing = 0


# Convenience functions
def check_missing_data(
    zarr_path: Union[str, Any],
    input_dataset: xr.Dataset,
    missing_check_vars: Optional[Union[str, List[str]]] = "all",
    region: Optional[Dict[str, slice]] = None,
    time_dim: str = "time"
) -> bool:
    """
    Check for missing data in a Zarr store.
    
    Args:
        zarr_path: Path to Zarr store
        input_dataset: Input dataset that was written
        missing_check_vars: Variables to check for missing values
        region: Region that was written to
        time_dim: Name of the time dimension
        
    Returns:
        True if missing data is detected, False otherwise
    """
    handler = MissingDataHandler(
        missing_check_vars=missing_check_vars,
        time_dim=time_dim
    )
    return handler.has_missing(zarr_path, input_dataset, region)


def handle_missing_with_retry(
    zarr_path: Union[str, Any],
    input_dataset: xr.Dataset,
    write_func: Callable,
    missing_check_vars: Optional[Union[str, List[str]]] = "all",
    retries_on_missing: int = 0,
    region: Optional[Dict[str, slice]] = None,
    time_dim: str = "time",
    **kwargs
) -> bool:
    """
    Handle missing data with retry logic.
    
    Args:
        zarr_path: Path to Zarr store
        input_dataset: Input dataset that was written
        write_func: Function to call for retry
        missing_check_vars: Variables to check for missing values
        retries_on_missing: Number of retries if missing values are encountered
        region: Region that was written to
        time_dim: Name of the time dimension
        **kwargs: Additional arguments for write_func
        
    Returns:
        True if successful, False if retry limit exceeded
    """
    handler = MissingDataHandler(
        missing_check_vars=missing_check_vars,
        retries_on_missing=retries_on_missing,
        time_dim=time_dim
    )
    return handler.handle_missing_data(
        zarr_path, input_dataset, region, write_func, **kwargs
    )