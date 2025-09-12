"""
Time series handling for zarrify.
"""

import logging
from typing import Tuple

import xarray as xr
import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)


class TimeManager:
    """Handles time series operations."""
    
    def __init__(self, time_dim: str = "time"):
        """
        Initialize the TimeManager.
        
        Args:
            time_dim: Name of the time dimension
        """
        self.time_dim = time_dim
    
    def remove_duplicates(self, ds: xr.Dataset) -> xr.Dataset:
        """
        Remove duplicate time values from dataset.
        
        Args:
            ds: Dataset to remove duplicates from
            
        Returns:
            Dataset with duplicates removed
        """
        if self.time_dim not in ds.dims:
            return ds
            
        # Get time coordinates
        times = ds[self.time_dim].values
        
        # Find unique times
        unique_times, indices = np.unique(times, return_index=True)
        
        # If no duplicates, return original dataset
        if len(unique_times) == len(times):
            return ds
            
        # Log duplicate removal
        num_duplicates = len(times) - len(unique_times)
        logger.info(f"Removed {num_duplicates} duplicate time entries")
        
        # Select unique time entries
        return ds.isel({self.time_dim: indices})
    
    def get_time_bounds(self, ds: xr.Dataset) -> Tuple[np.datetime64, np.datetime64]:
        """
        Get the start and end times from a dataset.
        
        Args:
            ds: Dataset to get time bounds from
            
        Returns:
            Tuple of (start_time, end_time)
        """
        if self.time_dim not in ds.dims:
            raise ValueError(f"Time dimension '{self.time_dim}' not found in dataset")
            
        times = ds[self.time_dim].to_index()
        return times[0], times[-1]
    
    def align_for_append(self, existing_ds: xr.Dataset, new_ds: xr.Dataset) -> xr.Dataset:
        """
        Align a new dataset with an existing dataset for appending.
        
        Args:
            existing_ds: Existing dataset
            new_ds: New dataset to append
            
        Returns:
            Aligned new dataset
        """
        if self.time_dim not in existing_ds.dims or self.time_dim not in new_ds.dims:
            return new_ds
            
        # Get time bounds
        existing_start, existing_end = self.get_time_bounds(existing_ds)
        new_start, new_end = self.get_time_bounds(new_ds)
        
        # Check for overlap
        if new_start <= existing_end and new_end >= existing_start:
            logger.info("Datasets overlap, selecting non-overlapping portion")
            
            # Select only the portion of new data that extends beyond existing data
            if new_end > existing_end:
                # New data extends beyond existing data
                # Select data from after the existing end time
                new_ds = new_ds.sel({self.time_dim: slice(existing_end, None)})
                # But we need to exclude the existing_end point if it exists in new data
                # to avoid duplicates
                if new_start <= existing_end:
                    # Find the first time in new data that is after existing_end
                    new_times = new_ds[self.time_dim].values
                    # Find first time that is strictly after existing_end
                    after_existing = new_times[new_times > existing_end]
                    if len(after_existing) > 0:
                        # Select only data from the first time after existing_end
                        new_ds = new_ds.sel({self.time_dim: slice(after_existing[0], None)})
                    else:
                        # No data after existing_end, return empty dataset
                        return new_ds.isel({self.time_dim: []})
            else:
                # New data is completely within existing data, nothing to append
                logger.warning("New data is within existing time range, nothing to append")
                return new_ds.isel({self.time_dim: []})  # Return empty dataset
        
        return new_ds
    
    def interpolate_irregular_times(self, ds: xr.Dataset) -> xr.Dataset:
        """
        Interpolate irregular time steps to regular intervals.
        
        Args:
            ds: Dataset with potentially irregular time steps
            
        Returns:
            Dataset with regular time steps
        """
        if self.time_dim not in ds.dims:
            return ds
            
        times = ds[self.time_dim].values
        
        # Check if times are irregular
        if len(times) < 2:
            return ds
            
        # Calculate time differences
        time_diffs = np.diff(times)
        unique_diffs = np.unique(time_diffs)
        
        # If regular intervals, return as is
        if len(unique_diffs) == 1:
            return ds
            
        logger.info("Interpolating irregular time steps")
        
        # Determine regular interval from first two time points
        interval = unique_diffs[0] if len(unique_diffs) > 0 else time_diffs[0]
        
        # Create regular time grid
        start_time = times[0]
        end_time = times[-1]
        regular_times = np.arange(start_time, end_time + interval/2, interval)
        
        # Interpolate to regular grid
        return ds.interp({self.time_dim: regular_times}, method="linear")
    
    def add_time_attributes(self, ds: xr.Dataset) -> xr.Dataset:
        """
        Add time-related attributes to dataset.
        
        Args:
            ds: Dataset to add attributes to
            
        Returns:
            Dataset with added attributes
        """
        ds = ds.copy()
        
        if self.time_dim in ds.dims:
            start_time, end_time = self.get_time_bounds(ds)
            ds.attrs["time_coverage_start"] = str(start_time)
            ds.attrs["time_coverage_end"] = str(end_time)
            
        return ds