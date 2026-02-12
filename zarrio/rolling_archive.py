from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Optional, Union

import zarr


class RollingArchiveBackend(ABC):
    """Abstract base class for rolling archive backends.

    This interface defines the minimal set of operations a rolling archive
    backend must implement in order to be pluggable into the zarrio storage
    layer. Concrete backends (e.g., cloud storage, local on-disk archives,
    etc.) should subclass this and provide backend-specific behavior
    while honoring the documented contract below.
    """

    @abstractmethod
    def enumerate_groups(self) -> List[str]:
        """Return the list of group identifiers present in the archive.

        Each string uniquely identifies a group within the rolling archive.

        Returns:
            A list of group names/identifiers as strings.
        """
        raise NotImplementedError

    @abstractmethod
    def get_group_timestamp(
        self, group: str, time_reference_attr: str = "cycle_time"
    ) -> Optional[datetime]:
        """Extract the timestamp associated with a specific group.

        Implementations may fetch the timestamp from group metadata or derived
        attributes using the provided time_reference_attr as the key. The exact
        semantics are backend-specific, but the contract is that a datetime
        object is returned when a valid timestamp is available, otherwise
        ``None`` is returned.

        Args:
            group: The identifier of the group for which the timestamp should be
                retrieved.
            time_reference_attr: Metadata attribute name (or key) used to obtain
                the timestamp. Defaults to "cycle_time".

        Returns:
            A datetime representing the group timestamp if available, otherwise None.
        """
        raise NotImplementedError

    @abstractmethod
    def delete_groups(
        self, groups: List[str], dry_run: bool = False
    ) -> Dict[str, List[str]]:
        """Delete the specified groups from the archive.

        This method may perform a real deletion or a dry-run depending on the
        ``dry_run`` flag. The return value is a mapping describing the outcome
        of the operation. Typical keys may include but are not limited to
        "deleted" (groups successfully removed) and "not_deleted" (groups that
        could not be removed or were skipped).

        Args:
            groups: A list of group identifiers to delete.
            dry_run: If True, simulate deletion without removing data.

        Returns:
            A dictionary mapping outcome categories to lists of group identifiers.
        """
        raise NotImplementedError

    @property
    @abstractmethod
    def backend_type(self) -> str:
        """Return a human-readable identifier for the backend type."""
        raise NotImplementedError


class FileRollingArchiveBackend(RollingArchiveBackend):
    """Rolling archive backend for file-based Zarr stores."""

    def __init__(self, zarr_path: Union[str, Path]):
        """
        Initialize with path to Zarr store.

        Args:
            zarr_path: Path to Zarr store directory
        """
        self.zarr_path = Path(zarr_path)
        self.logger = logging.getLogger(__name__)

    def enumerate_groups(self) -> List[str]:
        """List all groups in the Zarr store."""
        self.logger.debug(f"Enumerating groups in {self.zarr_path}")

        if not self.zarr_path.exists():
            return []

        try:
            store = zarr.open_group(self.zarr_path, mode="r")
            # Get all groups recursively
            groups = []
            self._collect_groups(store, "", groups)
            return groups
        except Exception as e:
            self.logger.error(f"Failed to enumerate groups: {e}")
            return []

    def _collect_groups(self, group, prefix: str, result: List[str]):
        """Recursively collect all group paths."""
        for name in group.group_keys():
            full_path = f"{prefix}/{name}" if prefix else name
            result.append(full_path)
            # Recurse into subgroups
            subgroup = group[name]
            self._collect_groups(subgroup, full_path, result)

    def get_group_timestamp(
        self, group: str, time_reference_attr: str = "cycle_time"
    ) -> Optional[datetime]:
        """
        Get timestamp for a file-based group.

        Strategy:
        1. Try to parse from group name (last segment)
        2. Open group and read attribute
        3. Parse attribute as datetime
        """
        # First try parsing from group name
        from .time_parsing import extract_timestamp_from_group_name

        timestamp = extract_timestamp_from_group_name(group)
        if timestamp:
            return timestamp

        # Fall back to reading from group attributes
        try:
            store = zarr.open_group(self.zarr_path, mode="r")
            subgroup = store[group]
            attrs = dict(subgroup.attrs)

            if time_reference_attr in attrs:
                timestamp_str = attrs[time_reference_attr]
                # Parse timestamp string
                from .time_parsing import parse_timestamp_from_string

                return parse_timestamp_from_string(timestamp_str)
        except Exception as e:
            self.logger.warning(f"Could not read timestamp for group {group}: {e}")

        return None

    def delete_groups(
        self, groups: List[str], dry_run: bool = False
    ) -> Dict[str, List[str]]:
        """
        Delete groups from filesystem.

        Args:
            groups: List of group paths to delete
            dry_run: If True, don't actually delete

        Returns:
            Dict with 'deleted', 'failed' lists
        """
        result = {"deleted": [], "failed": []}

        if not self.zarr_path.exists():
            self.logger.warning(f"Zarr store does not exist: {self.zarr_path}")
            return result

        try:
            store = zarr.open_group(self.zarr_path, mode="a")
        except Exception as e:
            self.logger.error(f"Failed to open Zarr store: {e}")
            return {"deleted": [], "failed": groups}

        for group in groups:
            if dry_run:
                self.logger.info(f"[DRY RUN] Would delete group: {group}")
                result["deleted"].append(group)
                continue

            try:
                self.logger.info(f"Deleting group: {group}")
                if group in store:
                    del store[group]
                    result["deleted"].append(group)
                else:
                    self.logger.warning(f"Group not found: {group}")
                    result["failed"].append(group)
            except Exception as e:
                self.logger.error(f"Failed to delete group {group}: {e}")
                result["failed"].append(group)

        return result

    @property
    def backend_type(self) -> str:
        return "file"


class DatameshRollingArchiveBackend(RollingArchiveBackend):
    """Rolling archive backend for Oceanum datamesh using ZarrClient."""

    def __init__(self, zarr_client):
        """
        Initialize with existing ZarrClient instance.

        Args:
            zarr_client: ZarrClient from _get_store() or existing session
        """
        self.zarr_client = zarr_client
        self.logger = logging.getLogger(__name__)

    def enumerate_groups(self) -> List[str]:
        """List all groups using ZarrClient iteration."""
        self.logger.debug("Enumerating groups via ZarrClient")
        return list(self.zarr_client)

    def get_group_timestamp(
        self, group: str, time_reference_attr: str = "cycle_time"
    ) -> Optional[datetime]:
        """
        Get timestamp for a datamesh group.

        Strategy:
        1. Try to parse from group name (last segment)
        2. Open group via ZarrClient and read attribute
        3. Parse attribute as datetime
        """
        # First try parsing from group name
        from .time_parsing import extract_timestamp_from_group_name

        timestamp = extract_timestamp_from_group_name(group)
        if timestamp:
            return timestamp

        # Fall back to reading from group attributes
        try:
            # Read group data via ZarrClient
            group_data = self.zarr_client[group]
            # Note: ZarrClient returns raw bytes, need to parse attrs differently
            # For now, return None if name parsing failed
            self.logger.debug(
                f"Group data retrieved but attribute parsing not implemented for {group}"
            )
            return None
        except Exception as e:
            self.logger.warning(f"Could not read timestamp for group {group}: {e}")
            return None

    def delete_groups(
        self, groups: List[str], dry_run: bool = False
    ) -> Dict[str, List[str]]:
        """
        Delete groups from datamesh.

        Args:
            groups: List of group paths to delete
            dry_run: If True, don't actually delete

        Returns:
            Dict with 'deleted', 'failed' lists
        """
        result = {"deleted": [], "failed": []}

        for group in groups:
            if dry_run:
                self.logger.info(f"[DRY RUN] Would delete group: {group}")
                result["deleted"].append(group)
                continue

            try:
                self.logger.info(f"Deleting group: {group}")
                del self.zarr_client[group]
                result["deleted"].append(group)
            except Exception as e:
                self.logger.error(f"Failed to delete group {group}: {e}")
                result["failed"].append(group)

        return result

    @property
    def backend_type(self) -> str:
        return "datamesh"
