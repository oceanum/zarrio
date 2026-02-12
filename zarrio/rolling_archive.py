from __future__ import annotations

from abc import ABC, abstractmethod
from datetime import datetime
from typing import List, Dict, Optional


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
