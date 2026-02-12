# Rolling Archive Feature - Work Plan (Datamesh-First)

## TL;DR

**Quick Summary**: Implement automatic cleanup of old Zarr groups in datamesh based on time-based retention windows. When new data is written to a group, groups older than the configured retention window are automatically removed via the ZarrClient interface (which handles HTTP internally).

**Deliverables**:
- Abstract `RollingArchiveBackend` interface
- `DatameshRollingArchiveBackend` for Oceanum datamesh
- `FileRollingArchiveBackend` for file-based Zarr (secondary)
- `RollingArchiveConfig` Pydantic model
- `cleanup_archive()` method with auto-backend selection
- Automatic cleanup integration with `convert()`, `append()`, `write_region()`
- CLI support via `--rolling-archive-hours` flag
- Comprehensive test coverage (unit + integration)
- Documentation and examples

**Estimated Effort**: Large (6-7 tasks)
**Parallel Execution**: YES - Tasks 1, 2, and 3 can run in parallel
**Critical Path**: Task 4 (datamesh backend) → Task 5 (core integration) → Task 6 (CLI) → Tasks 7-8 (testing)

---

## Context

### Original Request
Implement rolling archive management for datamesh groups. Nominally using groups for forecast cycles (dates). Example: keep a one day rolling archive with four cycles per day - when a new cycle is written, the oldest cycle is removed.

### CRITICAL: Datamesh is Primary Target

This is **not** a file-based Zarr feature with datamesh as an afterthought. The primary use case is managing forecast cycles in Oceanum datamesh via their API.

### Architecture: Backend Abstraction

```
┌─────────────────────────────────────────────────────────────────┐
│                    ZarrConverter                                 │
├─────────────────────────────────────────────────────────────────┤
│  Config: ZarrConverterConfig                                     │
│    └── rolling_archive: RollingArchiveConfig                    │
│         ├── enabled: bool                                       │
│         ├── retention_window: timedelta                         │
│         ├── time_reference_attr: str                            │
│         ├── auto_cleanup: bool                                  │
│         └── min_groups_to_keep: int                             │
├─────────────────────────────────────────────────────────────────┤
│  Rolling Archive System:                                         │
│    ├── RollingArchiveBackend (abstract interface)               │
│    │   ├── enumerate_groups() -> List[str]                      │
│    │   ├── get_group_timestamp(group) -> datetime               │
│    │   └── delete_groups(groups) -> None                        │
│    │                                                            │
│    ├── DatameshRollingArchiveBackend                            │
│    │   ├── Uses ZarrClient.__iter__ to list groups              │
│    │   ├── Uses ZarrClient.__delitem__ to delete groups         │
│    │   └── Extracts timestamps from group attributes            │
│    │                                                            │
│    └── FileRollingArchiveBackend (secondary)                    │
│        ├── Uses zarr/zarr storage API                           │
│        ├── Extracts timestamps from attrs/names                 │
│        └── Local filesystem operations                          │
├─────────────────────────────────────────────────────────────────┤
│  Methods:                                                        │
│    ├── convert() → _cleanup_if_enabled()                        │
│    ├── append() → _cleanup_if_enabled()                         │
│    ├── write_region() → _cleanup_if_enabled()                   │
│    └── cleanup_archive(dry_run=False)                           │
└─────────────────────────────────────────────────────────────────┘
```

### Design Decisions

| Decision | Choice | Rationale |
|----------|--------|-----------|
| **Primary target** | **Datamesh** | This is the key use case per user |
| **Secondary target** | File-based Zarr | Included for completeness and testing |
| **Time-based vs Count-based** | Time-based | Handles missing cycles naturally |
| **Strict window vs Count** | Strict time window | Consistent temporal coverage |
| **Backend selection** | Auto-detect from output type | Seamless UX |
| **CLI vs Config** | Both | Consistent with existing zarrio patterns |
| **Concurrent writes** | External coordination | Document clearly for datamesh |
| **Time parsing failure** | Skip with warning | Safe default, don't lose data |
| **Cleanup trigger** | Automatic on write | Configurable, can be disabled |

---

## Work Objectives

### Core Objective
Add automatic rolling archive management to zarrio that removes old datamesh groups via the ZarrClient interface (which handles HTTP internally) based on time-based retention windows, ensuring archives don't grow unbounded while maintaining consistent temporal coverage.

### Concrete Deliverables
1. `zarrio/rolling_archive.py`: Backend abstraction and implementations
2. `zarrio/models.py`: `RollingArchiveConfig` Pydantic model
3. `zarrio/core.py`: `cleanup_archive()` method and integration points
4. `zarrio/cli.py`: `--rolling-archive-hours` CLI flag
5. `tests/test_rolling_archive.py`: Unit tests with mocked backends
6. `tests/test_rolling_archive_datamesh.py`: Datamesh integration tests
7. `tests/test_rolling_archive_file.py`: File-based integration tests
8. Documentation and examples

### Definition of Done
- [ ] All TODOs complete with passing tests
- [ ] `pytest tests/test_rolling_archive*.py` passes
- [ ] Datamesh integration tested (or properly mocked)
- [ ] CLI integration tested end-to-end
- [ ] Documentation complete with examples
- [ ] No breaking changes to existing API

### Must Have
- ✅ **Datamesh primary support** via Oceanum API
- Time-based group retention (configurable window)
- Automatic cleanup on write operations (configurable)
- Manual cleanup via `cleanup_archive()` method
- Dry run mode for safety testing
- Backend auto-detection (datamesh vs file)
- CLI support for all operations
- Skip groups with unparseable timestamps (with warning)
- Minimum groups safeguard (always keep at least N groups)
- Network error handling with retry logic

### Must NOT Have (Guardrails)
- ❌ **NO complex retention policies** per datasource
- ❌ **NO compression/cold storage** - Delete only
- ❌ **NO undo/restore** - Out of scope
- ❌ **NO cron scheduling** - Trigger-based only
- ❌ **NO concurrent write safety** - Document user responsibility for datamesh

---

## Verification Strategy

### Test Decision
- **Infrastructure exists**: YES - pytest already configured
- **Automated tests**: Tests after implementation
- **Framework**: pytest with existing fixtures
- **Datamesh Testing**: Mock datamesh API responses (can't require live service)

### Agent-Executed QA Scenarios

Every task includes automated verification via bash commands and pytest assertions. No human intervention required.

---

## Execution Strategy

### Parallel Execution Waves

```
Wave 1 (Start Immediately - No Dependencies):
├── Task 1: Backend abstraction interface
│   └── RollingArchiveBackend abstract base class
├── Task 2: Configuration model
│   └── RollingArchiveConfig, validation, defaults
└── Task 3: Time parsing utilities
    └── Flexible time parsing from names and attributes

Wave 2 (Depends on Wave 1):
├── Task 4: Datamesh backend (PRIMARY)
│   └── DatameshRollingArchiveBackend with API integration
└── Task 5: File backend (SECONDARY)
    └── FileRollingArchiveBackend for file-based Zarr

Wave 3 (Depends on Wave 2):
├── Task 6: Core integration and auto-detection
│   └── cleanup_archive(), _cleanup_if_enabled(), backend selection
└── Task 7: CLI integration
    └── --rolling-archive-hours flag, config propagation

Wave 4 (Depends on Wave 3):
├── Task 8: Unit tests (mocked backends)
│   └── test_rolling_archive.py
├── Task 9: Datamesh integration tests
│   └── test_rolling_archive_datamesh.py (mocked API)
└── Task 10: File-based integration tests
    └── test_rolling_archive_file.py (real Zarr stores)

Wave 5 (Final):
└── Task 11: Documentation and examples
    └── Docstrings, CLI help, example scripts, README

Critical Path: 1/2/3 → 4/5 → 6/7 → 8/9/10 → 11
```

### Dependency Matrix

| Task | Depends On | Blocks | Can Parallelize With |
|------|------------|--------|---------------------|
| 1 (Backend interface) | None | 4, 5 | 2, 3 |
| 2 (Config model) | None | 6, 7 | 1, 3 |
| 3 (Utilities) | None | 4, 5 | 1, 2 |
| 4 (Datamesh backend) | 1, 3 | 6 | 5 |
| 5 (File backend) | 1, 3 | 6 | 4 |
| 6 (Core integration) | 1, 2, 4, 5 | 7, 8, 9, 10 | None |
| 7 (CLI) | 2, 6 | 11 | None |
| 8 (Unit tests) | 1, 2, 3 | None | 9, 10 |
| 9 (Datamesh tests) | 4, 6 | None | 8, 10 |
| 10 (File tests) | 5, 6 | None | 8, 9 |
| 11 (Docs) | 7 | None | None |

### Agent Dispatch Summary

| Wave | Tasks | Recommended Agents |
|------|-------|-------------------|
| 1 | 1, 2, 3 | task(category="unspecified-high", load_skills=[], run_in_background=false) |
| 2 | 4, 5 | task(category="ultrabrain", load_skills=[], run_in_background=false) |
| 3 | 6, 7 | task(category="unspecified-high", load_skills=[], run_in_background=false) |
| 4 | 8, 9, 10 | task(category="unspecified-high", load_skills=[], run_in_background=false) |
| 5 | 11 | task(category="writing", load_skills=[], run_in_background=false) |

---

## TODOs

### Task 1: Backend Abstraction Interface ✅

**Status**: COMPLETED

**What to do**:
Create the abstract base class for rolling archive backends in new file `zarrio/rolling_archive.py`.

```python
from abc import ABC, abstractmethod
from datetime import datetime
from typing import List, Dict, Optional, Union
from pathlib import Path

class RollingArchiveBackend(ABC):
    """
    Abstract base class for rolling archive backends.
    
    Implementations handle group enumeration, timestamp extraction,
    and deletion for different storage backends (datamesh, filesystem).
    """
    
    @abstractmethod
    def enumerate_groups(self) -> List[str]:
        """
        List all groups in the archive.
        
        Returns:
            List of group paths (e.g., ['cycle/20240101T000000', ...])
        """
        pass
    
    @abstractmethod
    def get_group_timestamp(
        self, 
        group: str,
        time_reference_attr: str = "cycle_time"
    ) -> Optional[datetime]:
        """
        Extract timestamp for a group.
        
        Args:
            group: Group path
            time_reference_attr: Attribute name containing timestamp
            
        Returns:
            Parsed datetime or None if unparseable
        """
        pass
    
    @abstractmethod
    def delete_groups(self, groups: List[str], dry_run: bool = False) -> Dict[str, List[str]]:
        """
        Delete groups from the archive.
        
        Args:
            groups: List of group paths to delete
            dry_run: If True, don't actually delete
            
        Returns:
            Dict with 'deleted', 'failed' lists
        """
        pass
    
    @property
    @abstractmethod
    def backend_type(self) -> str:
        """Return backend type identifier."""
        pass
```

**Must NOT do**:
- Don't implement concrete logic here (just interface)
- Don't add backend-specific imports

**Recommended Agent Profile**:
- **Category**: `quick`
- **Reason**: Abstract base class with clear interface definition
- **Skills**: None required

**Parallelization**:
- **Can Run In Parallel**: YES
- **Parallel Group**: Wave 1
- **Blocks**: Tasks 4, 5
- **Blocked By**: None

**References**:
- Python `abc` module documentation
- `zarrio/core.py:_get_store()` - How backends are currently selected
- Existing datamesh integration patterns in `core.py`

**Acceptance Criteria**:

- [x] Abstract base class defined with all abstract methods
- [x] Type hints complete
- [x] Docstrings follow Google style
- [x] Can be imported without errors

**Agent-Executed QA Scenarios**:

```
Scenario: Backend interface is importable
  Tool: Bash (Python)
  Steps:
    1. Run: cd /home/tdurrant/source/zarrio && python -c "from zarrio.rolling_archive import RollingArchiveBackend; print('OK')"
    2. Assert: Exit code 0
    3. Assert: Output shows "OK"
  Expected Result: Module imports successfully
  Evidence: Python output captured
```

**Commit**: YES
- Message: `feat(rolling): Add RollingArchiveBackend abstract interface`
- Files: `zarrio/rolling_archive.py`
- Pre-commit: `python -c "from zarrio.rolling_archive import RollingArchiveBackend"`

---

### Task 2: Configuration Model ✅

**Status**: COMPLETED

**What to do**:
Add `RollingArchiveConfig` Pydantic model to `zarrio/models.py` and integrate into `ZarrConverterConfig`.

```python
from datetime import timedelta
from typing import Optional

class RollingArchiveConfig(BaseModel):
    """Configuration for rolling archive management."""
    
    enabled: bool = Field(
        False, 
        description="Enable automatic rolling archive cleanup"
    )
    retention_window: Optional[timedelta] = Field(
        None,
        description="Retention window (e.g., timedelta(hours=24))"
    )
    time_reference_attr: str = Field(
        "cycle_time",
        description="Attribute or metadata field containing timestamp"
    )
    auto_cleanup: bool = Field(
        True,
        description="Automatically cleanup on write operations"
    )
    min_groups_to_keep: int = Field(
        1,
        description="Minimum number of groups to preserve",
        ge=0
    )
    
    @field_validator("retention_window")
    @classmethod
    def validate_retention_window(cls, v: Optional[timedelta]) -> Optional[timedelta]:
        if v is not None and v.total_seconds() < 3600:  # Minimum 1 hour
            raise ValueError("retention_window must be at least 1 hour")
        return v
```

Update `ZarrConverterConfig`:
```python
rolling_archive: RollingArchiveConfig = Field(
    default_factory=RollingArchiveConfig,
    description="Rolling archive configuration"
)
```

**Validation Rules**:
- If `enabled=True`, `retention_window` must be set
- `retention_window` must be >= 1 hour
- `min_groups_to_keep` must be >= 0

**Must NOT do**:
- Don't add backend-specific configuration here

**Recommended Agent Profile**:
- **Category**: `quick`
- **Reason**: Pydantic model following existing patterns
- **Skills**: None required

**Parallelization**:
- **Can Run In Parallel**: YES
- **Parallel Group**: Wave 1
- **Blocks**: Tasks 6, 7
- **Blocked By**: None

**References**:
- `zarrio/models.py:ChunkingConfig` - Example config model
- `zarrio/models.py:PackingConfig` - Example with validators

**Acceptance Criteria**:

- [x] `RollingArchiveConfig` model defined with all fields
- [x] Field validators implemented for constraints
- [x] Integrated into `ZarrConverterConfig`
- [x] YAML/JSON serialization works correctly

**Agent-Executed QA Scenarios**:

```
Scenario: Config model validates correctly
  Tool: Bash (pytest)
  Steps:
    1. Run: pytest tests/test_rolling_archive.py::TestRollingArchiveConfig -xvs
    2. Assert: Exit code 0
  Expected Result: All config validation tests pass
  Evidence: Test output captured
```

**Commit**: YES
- Message: `feat(models): Add RollingArchiveConfig for archive management`
- Files: `zarrio/models.py`
- Pre-commit: `mypy zarrio/models.py`

---

### Task 3: Time Parsing Utilities ✅

**Status**: COMPLETED

**What to do**:
Create robust time parsing utilities in new file `zarrio/time_parsing.py`.

The module should handle:
1. Parsing ISO8601 timestamps from strings
2. Extracting timestamps from group names
3. Reading timestamps from metadata (Zarr attrs or datamesh metadata)
4. Multiple format support with fallbacks

Functions to implement:
```python
def parse_timestamp_from_string(
    value: str,
    formats: Optional[List[str]] = None
) -> Optional[datetime]:
    """Parse timestamp from string using multiple format attempts."""

def extract_timestamp_from_group_name(group_path: str) -> Optional[datetime]:
    """
    Extract timestamp from the last segment of a group path.
    Example: "cycle/20240101T000000" -> datetime(2024, 1, 1, 0, 0, 0)
    """

def parse_with_dateutil(value: str) -> Optional[datetime]:
    """Fallback parsing using dateutil parser."""
```

**Supported Formats**:
```python
DEFAULT_TIMESTAMP_FORMATS = [
    "%Y%m%dT%H%M%S",      # 20240101T000000 (compact ISO)
    "%Y-%m-%dT%H:%M:%S",  # 2024-01-01T00:00:00 (standard ISO)
    "%Y%m%d%H%M%S",       # 20240101000000 (no separator)
]
```

**Must NOT do**:
- Don't add Zarr/datamesh-specific I/O here (just string parsing)

**Recommended Agent Profile**:
- **Category**: `quick`
- **Reason**: Utility functions
- **Skills**: None required

**Parallelization**:
- **Can Run In Parallel**: YES
- **Parallel Group**: Wave 1
- **Blocks**: Tasks 4, 5
- **Blocked By**: None

**References**:
- Python `datetime.strptime()` documentation
- `dateutil.parser` for fuzzy parsing

**Acceptance Criteria**:

- [x] All parsing functions implemented
- [x] Handles all common ISO8601 formats
- [x] Gracefully handles unparseable strings (returns None)
- [ ] Unit tests for all parsing scenarios

**Commit**: YES
- Message: `feat(time): Add timestamp parsing utilities for rolling archive`
- Files: `zarrio/time_parsing.py`
- Pre-commit: `pytest tests/test_rolling_archive.py::TestTimeParsing -x`

---

### Task 4: Datamesh Backend (PRIMARY)

**What to do**:
Implement `DatameshRollingArchiveBackend` in `zarrio/rolling_archive.py`.

```python
class DatameshRollingArchiveBackend(RollingArchiveBackend):
    """
    Rolling archive backend for Oceanum datamesh.
    
    Uses ZarrClient's MutableMapping interface to enumerate and delete groups.
    No direct HTTP calls needed - ZarrClient handles all HTTP internally.
    """
    
    def __init__(self, zarr_client: ZarrClient):
        """
        Initialize with existing ZarrClient instance.
        
        Args:
            zarr_client: ZarrClient from _get_store() or existing session
        """
        self.zarr_client = zarr_client
    
    def enumerate_groups(self) -> List[str]:
        """
        List all groups in the datamesh datasource.
        
        Uses ZarrClient.__iter__ which makes HTTP GET requests internally.
        """
        # Implementation: list(self.zarr_client)
    
    def get_group_timestamp(
        self,
        group: str,
        time_reference_attr: str = "cycle_time"
    ) -> Optional[datetime]:
        """
        Get timestamp for a datamesh group.
        
        Strategy:
        1. Try to parse from group name (last segment)
        2. Open group via ZarrClient and read attribute
        3. Parse attribute as datetime
        """
        # Implementation using zarr_client to read group attrs
    
    def delete_groups(
        self,
        groups: List[str],
        dry_run: bool = False
    ) -> Dict[str, List[str]]:
        """
        Delete groups from datamesh.
        
        Uses ZarrClient.__delitem__ which makes HTTP DELETE requests internally.
        ZarrClient handles retry logic automatically.
        
        Args:
            groups: List of group paths to delete
            dry_run: If True, don't actually delete
        
        Returns:
            Dict with 'deleted', 'failed' lists
        """
        # Implementation: del self.zarr_client[group]
    
    @property
    def backend_type(self) -> str:
        return "datamesh"
```

**Implementation Notes**:
- **NO direct HTTP calls** - Use ZarrClient's MutableMapping interface
- **Group enumeration**: `list(zarr_client)` uses `__iter__` → HTTP GET
- **Group deletion**: `del zarr_client[group]` uses `__delitem__` → HTTP DELETE
- **Retry logic**: Already built into ZarrClient (retried_request)
- **Authentication**: Already handled by ZarrClient (uses connection's auth headers)
- **Session management**: Reuse existing session from `_get_store()`

**Must NOT do**:
- Don't use `requests.get/post/delete` directly
- Don't create new ZarrClient instances (reuse from `_get_store()`)
- Don't implement custom retry logic (ZarrClient has it built-in)

**Recommended Agent Profile**:
- **Category**: `ultrabrain`
- **Reason**: Complex API integration with error handling
- **Skills**: None required

**Parallelization**:
- **Can Run In Parallel**: YES
- **Parallel Group**: Wave 2
- **Blocks**: Task 6
- **Blocked By**: Tasks 1, 3

**References**:
- `zarrio/core.py:conn` - Existing datamesh connector
- `zarrio/core.py:_get_store()` - Session management
- `test_hierarchical_groups_datamesh.py` - Datamesh usage examples
- Oceanum datamesh API documentation (check codebase for patterns)

**Acceptance Criteria**:

- [ ] `DatameshRollingArchiveBackend` implements all abstract methods
- [ ] Uses existing datamesh connection/session
- [ ] Implements retry logic for network failures
- [ ] Handles API errors gracefully
- [ ] Logs all operations

**Agent-Executed QA Scenarios**:

```
Scenario: Datamesh backend enumerates groups via ZarrClient
  Tool: Bash (pytest)
  Steps:
    1. Create mock ZarrClient with __iter__ returning test groups
    2. Run: pytest tests/test_rolling_archive.py::TestDatameshBackend::test_enumerate_groups -xvs
    3. Assert: Returns correct group list from zarr_client iteration
  Expected Result: Group enumeration works via ZarrClient.__iter__
  Evidence: Test output captured

Scenario: Datamesh backend deletes groups via ZarrClient
  Tool: Bash (pytest)
  Steps:
    1. Create mock ZarrClient tracking __delitem__ calls
    2. Call backend.delete_groups(['cycle/old'])
    3. Assert: zarr_client.__delitem__ called with 'cycle/old'
  Expected Result: Deletion works via ZarrClient.__delitem__
  Evidence: Test output captured

Scenario: ZarrClient retry logic handles network failures
  Tool: Bash (pytest)
  Steps:
    1. Mock ZarrClient to simulate transient failure then success
    2. Call backend.delete_groups(['cycle/old'])
    3. Assert: Operation succeeds after retry
  Expected Result: ZarrClient's built-in retry logic works
  Evidence: Test output captured
```

**Commit**: YES
- Message: `feat(rolling): Add DatameshRollingArchiveBackend for datamesh support`
- Files: `zarrio/rolling_archive.py`
- Pre-commit: `pytest tests/test_rolling_archive.py::TestDatameshBackend -x`

---

### Task 5: File Backend (SECONDARY)

**What to do**:
Implement `FileRollingArchiveBackend` in `zarrio/rolling_archive.py`.

```python
class FileRollingArchiveBackend(RollingArchiveBackend):
    """
    Rolling archive backend for file-based Zarr stores.
    
    Uses zarr storage API to enumerate groups, read attributes,
    and delete groups from local filesystem.
    """
    
    def __init__(self, zarr_path: Union[str, Path]):
        self.zarr_path = Path(zarr_path)
    
    def enumerate_groups(self) -> List[str]:
        """
        List all groups in the Zarr store.
        
        Uses zarr to traverse group hierarchy.
        """
        # Implementation using zarr API
    
    def get_group_timestamp(
        self,
        group: str,
        time_reference_attr: str = "cycle_time"
    ) -> Optional[datetime]:
        """
        Get timestamp for a file-based group.
        
        Strategy:
        1. Try to parse from group name (last segment)
        2. Open group and read attribute
        3. Parse attribute as datetime
        """
        # Implementation
    
    def delete_groups(
        self,
        groups: List[str],
        dry_run: bool = False
    ) -> Dict[str, List[str]]:
        """
        Delete groups from filesystem.
        
        Uses zarr API to remove groups.
        """
        # Implementation
    
    @property
    def backend_type(self) -> str:
        return "file"
```

**Implementation Notes**:
- Use zarr API for group operations
- Handle permission errors gracefully
- Log all operations

**Must NOT do**:
- Don't use filesystem calls directly (use zarr API)

**Recommended Agent Profile**:
- **Category**: `unspecified-high`
- **Reason**: Zarr integration
- **Skills**: None required

**Parallelization**:
- **Can Run In Parallel**: YES
- **Parallel Group**: Wave 2
- **Blocks**: Task 6
- **Blocked By**: Tasks 1, 3

**References**:
- `zarr` library documentation
- `test_hierarchical_groups.py` - File-based Zarr operations

**Acceptance Criteria**:

- [ ] `FileRollingArchiveBackend` implements all abstract methods
- [ ] Uses zarr API for all operations
- [ ] Handles errors gracefully

**Commit**: YES
- Message: `feat(rolling): Add FileRollingArchiveBackend for file-based Zarr`
- Files: `zarrio/rolling_archive.py`
- Pre-commit: `pytest tests/test_rolling_archive.py::TestFileBackend -x`

---

### Task 6: Core Integration and Auto-Detection

**What to do**:
Integrate rolling archive into `ZarrConverter` with backend auto-detection.

Add to `zarrio/core.py`:

```python
def _get_rolling_archive_backend(
    self,
    output_path: Union[str, Path, Any],
    group: Optional[str] = None
) -> Optional[RollingArchiveBackend]:
    """
    Get appropriate backend for rolling archive operations.
    
    Auto-detects backend type based on output:
    - Datamesh: If using datamesh zarr client
    - File: If output is filesystem path
    
    Returns None if rolling archive not enabled/configured.
    """
    if not self.config.rolling_archive.enabled:
        return None
    
    # Detect backend type
    if self.use_datamesh_zarr_client:
        return DatameshRollingArchiveBackend(
            conn=self.conn,
            datasource_id=self.config.datamesh.datasource.id,
            session=self._session
        )
    else:
        return FileRollingArchiveBackend(output_path)

def cleanup_archive(
    self,
    output_path: Union[str, Path, Any],
    dry_run: bool = False
) -> Dict[str, Any]:
    """
    Clean up expired groups from archive.
    
    Args:
        output_path: Path to Zarr store or datamesh datasource
        dry_run: If True, only report what would be deleted
    
    Returns:
        Dict with 'deleted', 'kept', 'skipped', 'failed' lists
    """
    # Implementation using backend

def _cleanup_if_enabled(
    self,
    output_path: Union[str, Path, Any],
    skip_cleanup: bool = False
) -> None:
    """
    Trigger cleanup if rolling archive is enabled and auto_cleanup is True.
    """
    if skip_cleanup:
        return
    
    if not (self.config.rolling_archive.enabled and 
            self.config.rolling_archive.auto_cleanup):
        return
    
    try:
        result = self.cleanup_archive(output_path, dry_run=False)
        logger.info(f"Rolling archive cleanup: {result}")
    except Exception as e:
        logger.warning(f"Rolling archive cleanup failed: {e}")
```

Modify `convert()`, `append()`, `write_region()` to:
- Add `skip_cleanup: bool = False` parameter
- Call `_cleanup_if_enabled()` at end of successful operations

**Must NOT do**:
- Don't cleanup before write
- Don't fail write if cleanup fails

**Recommended Agent Profile**:
- **Category**: `unspecified-high`
- **Reason**: Integration with existing write logic
- **Skills**: None required

**Parallelization**:
- **Can Run In Parallel**: NO
- **Parallel Group**: Wave 3
- **Blocks**: Tasks 7, 8, 9, 10
- **Blocked By**: Tasks 1, 2, 4, 5

**References**:
- `zarrio/core.py:convert()` - Existing method to modify
- `zarrio/core.py:_get_store()` - Backend detection pattern

**Acceptance Criteria**:

- [ ] Auto-detection works for datamesh and file outputs
- [ ] `cleanup_archive()` uses correct backend
- [ ] `_cleanup_if_enabled()` called after writes
- [ ] `skip_cleanup` parameter respected
- [ ] Cleanup failures don't fail primary operation

**Commit**: YES
- Message: `feat(core): Integrate rolling archive with auto-backend detection`
- Files: `zarrio/core.py`
- Pre-commit: `pytest tests/test_rolling_archive.py::TestCoreIntegration -x`

---

### Task 7: CLI Integration

**What to do**:
Add CLI support for rolling archive configuration.

Add to `zarrio/cli.py`:

1. New argument for `convert`, `append`, `write-region` commands:
```python
parser.add_argument(
    "--rolling-archive-hours",
    type=int,
    default=None,
    help="Enable rolling archive with retention window in hours (e.g., 24 for 1 day)"
)
```

2. Update command handlers to propagate config:
```python
if args.rolling_archive_hours:
    config_dict.setdefault("rolling_archive", {})
    config_dict["rolling_archive"]["enabled"] = True
    config_dict["rolling_archive"]["retention_window"] = timedelta(
        hours=args.rolling_archive_hours
    )
```

**Must NOT do**:
- Don't add standalone `cleanup` command

**Recommended Agent Profile**:
- **Category**: `quick`
- **Reason**: CLI argument addition
- **Skills**: None required

**Parallelization**:
- **Can Run In Parallel**: NO
- **Parallel Group**: Wave 3
- **Blocks**: Task 11
- **Blocked By**: Tasks 2, 6

**Acceptance Criteria**:

- [ ] `--rolling-archive-hours` flag added
- [ ] Flag correctly propagates to config
- [ ] Works with both CLI and --config file

**Commit**: YES
- Message: `feat(cli): Add --rolling-archive-hours flag`
- Files: `zarrio/cli.py`
- Pre-commit: `make test-quick`

---

### Task 8: Unit Tests

**What to do**:
Create comprehensive unit tests in `tests/test_rolling_archive.py`.

Test all components with mocked dependencies:
- Config validation
- Time parsing
- Backend interface
- Datamesh backend (mocked API)
- File backend (mocked zarr)
- Core integration

**Key test cases**:
```python
class TestRollingArchiveConfig:
    def test_default_config_disabled(self)
    def test_enable_requires_retention_window(self)
    def test_validation_rules(self)

class TestTimeParsing:
    def test_parse_iso8601_formats(self)
    def test_parse_unparseable_returns_none(self)

class TestDatameshBackend:
    def test_enumerate_groups(self)
    def test_get_group_timestamp(self)
    def test_delete_groups(self)
    def test_retry_on_network_error(self)

class TestFileBackend:
    def test_enumerate_groups(self)
    def test_get_group_timestamp(self)
    def test_delete_groups(self)

class TestCoreIntegration:
    def test_auto_detect_datamesh_backend(self)
    def test_auto_detect_file_backend(self)
    def test_cleanup_filters_expired_groups(self)
```

**Recommended Agent Profile**:
- **Category**: `unspecified-high`
- **Reason**: Comprehensive testing with mocking
- **Skills**: None required

**Parallelization**:
- **Can Run In Parallel**: YES
- **Parallel Group**: Wave 4
- **Blocks**: None
- **Blocked By**: Tasks 1, 2, 3, 6

**Acceptance Criteria**:

- [ ] All components have unit tests
- [ ] 90%+ code coverage
- [ ] All tests pass

**Commit**: YES
- Message: `test(rolling): Add comprehensive unit tests`
- Files: `tests/test_rolling_archive.py`
- Pre-commit: `pytest tests/test_rolling_archive.py -v`

---

### Task 9: Datamesh Integration Tests

**What to do**:
Create integration tests for datamesh backend in `tests/test_rolling_archive_datamesh.py`.

Tests use mocked datamesh API responses (can't require live service).

**Test scenarios**:
- End-to-end workflow with mocked API
- Network error handling
- Authentication failures
- Group enumeration with pagination
- Timestamp extraction from metadata

**Recommended Agent Profile**:
- **Category**: `unspecified-high`
- **Reason**: Integration testing with API mocking
- **Skills**: None required

**Parallelization**:
- **Can Run In Parallel**: YES
- **Parallel Group**: Wave 4
- **Blocks**: None
- **Blocked By**: Tasks 4, 6

**Acceptance Criteria**:

- [ ] Datamesh backend integration tested
- [ ] API mocking comprehensive
- [ ] Error scenarios covered

**Commit**: YES
- Message: `test(rolling): Add datamesh integration tests`
- Files: `tests/test_rolling_archive_datamesh.py`
- Pre-commit: `pytest tests/test_rolling_archive_datamesh.py -v`

---

### Task 10: File-Based Integration Tests

**What to do**:
Create integration tests for file backend in `tests/test_rolling_archive_file.py`.

Tests use real Zarr stores on filesystem.

**Test scenarios**:
- Realistic forecast cycle scenario
- Multiple groups with various timestamps
- Cleanup with different retention windows
- Dry run verification

**Recommended Agent Profile**:
- **Category**: `unspecified-high`
- **Reason**: Integration testing with real I/O
- **Skills**: None required

**Parallelization**:
- **Can Run In Parallel**: YES
- **Parallel Group**: Wave 4
- **Blocks**: None
- **Blocked By**: Tasks 5, 6

**Acceptance Criteria**:

- [ ] File backend integration tested
- [ ] Real Zarr stores used
- [ ] Cleanup behavior verified

**Commit**: YES
- Message: `test(rolling): Add file-based integration tests`
- Files: `tests/test_rolling_archive_file.py`
- Pre-commit: `pytest tests/test_rolling_archive_file.py -v`

---

### Task 11: Documentation and Examples

**What to do**:
Add comprehensive documentation and examples.

1. **Docstrings**: Update all public methods
2. **CLI help**: Ensure --help shows rolling archive options
3. **Example script**: `examples/rolling_archive_demo.py`
4. **README update**: Add section on rolling archive

**Example script should demonstrate**:
- Datamesh usage (primary)
- File-based usage (secondary)
- Configuration options
- Dry run mode

**Documentation Content**:
- What is rolling archive and when to use it
- Datamesh-specific setup and usage
- Configuration options with examples
- CLI usage examples
- Python API examples
- Best practices for datamesh
- Troubleshooting

**Recommended Agent Profile**:
- **Category**: `writing`
- **Reason**: Documentation and examples
- **Skills**: None required

**Parallelization**:
- **Can Run In Parallel**: NO
- **Parallel Group**: Wave 5 (final)
- **Blocks**: None
- **Blocked By**: Task 7

**Acceptance Criteria**:

- [ ] All public methods documented
- [ ] Example script runs without errors
- [ ] README section added
- [ ] CLI help text clear

**Commit**: YES
- Message: `docs(rolling): Add documentation and examples`
- Files: `examples/rolling_archive_demo.py`, `README.md`, docstrings
- Pre-commit: `python examples/rolling_archive_demo.py`

---

## Commit Strategy

| After Task | Message | Files |
|------------|---------|-------|
| 1 | `feat(rolling): Add RollingArchiveBackend abstract interface` | `zarrio/rolling_archive.py` |
| 2 | `feat(models): Add RollingArchiveConfig` | `zarrio/models.py` |
| 3 | `feat(time): Add timestamp parsing utilities` | `zarrio/time_parsing.py` |
| 4 | `feat(rolling): Add DatameshRollingArchiveBackend` | `zarrio/rolling_archive.py` |
| 5 | `feat(rolling): Add FileRollingArchiveBackend` | `zarrio/rolling_archive.py` |
| 6 | `feat(core): Integrate rolling archive with auto-backend detection` | `zarrio/core.py` |
| 7 | `feat(cli): Add --rolling-archive-hours flag` | `zarrio/cli.py` |
| 8 | `test(rolling): Add unit tests` | `tests/test_rolling_archive.py` |
| 9 | `test(rolling): Add datamesh integration tests` | `tests/test_rolling_archive_datamesh.py` |
| 10 | `test(rolling): Add file-based integration tests` | `tests/test_rolling_archive_file.py` |
| 11 | `docs(rolling): Add documentation and examples` | `examples/`, `README.md` |

---

## Success Criteria

### Verification Commands
```bash
# All tests pass
pytest tests/test_rolling_archive*.py -v

# Code quality checks
make check

# Example runs
python examples/rolling_archive_demo.py

# CLI integration
zarrio convert --help | grep rolling
```

### Final Checklist
- [ ] All 11 TODOs complete with passing tests
- [ ] 90%+ code coverage for new code
- [ ] All code quality checks pass
- [ ] Documentation complete
- [ ] Datamesh backend fully functional (primary target)
- [ ] File backend functional (secondary)
- [ ] No breaking changes to existing API
- [ ] CLI integration tested
- [ ] Configuration file loading tested

### Key Design Principles

1. **Datamesh-First**: Datamesh is the primary target, file-based is secondary
2. **Backend Abstraction**: Clean interface allows future backend types
3. **Auto-Detection**: Backend chosen automatically based on output type
4. **Safe Defaults**: Skip unparseable groups, never fail primary operation
5. **Observable**: All operations logged for debugging

---

## Important Note: Datamesh Implementation

The datamesh backend **does NOT require direct HTTP API calls**. Instead, it leverages the existing **ZarrClient** from the `oceanum.datamesh` library:

### ZarrClient Operations Used

| Operation | ZarrClient Method | HTTP Method (Internal) |
|-----------|-------------------|------------------------|
| **List groups** | `list(zarr_client)` → `__iter__` | GET |
| **Delete group** | `del zarr_client[group]` → `__delitem__` | DELETE |
| **Check existence** | `group in zarr_client` → `__contains__` | HEAD/GET |
| **Read attributes** | `zarr_client[group]` → `__getitem__` | GET |

### Benefits

- **No new HTTP logic**: ZarrClient handles all HTTP internally
- **Built-in retry**: ZarrClient has `retried_request` with exponential backoff
- **Authentication**: Reuses existing connection auth headers
- **Session management**: Works with existing session from `_get_store()`
- **Error handling**: ZarrClient raises `DatameshConnectError`, `DatameshWriteError`

### Implementation Pattern

```python
# Get existing ZarrClient from _get_store()
zarr_client = self._get_store(cycle=cycle, group=group)

# Create backend with existing client
backend = DatameshRollingArchiveBackend(zarr_client)

# Use ZarrClient's MutableMapping interface
groups = backend.enumerate_groups()  # list(zarr_client)
backend.delete_groups(old_groups)     # del zarr_client[group]
```

This approach is **much simpler** than direct HTTP API calls and maintains consistency with existing zarrio datamesh integration.

