# Learnings
- Implementing an abstract RollingArchiveBackend interface helps decouple core archive logic from backend implementations.
- The interface includes enumerating groups, retrieving per-group timestamps, deleting groups (with dry-run support), and reporting backend type.
- Google-style docstrings and explicit type hints improve usability and static checks for implementers.
- Do not include any concrete logic or backend-specific imports in the abstract base class.

## Task 5: FileRollingArchiveBackend Implementation

### Implementation Details
- Uses `zarr.open_group()` for all store operations (read-only mode for enumeration, append mode for deletion)
- `enumerate_groups()` recursively collects all groups including parent groups (e.g., "cycle", "cycle/20240101T000000")
- Timestamp resolution follows two-step strategy: parse from group name first, fallback to group attributes
- `delete_groups()` uses `del store[group]` to remove groups via zarr API (not filesystem operations)

### Key Patterns
- Lazy imports in methods for time parsing utilities to avoid circular dependencies
- Comprehensive error handling with logging at appropriate levels (debug, info, warning, error)
- Proper dry_run support - logs actions but skips mutations
- Returns structured result dicts with 'deleted' and 'failed' lists for observability

### Dependencies
- zarr library for group operations
- time_parsing module for timestamp extraction
- Python logging for operation tracking
- pathlib.Path for filesystem path handling

### Testing
- All abstract methods implemented and callable
- Verified with real Zarr store: enumeration, timestamp parsing, deletion (dry_run and real)
- Full test suite passes (93 tests)
- Handles edge cases: missing store, nonexistent groups, failed operations

## Task 6: ZarrConverter Integration

Successfully integrated rolling archive cleanup into ZarrConverter class:

### Methods Added
1. `_get_rolling_archive_backend()` - Auto-detects FileRollingArchiveBackend vs DatameshRollingArchiveBackend
   - Returns None if rolling archive disabled
   - Checks `use_datamesh_zarr_client` property for backend selection
   - Uses existing `_get_store()` for datamesh ZarrClient

2. `cleanup_archive()` - Main cleanup orchestration
   - Enumerates groups via backend
   - Parses timestamps (name or attribute)
   - Filters by retention_window
   - Respects min_groups_to_keep safeguard
   - Returns detailed results dict

3. `_cleanup_if_enabled()` - Conditional cleanup trigger
   - Checks `skip_cleanup` parameter
   - Checks `auto_cleanup` config
   - Catches exceptions without failing primary operation

### Modified Methods
- `convert()` - Added `skip_cleanup: bool = False` parameter, calls `_cleanup_if_enabled()` after successful write
- `append()` - Added `skip_cleanup: bool = False` parameter, calls `_cleanup_if_enabled()` after successful write  
- `write_region()` - Added `skip_cleanup: bool = False` parameter, calls `_cleanup_if_enabled()` after successful write

### Design Decisions
- **Import location**: Backend classes imported inside methods to avoid circular imports
- **Cleanup timing**: Called AFTER successful write, before session close
- **Error handling**: Cleanup failures logged as warnings, never fail primary operation
- **Backend reuse**: Datamesh backend reuses existing ZarrClient from `_get_store()`

### Testing
- Verified methods exist and callable
- Verified backend auto-detection (file vs datamesh)
- Verified returns None when disabled
- Verified parameter signatures correct
- All imports work without errors


## Task 8: Unit Tests (test_rolling_archive.py)

### Test Coverage Achievement
- **63 comprehensive unit tests** covering all rolling archive components
- **Config validation**: 13 tests for RollingArchiveConfig edge cases
- **Time parsing**: 15 tests for timestamp extraction and format handling
- **Datamesh backend**: 15 tests with mocked ZarrClient
- **File backend**: 14 tests with temp zarr stores
- **Interface compliance**: 6 tests verifying abstract backend contract

### Mock Strategy for DatameshRollingArchiveBackend
Key insight: ZarrClient behaves as MutableMapping[str, bytes] with specific behaviors:
- `__iter__`: Returns keys including .zgroup markers (e.g., "cycle/20240101T000000/.zgroup")
- `__contains__`: Checks if group path exists (without .zgroup suffix)
- `__getitem__`: Returns raw bytes for .zattrs files (JSON-encoded)
- `__delitem__`: Deletes group (no exception handling in implementation)

### Testing Patterns
1. **Config validation**: Test boundary conditions (1 hour minimum retention, >= 0 min_groups)
2. **Time parsing**: Multiple format attempts with dateutil fallback
3. **Backend mocking**: MagicMock for magic methods (__delitem__, __contains__, __iter__)
4. **File backend**: Real temp directories with actual zarr stores (more integration-style)
5. **Exception propagation**: delete_groups() doesn't catch exceptions - tests expect propagation

### Test Organization
- Section separators for maintainability (620-line file)
- Pytest fixtures for reusable test setup (mock_zarr_client, temp_zarr_store)
- Class-based grouping (TestRollingArchiveConfig, TestTimeParsing, etc.)
- Descriptive docstrings for pytest verbose output

### Edge Cases Covered
- Empty/non-existent paths
- Invalid timestamp formats
- Dry-run mode (no actual deletions)
- Partial failures in batch operations
- Nested group hierarchies
- Custom time_reference_attr parameters

## Task 10: File Backend Integration Tests (2026-02-12)

### Test Coverage Created
- **tests/test_rolling_archive_file.py**: 17 integration tests with real Zarr stores
  - TestFileBackendIntegration: 10 tests for basic backend operations
  - TestRealisticForecastScenarios: 3 tests for operational forecast scenarios
  - TestZarrConverterIntegration: 4 tests for end-to-end integration

### Key Testing Patterns
1. **Fixture isolation**: Used `tempfile.TemporaryDirectory()` context managers in fixtures
2. **Real Zarr operations**: Created actual Zarr groups and verified deletion behavior
3. **Nested groups**: Tested multi-level hierarchies (data/forecast/cycle/timestamp)
4. **Operational scenarios**: 7 days × 4 cycles/day = 28 forecast groups
5. **Dry run validation**: Verified no actual deletion occurs in dry_run mode

### Fixture Design
- `forecast_zarr_store`: 4 forecast cycles with timestamps
- `multi_level_zarr_store`: Nested group structure testing
- `operational_forecast_store`: 28 cycles over 7 days
- `populated_zarr_store`: Mix of old and recent cycles for cleanup testing

### Test Organization
1. Basic operations: enumerate, get_timestamp, delete
2. Error handling: nonexistent groups, partial failures
3. Dry run verification: no actual deletion
4. Realistic scenarios: forecast cycle cleanup, retention policies
5. Integration: Path handling, incremental cleanup

### Code Quality
- Removed agent memo comments per project standards
- Clean LSP diagnostics (no errors)
- Removed unused imports (ZarrConverterConfig, RollingArchiveConfig)
- All tests pass with pytest


## DatameshRollingArchiveBackend Testing (Task 9)

### Mock Design Patterns

**Critical lesson: Special methods must be mocked via class, not instance**
- Cannot assign to `instance.__delitem__` - Python lookups special methods on the class
- Solution: Add `side_effect` parameters to MockZarrClient constructor
- Mock implements MutableMapping interface partially (only required methods)

**MockZarrClient behavior simulation:**
- enumerate_groups() expects `.zgroup` markers, not raw group names
- `__iter__` yields `f"{group}/.zgroup"` for each group
- This matches actual ZarrClient behavior which lists keys including metadata files

### Test Coverage Achieved

**End-to-end workflows** (13 tests):
- Group enumeration (empty, normal, large datasets)
- Group deletion (single, batch, dry-run)
- Network error propagation (connection errors, auth failures)
- Realistic forecast cycle management scenarios

**Timestamp extraction** (7 tests):
- Multiple timestamp formats (compact, ISO, nested paths)
- Unparseable names handled gracefully
- custom time_reference_attr parameter tested

**Edge cases** (9 tests):
- Order preservation in enumeration  
- Special characters in group names
- Deep nesting support
- Empty group list deletion
- Pagination simulation (1000+ groups)

### Implementation Notes

**Error handling behavior:**
- DatameshRollingArchiveBackend does NOT catch exceptions during deletion
- Exceptions propagate to caller (different from FileRollingArchiveBackend)
- Tests verify exception propagation with pytest.raises()

**Selective failure pattern:**
- Use callable side_effect to test partial failures
- Track call_count to verify deletion stopped at failure point
- Verify first group deleted, second raised error

## DatameshRollingArchiveBackend Implementation Notes (2026-02-12)

- `ZarrClient.__iter__()` yields store keys (often including `.../.zgroup` and `.../.zattrs`). Group paths are derived by stripping the `/.zgroup` suffix.
- `get_group_timestamp()` strategy:
  1) parse from group name last segment via `extract_timestamp_from_group_name()`
  2) fallback to reading `f"{group}/.zattrs"` via `zarr_client[...]` (bytes), decoding JSON, and parsing `time_reference_attr` via `parse_timestamp_from_string()`.
- `delete_groups()`:
  - dry-run logs and reports as deleted without mutation
  - existence check uses `group in zarr_client` (so ZarrClient handles remote lookup)
  - deletion uses `del zarr_client[group]` (no direct HTTP)
  - exceptions from ZarrClient are intentionally not swallowed.
