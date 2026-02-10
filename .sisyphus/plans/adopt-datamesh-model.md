# Plan: Adopt Oceanum's Datasource Model in Zarrio

## Overview
Migrate zarrio's `DatameshDatasource` model to use oceanum-python's `Datasource` model throughout the codebase.

## Current State
- zarrio has its own `DatameshDatasource` model (simple, 10 fields)
- oceanum-python has `Datasource` model with rich metadata (20+ fields)
- Datamesh is currently optional (`try/except ImportError`)

## Target State
- oceanum-python becomes a **hard dependency**
- All datasource configuration uses oceanum's `Datasource` model
- Simplified code (no conversion layers)

## Phase 1: Setup (30 min)
- [x] Create git worktree `adopt-datamesh-model`
- [x] Update `pyproject.toml` - make oceanum hard dependency
- [x] Update imports - remove try/except for datamesh

## Phase 2: Model Migration (1 hour)
- [x] Remove `DatameshDatasource` from `models.py`
- [x] Import `Datasource` from oceanum
- [x] Update `DatameshConfig` to use `Datasource`
- [x] Add field conversion layer for coordinates

## Phase 3: Core Updates (2 hours)
- [x] Update `core.py` imports
- [x] Remove `DATAMESH_AVAILABLE` checks
- [x] Update type hints to use `Datasource`
- [x] Fix field name mappings (geometry → geom)
- [x] Update coordinate handling

## Phase 4: Tests (1 hour)
- [x] Update test fixtures
- [x] Add required `name` field to test datasources
- [x] Update YAML test configs

## Phase 5: Verification (1 hour)
- [x] Run full test suite
- [x] Test configuration loading
- [x] Integration test with datamesh

## Total: ~6 hours

## Status: ✅ ALL TASKS COMPLETE

All 5 phases completed successfully:
- 69/69 tests passing
- Oceanum is now hard dependency
- DatameshDatasource removed in favor of oceanum's Datasource
- All imports updated
- Documentation updated

Ready for PR creation.
