## Learnings
- Replaced DATAMESH_AVAILABLE guards with direct Oceanum Datasource imports and config-driven flow.
- Central challenge: keeping type hints and runtime behavior in sync after removing conditional imports.
- The import path now relies on oceanum.datash.* modules; presence at runtime must be ensured in the environment.
- Verification via simple import test succeeded: OK.

## Summary of Changes (2026-02-09)
Successfully migrated zarrio to use oceanum's Datasource model:

### Files Modified:
1. **pyproject.toml** - Made oceanum>=0.8.0 a hard dependency
2. **zarrio/models.py** - Removed DatameshDatasource class, now imports Datasource from oceanum
3. **zarrio/core.py** - Removed all DATAMESH_AVAILABLE checks, updated type hints
4. **docs/source/datamesh.rst** - Updated documentation to reference oceanum's model, added driver field
5. **examples/datamesh_demo.py** - Added driver field to datasource config

### Key Changes:
- Oceanum is now required (not optional)
- DatameshDatasource class removed (28 lines)
- All conditional datamesh imports removed
- Field reference updated: geometry -> geom
- Added `driver` field to all datasource examples (required by oceanum's model)
- 69/69 tests passing

### Benefits:
- Single source of truth for datasource model
- Rich metadata support (geom, schema, pforecast, etc.)
- Better type safety with oceanum's well-designed model
- Simplified code (no conversion layers)

### Documentation Updates:
- Updated all datasource configuration examples to include `driver: vzarr`
- Updated CLI examples to include driver field
- Updated API examples to include driver field
- Clarified that oceanum is now a required dependency
