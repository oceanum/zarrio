# Decisions
- Chose an abstract base class RollingArchiveBackend to define a minimal, pluggable API for rolling archive backends.
- Interfaces reflect operations used by core: enumerate_groups, get_group_timestamp (with a time_reference_attr key), delete_groups (with dry_run toggle), and backend_type.
- Default time_reference_attr is set to "cycle_time" to align with typical NetCDF time referencing in rolling archives.
