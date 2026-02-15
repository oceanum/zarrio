# zarrio

A modern, clean library for converting scientific data formats to Zarr format.

## Overview

zarrio is a rewrite of the original onzarr library with a focus on simplicity, performance, and maintainability. It leverages modern xarray and zarr capabilities to provide efficient conversion of NetCDF and other scientific data formats to Zarr format.

## Features

- **Simple API**: Clean, intuitive interfaces for common operations
- **Efficient Conversion**: Fast conversion of NetCDF to Zarr format
- **Data Packing**: Compress data using fixed-scale offset encoding
- **Intelligent Chunking**: Automatic chunking recommendations based on access patterns (temporal, spatial, balanced) with intelligent chunking for parallel archives
- **Compression**: Support for various compression algorithms
- **Time Series Handling**: Efficient handling of time-series data
- **Data Appending**: Append new data to existing Zarr archives
- **Parallel Writing**: Create template archives and write regions in parallel with intelligent chunking
- **Metadata Preservation**: Maintain dataset metadata during conversion

## Installation

```bash
pip install zarrio
```

## Usage

### Command Line Interface

```bash
# Convert NetCDF to Zarr
zarrio convert input.nc output.zarr

# Convert with chunking
zarrio convert input.nc output.zarr --chunking "time:100,lat:50,lon:100"

# Convert with compression
zarrio convert input.nc output.zarr --compression "blosc:zstd:3"

# Convert with data packing
zarrio convert input.nc output.zarr --packing --packing-bits 16

# Convert with manual packing ranges
zarrio convert input.nc output.zarr --packing \
    --packing-manual-ranges '{"temperature": {"min": -50, "max": 50}}'

# Analyze NetCDF file for optimization recommendations
zarrio analyze input.nc

# Analyze with theoretical performance testing
zarrio analyze input.nc --test-performance

# Analyze with actual performance testing
zarrio analyze input.nc --run-tests

# Analyze with interactive configuration setup
zarrio analyze input.nc --interactive

# Create template for parallel writing
zarrio create-template template.nc archive.zarr --global-start 2023-01-01 --global-end 2023-12-31

# Create template with intelligent chunking
zarrio create-template template.nc archive.zarr --global-start 2023-01-01 --global-end 2023-12-31 --intelligent-chunking --access-pattern temporal

# Write region to existing archive
zarrio write-region data.nc archive.zarr

# Append to existing Zarr store
zarrio append new_data.nc existing.zarr
```

### Python API

```python
from zarrio import convert_to_zarr, append_to_zarr, ZarrConverter

# Simple conversion
convert_to_zarr("input.nc", "output.zarr")

# Conversion with options
convert_to_zarr(
    "input.nc",
    "output.zarr",
    chunking={"time": 100, "lat": 50, "lon": 100},
    compression="blosc:zstd:3",
    packing=True,
    packing_bits=16,
    packing_manual_ranges={
        "temperature": {"min": -50, "max": 50}
    },
    packing_auto_buffer_factor=0.05
)

# Using the class-based interface
converter = ZarrConverter(
    chunking={"time": 100, "lat": 50, "lon": 100},
    compression="blosc:zstd:3",
    packing=True,
    packing_manual_ranges={
        "temperature": {"min": -50, "max": 50}
    }
)
converter.convert("input.nc", "output.zarr")

# Parallel writing workflow
# 1. Create template archive
converter.create_template(
    template_dataset=template_ds,
    output_path="archive.zarr",
    global_start="2023-01-01",
    global_end="2023-12-31",
    compute=False  # Metadata only
)

# 2. Write regions in parallel (in separate processes)
converter.write_region("data1.nc", "archive.zarr")
converter.write_region("data2.nc", "archive.zarr")
converter.write_region("data3.nc", "archive.zarr")

# Append to existing Zarr store
append_to_zarr("new_data.nc", "existing.zarr")
```

## Parallel Writing

One of the key features of zarrio is support for parallel writing of large datasets:

```python
# Step 1: Create template archive with intelligent chunking
converter = ZarrConverter(
    chunking={"time": 100, "lat": 50, "lon": 100},
    access_pattern="temporal"  # Optimize for time series analysis
)
converter.create_template(
    template_dataset=template_dataset,
    output_path="large_archive.zarr",
    global_start="2020-01-01",
    global_end="2023-12-31",
    compute=False,  # Metadata only, no data computation
    intelligent_chunking=True,  # Enable intelligent chunking based on full archive dimensions
    access_pattern="temporal"   # Optimize for time series analysis
)

# Step 2: Write regions in parallel processes
# Process 1: converter.write_region("file1.nc", "large_archive.zarr")
# Process 2: converter.write_region("file2.nc", "large_archive.zarr")
# Process 3: converter.write_region("file3.nc", "large_archive.zarr")
```

This approach is ideal for converting large numbers of NetCDF files to a single Zarr archive in parallel. The intelligent chunking feature ensures optimal chunking based on the full archive dimensions rather than just the template dataset.

## Rolling Archive

Manage forecast cycle archives with automatic cleanup of old data based on time-based retention windows.

### What is Rolling Archive?

Rolling archive automatically removes old forecast cycles from your Zarr store based on a configurable retention window. This is useful when:

- You run forecasts multiple times per day
- You want to keep only the last N hours/days of data
- You need to manage disk space or datamesh storage

### Python API

```python
from zarrio import ZarrConverter
from datetime import timedelta

# Configure rolling archive
converter = ZarrConverter(
    rolling_archive={
        "enabled": True,
        "retention_window": timedelta(hours=24),  # Keep last 24 hours
        "min_groups_to_keep": 4,  # Always keep at least 4 cycles
        "auto_cleanup": True,  # Cleanup after each write
    }
)

# Write data - cleanup happens automatically after each write
converter.convert("forecast.nc", "archive.zarr", group="cycle/20240101T000000")
```

### Manual Cleanup

You can also trigger cleanup manually:

```python
# Dry run - see what would be deleted without making changes
result = converter.cleanup_archive("archive.zarr", dry_run=True)
print(f"Would delete: {len(result['deleted'])} groups")
for g in result['deleted']:
    print(f"  - {g}")
print(f"Would keep: {len(result['kept'])} groups")

# Actual cleanup
result = converter.cleanup_archive("archive.zarr")
print(f"Deleted: {len(result['deleted'])} groups")
print(f"Kept: {len(result['kept'])} groups")
if result['skipped']:
    print(f"Skipped: {len(result['skipped'])} groups (unparseable timestamp)")
```

### CLI Usage

Enable rolling archive via command line:

```bash
# Convert with 24-hour retention
zarrio convert forecast.nc archive.zarr --rolling-archive-hours 24

# This enables automatic cleanup after each write
```

### Configuration Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `enabled` | bool | `False` | Enable automatic rolling archive cleanup |
| `retention_window` | timedelta | `None` | How long to keep data (minimum 1 hour) |
| `time_reference_attr` | str | `'cycle_time'` | Attribute name containing timestamp |
| `auto_cleanup` | bool | `True` | Cleanup automatically after each write |
| `min_groups_to_keep` | int | `1` | Minimum number of groups to always preserve |

### Best Practices

1. **Use dry_run first** to verify what will be deleted before actual cleanup
2. **Set min_groups_to_keep** to prevent accidental total deletion
3. **Use time-based retention** (not cycle count) for predictable behavior
4. **Monitor cleanup logs** to ensure it's working as expected

See `examples/rolling_archive_demo.py` for a complete working example.

## Configuration

You can also use configuration files (YAML or JSON):

```yaml
# config.yaml
chunking:
  time: 100
  lat: 50
  lon: 100
compression: "blosc:zstd:3"
packing:
  enabled: true
  bits: 16
  manual_ranges:
    temperature:
      min: -50
      max: 50
  auto_buffer_factor: 0.05
variables:
  - temperature
  - pressure
drop_variables:
  - unused_var
```

Then use it with the CLI:

```bash
zarrio convert input.nc output.zarr --config config.yaml
```

## Development

### Installation

```bash
git clone https://github.com/oceanum/zarrio.git
cd zarrio
pip install -e .
```

### Running Tests

```bash
pip install -e ".[dev]"
pytest
```

### Code Quality

```bash
# Format code
black .

# Check code style
flake8

# Type checking
mypy zarrio
```

## License

MIT License

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.
