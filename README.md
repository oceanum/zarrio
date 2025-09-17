# zarrify

A modern, clean library for converting scientific data formats to Zarr format.

## Overview

zarrify is a complete rewrite of the original onzarr library with a focus on simplicity, performance, and maintainability. It leverages modern xarray and zarr capabilities to provide efficient conversion of NetCDF and other scientific data formats to Zarr format.

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
pip install zarrify
```

## Usage

### Command Line Interface

```bash
# Convert NetCDF to Zarr
zarrify convert input.nc output.zarr

# Convert with chunking
zarrify convert input.nc output.zarr --chunking "time:100,lat:50,lon:100"

# Convert with compression
zarrify convert input.nc output.zarr --compression "blosc:zstd:3"

# Convert with data packing
zarrify convert input.nc output.zarr --packing --packing-bits 16

# Convert with manual packing ranges
zarrify convert input.nc output.zarr --packing \
    --packing-manual-ranges '{"temperature": {"min": -50, "max": 50}}'

# Analyze NetCDF file for optimization recommendations
zarrify analyze input.nc

# Analyze with theoretical performance testing
zarrify analyze input.nc --test-performance

# Analyze with actual performance testing
zarrify analyze input.nc --run-tests

# Analyze with interactive configuration setup
zarrify analyze input.nc --interactive

# Create template for parallel writing
zarrify create-template template.nc archive.zarr --global-start 2023-01-01 --global-end 2023-12-31

# Create template with intelligent chunking
zarrify create-template template.nc archive.zarr --global-start 2023-01-01 --global-end 2023-12-31 --intelligent-chunking --access-pattern temporal

# Write region to existing archive
zarrify write-region data.nc archive.zarr

# Append to existing Zarr store
zarrify append new_data.nc existing.zarr
```

### Python API

```python
from zarrify import convert_to_zarr, append_to_zarr, ZarrConverter

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

One of the key features of zarrify is support for parallel writing of large datasets:

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
zarrify convert input.nc output.zarr --config config.yaml
```

## Development

### Installation

```bash
git clone https://github.com/oceanum/zarrify.git
cd zarrify
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
mypy zarrify
```

## License

MIT License

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.