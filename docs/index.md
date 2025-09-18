# zarrio Documentation

## Table of Contents

1. [Installation](#installation)
2. [Quick Start](#quick-start)
3. [Core Concepts](#core-concepts)
4. [API Reference](#api-reference)
5. [Configuration](#configuration)
6. [CLI Usage](#cli-usage)
7. [Examples](#examples)

## Installation

```bash
pip install zarrio
```

## Quick Start

```python
from zarrio import convert_to_zarr

# Convert a NetCDF file to Zarr
convert_to_zarr("input.nc", "output.zarr")
```

## Core Concepts

### ZarrConverter

The main class for converting data to Zarr format. It provides fine-grained control over the conversion process.

### Data Packing

zarrio supports data packing using fixed-scale offset encoding to reduce storage requirements while maintaining data quality.

### Chunking

Efficient chunking strategies can significantly improve performance for large datasets.

### Compression

Various compression algorithms are supported through the zarr library.

## API Reference

### convert_to_zarr()

Convert data to Zarr format using default settings.

```python
from zarrio import convert_to_zarr

convert_to_zarr(
    input_path="input.nc",
    output_path="output.zarr",
    chunking={"time": 100, "lat": 50, "lon": 100},
    compression="blosc:zstd:3",
    packing=True,
    packing_bits=16
)
```

### append_to_zarr()

Append data to an existing Zarr store.

```python
from zarrio import append_to_zarr

append_to_zarr(
    input_path="new_data.nc",
    zarr_path="existing.zarr"
)
```

### ZarrConverter Class

```python
from zarrio import ZarrConverter

converter = ZarrConverter(
    chunking={"time": 100, "lat": 50, "lon": 100},
    compression="blosc:zstd:3",
    packing=True,
    packing_bits=16
)
converter.convert("input.nc", "output.zarr")
```

## Configuration

Configuration files can be YAML or JSON format:

```yaml
# config.yaml
chunking:
  time: 100
  lat: 50
  lon: 100
compression: "blosc:zstd:3"
packing: true
packing_bits: 16
```

Use with the CLI:

```bash
zarrio convert input.nc output.zarr --config config.yaml
```

## CLI Usage

```bash
# Convert NetCDF to Zarr
zarrio convert input.nc output.zarr

# Convert with chunking
zarrio convert input.nc output.zarr --chunking "time:100,lat:50,lon:100"

# Convert with compression
zarrio convert input.nc output.zarr --compression "blosc:zstd:3"

# Convert with data packing
zarrio convert input.nc output.zarr --packing --packing-bits 16

# Append to existing Zarr store
zarrio append new_data.nc existing.zarr

# Get help
zarrio --help
zarrio convert --help
```

## Examples

See the `examples/` directory for detailed usage examples:

1. `demo.py` - Comprehensive demonstration of all features
2. `config.yaml` - Example configuration file