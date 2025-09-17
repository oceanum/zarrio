Command-Line Interface
======================

zarrify provides a comprehensive command-line interface for converting data to Zarr format.

Basic Usage
-----------

The basic syntax for the CLI is:

.. code-block:: bash

    zarrify [OPTIONS] COMMAND [ARGS]...

Getting Help
------------

To get help on the available commands:

.. code-block:: bash

    zarrify --help

To get help on a specific command:

.. code-block:: bash

    zarrify COMMAND --help

Version Information
-------------------

To check the version of zarrify:

.. code-block:: bash

    zarrify --version

Convert Command
---------------

The `convert` command converts data to Zarr format.

.. code-block:: bash

    zarrify convert [OPTIONS] INPUT OUTPUT

Options:
~~~~~~~~

--chunking CHUNKING
    Chunking specification (e.g., 'time:100,lat:50,lon:100')

--compression COMPRESSION
    Compression specification (e.g., 'blosc:zstd:3')

--packing
    Enable data packing

--packing-bits {8,16,32}
    Number of bits for packing (default: 16)

--packing-manual-ranges PACKING_MANUAL_RANGES
    Manual min/max ranges as JSON string (e.g., '{"temperature": {"min": 0, "max": 100}}')

--packing-auto-buffer-factor PACKING_AUTO_BUFFER_FACTOR
    Buffer factor for automatically calculated ranges (default: 0.01)

--packing-check-range-exceeded
    Check if data exceeds specified ranges (default: True)

--packing-range-exceeded-action {warn,error,ignore}
    Action when data exceeds range (default: warn)

--variables VARIABLES
    Comma-separated list of variables to include

--drop-variables DROP_VARIABLES
    Comma-separated list of variables to exclude

--attrs ATTRS
    Additional global attributes as JSON string

--time-dim TIME_DIM
    Name of time dimension (default: time)

--target-chunk-size-mb TARGET_CHUNK_SIZE_MB
    Target chunk size in MB for intelligent chunking (default: 50)
    Use this option to configure chunk sizes for different environments:
    - Local development: 10-25 MB
    - Production servers: 50-100 MB
    - Cloud environments: 100-200 MB

--datamesh-datasource DATAMESH_DATASOURCE
    Datamesh datasource configuration as JSON string

--datamesh-token DATAMESH_TOKEN
    Datamesh token for authentication

--datamesh-service DATAMESH_SERVICE
    Datamesh service URL

--config CONFIG
    Configuration file (YAML or JSON)
- ``--access-pattern TEXT``: Expected access pattern (temporal, spatial, balanced)
- ``-v, --verbose``: Increase verbosity (use -v, -vv, or -vvv)

Examples:
~~~~~~~~~

Convert a single NetCDF file to Zarr:

.. code-block:: bash

    zarrify convert input.nc output.zarr

Convert with chunking:

.. code-block:: bash

    zarrify convert input.nc output.zarr --chunking "time:100,lat:50,lon:100"

Convert with compression:

.. code-block:: bash

    zarrify convert input.nc output.zarr --compression "blosc:zstd:3"

Convert with data packing:

.. code-block:: bash

    zarrify convert input.nc output.zarr --packing --packing-bits 16

Convert with variable selection:

.. code-block:: bash

    zarrify convert input.nc output.zarr --variables "temperature,pressure"

Convert with variable exclusion:

.. code-block:: bash

    zarrify convert input.nc output.zarr --drop-variables "humidity"

Convert with additional attributes:

.. code-block:: bash

    zarrify convert input.nc output.zarr --attrs '{"title": "Demo dataset", "source": "zarrify"}'

Convert with configuration file:

.. code-block:: bash

    zarrify convert input.nc output.zarr --config config.yaml

Append Command
--------------

The `append` command appends data to an existing Zarr store.

.. code-block:: bash

    zarrify append [OPTIONS] INPUT ZARR

Options:
~~~~~~~~

- ``--chunking TEXT``: Chunking specification (e.g., 'time:100,lat:50,lon:100')
- ``--variables TEXT``: Comma-separated list of variables to include
- ``--drop-variables TEXT``: Comma-separated list of variables to exclude
- ``--append-dim TEXT``: Dimension to append along (default: time)
- ``--time-dim TEXT``: Name of time dimension (default: time)
- ``--config PATH``: Configuration file (YAML or JSON)
- ``-v, --verbose``: Increase verbosity (use -v, -vv, or -vvv)

Examples:
~~~~~~~~~

Append data to an existing Zarr store:

.. code-block:: bash

    zarrify append new_data.nc existing.zarr

Append with variable selection:

.. code-block:: bash

    zarrify append new_data.nc existing.zarr --variables "temperature,pressure"

Append with chunking:

.. code-block:: bash

    zarrify append new_data.nc existing.zarr --chunking "time:50,lat:25,lon:50"

Create-Template Command
-----------------------

The `create-template` command creates a template Zarr archive for parallel writing.

.. code-block:: bash

    zarrify create-template [OPTIONS] TEMPLATE OUTPUT

Options:
~~~~~~~~

- ``--chunking TEXT``: Chunking specification (e.g., 'time:100,lat:50,lon:100')
- ``--compression TEXT``: Compression specification (e.g., 'blosc:zstd:3')
- ``--packing``: Enable data packing
- ``--packing-bits INTEGER``: Number of bits for packing (8, 16, or 32)
- ``--packing-manual-ranges TEXT``: Manual min/max ranges as JSON string
- ``--packing-auto-buffer-factor FLOAT``: Buffer factor for automatically calculated ranges
- ``--packing-check-range-exceeded``: Check if data exceeds specified ranges
- ``--packing-range-exceeded-action [warn|error|ignore]``: Action when data exceeds range
- ``--global-start TEXT``: Start time for full archive (e.g., '2020-01-01')
- ``--global-end TEXT``: End time for full archive (e.g., '2023-12-31')
- ``--freq TEXT``: Time frequency (e.g., '1D', '1H', inferred if not provided)
- ``--metadata-only``: Create metadata only (compute=False)
- ``--time-dim TEXT``: Name of time dimension (default: time)
- ``--intelligent-chunking``: Enable intelligent chunking based on full archive dimensions
- ``--access-pattern [temporal|spatial|balanced]``: Access pattern for chunking optimization (default: balanced)
- ``--config PATH``: Configuration file (YAML or JSON)
- ``-v, --verbose``: Increase verbosity (use -v, -vv, or -vvv)

Examples:
~~~~~~~~~

Create template for parallel writing:

.. code-block:: bash

    zarrify create-template template.nc archive.zarr \\
        --global-start 2020-01-01 \\
        --global-end 2023-12-31

Create template with intelligent chunking:

.. code-block:: bash

    zarrify create-template template.nc archive.zarr \\
        --global-start 2020-01-01 \\
        --global-end 2023-12-31 \\
        --intelligent-chunking \\
        --access-pattern temporal

Create template with chunking:

.. code-block:: bash

    zarrify create-template template.nc archive.zarr \
        --global-start 2020-01-01 \
        --global-end 2023-12-31 \
        --chunking "time:100,lat:50,lon:100"

Create template with compression:

.. code-block:: bash

    zarrify create-template template.nc archive.zarr \
        --global-start 2020-01-01 \
        --global-end 2023-12-31 \
        --compression "blosc:zstd:3"

Create template with data packing:

.. code-block:: bash

    zarrify create-template template.nc archive.zarr \
        --global-start 2020-01-01 \
        --global-end 2023-12-31 \
        --packing --packing-bits 16

Create template with manual packing ranges:

.. code-block:: bash

    zarrify create-template template.nc archive.zarr \
        --global-start 2020-01-01 \
        --global-end 2023-12-31 \
        --packing --packing-manual-ranges '{"temperature": {"min": -50, "max": 50}}'

Create template with automatic range calculation:

.. code-block:: bash

    zarrify create-template template.nc archive.zarr\n        --global-start 2020-01-01\n        --global-end 2023-12-31\n        --packing --packing-auto-buffer-factor 0.05

.. code-block:: bash

    zarrify analyze input.nc --interactive

Parallel Processing Example
---------------------------

To process thousands of NetCDF files in parallel:

1. Create template:

.. code-block:: bash

    zarrify create-template template.nc archive.zarr \\
        --global-start 2020-01-01 \\
        --global-end 2023-12-31

2. Write regions in parallel processes:

.. code-block:: bash

    # Process 1
    zarrify write-region data_2020.nc archive.zarr

    # Process 2  
    zarrify write-region data_2021.nc archive.zarr

    # Process 3
    zarrify write-region data_2022.nc archive.zarr

    # Process 4
    zarrify write-region data_2023.nc archive.zarr

Configuration File Support
--------------------------

zarrify supports configuration files in YAML or JSON format:

YAML Example:
~~~~~~~~~~~~~

.. code-block:: yaml

    # config.yaml
    chunking:
      time: 150
      lat: 60
      lon: 120
    compression:
      method: blosc:zstd:2
      clevel: 2
    packing:
      enabled: true
      bits: 16
    variables:
      include:
        - temperature
        - pressure
      exclude:
        - humidity
    attrs:
      title: YAML Config Demo
      version: 1.0
    time:
      dim: time
      append_dim: time
    retries_on_missing: 3
    missing_check_vars: all

Usage:
~~~~~~

.. code-block:: bash

    zarrify convert input.nc output.zarr --config config.yaml

JSON Example:
~~~~~~~~~~~~~

.. code-block:: json

    {
      "chunking": {
        "time": 125,
        "lat": 55,
        "lon": 110
      },
      "compression": {
        "method": "blosc:lz4:1",
        "clevel": 1
      },
      "packing": {
        "enabled": true,
        "bits": 8
      },
      "time": {
        "global_start": "2020-01-01",
        "global_end": "2023-12-31"
      },
      "attrs": {
        "title": "JSON Config Demo",
        "version": "1.0"
      }
    }

Usage:
~~~~~~

.. code-block:: bash

    zarrify convert input.nc output.zarr --config config.json

Compression Expectations
------------------------

When working with scientific datasets, it's important to understand what to expect from compression:

- Compression ratios of 1.0-1.1x are normal for scientific datasets
- Speed improvements (20-40% faster) are often more valuable than size reductions
- The primary benefit of compression is reduced I/O time, not file size reduction
- For significant size reductions, consider using data packing in combination with compression

For more detailed information about compression expectations, see the main documentation.

Analyze Command
---------------

The `analyze` command analyzes NetCDF files and provides optimization recommendations.

.. code-block:: bash

    zarrify analyze [OPTIONS] INPUT

Options:
~~~~~~~~

- ``--target-chunk-size-mb INTEGER``: Target chunk size in MB for analysis (default: 50)
- ``--test-performance``: Show theoretical performance benefits
- ``--run-tests``: Run actual performance tests to measure real-world benefits
- ``-i, --interactive``: Interactive mode to guide configuration setup
- ``-v, --verbose``: Increase verbosity (use -v, -vv, or -vvv)

Examples:
~~~~~~~~~

Analyze a NetCDF file:

.. code-block:: bash

    zarrify analyze input.nc

Analyze with theoretical performance testing:

.. code-block:: bash

    zarrify analyze input.nc --test-performance

Analyze with actual performance testing:

.. code-block:: bash

    zarrify analyze input.nc --run-tests

Analyze with custom target chunk size:

.. code-block:: bash

    zarrify analyze input.nc --target-chunk-size-mb 100

Analyze with interactive configuration setup:

.. code-block:: bash

    zarrify analyze input.nc --interactive

.. code-block:: bash

    zarrify analyze input.nc --interactive