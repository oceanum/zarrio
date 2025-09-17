Welcome to zarrify's documentation!
====================================

Overview
--------

zarrify is a modern, clean library for converting scientific data formats (primarily NetCDF) to Zarr format. It was created as a complete rewrite of the original onzarr library to address maintainability issues while preserving all essential functionality and adding crucial parallel writing capabilities.

Key Features
------------

Clean Architecture
^^^^^^^^^^^^^^^^^^

* **Fresh Start**: Completely new codebase without legacy constraints
* **Modular Design**: Clear separation of concerns with dedicated modules
* **Modern Python**: Full type hints, proper error handling, clean APIs
* **No Circular Imports**: Fixed all import issues from the original design

Core Functionality
^^^^^^^^^^^^^^^^^^

All essential features have been implemented:

* - NetCDF to Zarr conversion
* - Data packing with fixed-scale offset encoding
* - Flexible chunking strategies
* - Compression support
* - Time series handling
* - Data appending to existing archives
* - **Parallel writing with template creation and region writing**
* - **Pydantic configuration management and validation**
* - **Intelligent chunking analysis and recommendations**
* - **Retry logic for handling missing data**

User Interfaces
^^^^^^^^^^^^^^^

* - Simple functional API (``convert_to_zarr``, ``append_to_zarr``)
* - Advanced class-based API (``ZarrConverter``)
* - Command-line interface with comprehensive options
* - **Pydantic configuration file support (YAML/JSON)**
* - **Intelligent chunking recommendations**

Quality Assurance
^^^^^^^^^^^^^^^^^

* - Comprehensive test suite (46/46 core tests passing)
* - Proper error handling with custom exceptions
* - Logging for debugging and monitoring
* - Documentation with examples

Getting Started
---------------

Installation
^^^^^^^^^^^^

Install zarrify using pip:

.. code-block:: bash

    pip install zarrify

Or install from source:

.. code-block:: bash

    git clone https://github.com/oceanum/zarrify.git
    cd zarrify
    pip install -e .

Quick Start
^^^^^^^^^^^

Simple conversion:

.. code-block:: python

    from zarrify import convert_to_zarr

    # Convert NetCDF to Zarr
    convert_to_zarr("input.nc", "output.zarr")

Advanced conversion with configuration:

.. code-block:: python

    from zarrify import ZarrConverter
    from zarrify.models import ZarrConverterConfig, ChunkingConfig, PackingConfig

    # Create converter with configuration
    config = ZarrConverterConfig(
        chunking=ChunkingConfig(time=100, lat=50, lon=100),
        packing=PackingConfig(enabled=True, bits=16),
        attrs={"title": "Demo dataset", "source": "zarrify"}
    )

    converter = ZarrConverter(config=config)
    converter.convert("input.nc", "output.zarr")

Command-line interface:

.. code-block:: bash

    # Convert NetCDF to Zarr
    zarrify convert input.nc output.zarr

    # Convert with chunking
    zarrify convert input.nc output.zarr --chunking "time:100,lat:50,lon:100"

    # Convert with data packing
    zarrify convert input.nc output.zarr --packing --packing-bits 16

Parallel Processing
-------------------

One of the key features of zarrify is its support for parallel processing of large datasets through template creation and region writing.

**Intelligent Chunking**: When creating templates, zarrify can now perform intelligent chunking based on the full archive dimensions rather than just the template dataset. This ensures optimal chunking for the entire archive.

For detailed information, see the :doc:`parallel` documentation.

Pydantic Configuration
----------------------

zarrify uses Pydantic for type-safe configuration management:

Programmatic Configuration
^^^^^^^^^^^^^^^^^^^^^^^^^^

.. code-block:: python

    from zarrify.models import (
        ZarrConverterConfig,
        ChunkingConfig,
        PackingConfig,
        CompressionConfig
    )

    # Programmatic configuration with validation
    config = ZarrConverterConfig(
        chunking=ChunkingConfig(time=100, lat=50, lon=100),
        compression=CompressionConfig(method="blosc:zstd:3"),
        packing=PackingConfig(enabled=True, bits=16),
        attrs={"title": "Demo dataset", "source": "zarrify"}
    )

    converter = ZarrConverter(config=config)

Configuration File Support
^^^^^^^^^^^^^^^^^^^^^^^^^^

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

.. code-block:: python

    # Load from YAML file
    converter = ZarrConverter.from_config_file("config.yaml")
    converter.convert("input.nc", "output.zarr")

Intelligent Chunking Analysis
-----------------------------

zarrify provides intelligent chunking analysis:

Automatic Recommendations
^^^^^^^^^^^^^^^^^^^^^^^^^

When no chunking is specified, zarrify automatically analyzes the dataset and provides recommendations:

.. code-block:: python

    # No chunking specified - automatic analysis
    convert_to_zarr(
        "climate_data.nc", 
        "climate_data.zarr",
        access_pattern="balanced"  # Optimize for mixed workloads
    )

Access Pattern Optimization
^^^^^^^^^^^^^^^^^^^^^^^^^^^

Different strategies for different access patterns:

* **Temporal Focus**: Optimized for time series analysis
* **Spatial Focus**: Optimized for spatial analysis
* **Balanced**: Good performance for mixed workloads

Validation and Warnings
^^^^^^^^^^^^^^^^^^^^^^^

Validates user-provided chunking and provides actionable feedback:

.. code-block:: python

    # Configuration will be validated with warnings
    config = ZarrConverterConfig(
        chunking=ChunkingConfig(time=1000, lat=1, lon=1),  # Will generate warnings
    )

    converter = ZarrConverter(config=config)
    # Logs warnings about suboptimal chunking

Retry Logic for Missing Data
----------------------------

zarrify implements intelligent retry logic for handling missing data:

Missing Data Detection
^^^^^^^^^^^^^^^^^^^^^^

Automatically detect missing data after writing:

.. code-block:: python

    from zarrify.missing import MissingDataHandler

    handler = MissingDataHandler(
        missing_check_vars="all",  # Check all variables
        retries_on_missing=3,     # Enable 3 retries
        time_dim="time"
    )

    has_missing = handler.has_missing("archive.zarr", input_dataset)

Automatic Retry Mechanism
^^^^^^^^^^^^^^^^^^^^^^^^^

When missing data is detected, automatically retry with exponential backoff:

.. code-block:: python

    # Handle missing data with retry logic
    result = handler.handle_missing_data(
        zarr_path="archive.zarr",
        input_dataset=input_dataset,
        region=None,
        write_func=write_function,  # Function to retry
        **kwargs                   # Arguments for write_function
    )

CLI Support
^^^^^^^^^^^

.. code-block:: bash

    # Convert with retry logic
    zarrify convert input.nc output.zarr --retries-on-missing 3

    # Append with retry logic
    zarrify append new_data.nc existing.zarr --retries-on-missing 2

    # Create template with retry logic
    zarrify create-template template.nc archive.zarr --retries-on-missing 1

    # Write region with retry logic
    zarrify write-region data.nc archive.zarr --retries-on-missing 2

Configuration File Support
^^^^^^^^^^^^^^^^^^^^^^^^^^

.. code-block:: yaml

    # config.yaml with retry logic
    missing_data:
      check_vars: "all"
      retries_on_missing: 3
      missing_check_vars: 
        - temperature
        - pressure

.. code-block:: python

    # Load from YAML file
    converter = ZarrConverter.from_config_file("config.yaml")
    converter.convert("input.nc", "output.zarr")

Compression Expectations for Scientific Data
--------------------------------------------

When working with scientific datasets like climate or oceanographic data, it's important to understand what to expect from compression:

Why Compression Ratios Are Typically Low
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Scientific data characteristics that affect compression:

1. **High entropy**: Ocean/wave data contains complex, seemingly random patterns
2. **High precision**: Float32/Float64 values with many significant digits
3. **Natural variability**: Physical processes create inherently diverse data
4. **Limited redundancy**: Unlike text or images, scientific data has little repetitive structure

What This Means for Your Data
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

- Compression ratios of 1.0-1.1x are normal for scientific datasets
- Speed improvements (20-40% faster) are often more valuable than size reductions
- Focus on selective application rather than blanket compression
- Real-world testing with your actual workflows provides the best guidance

Best Strategies for Your Data
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

1. **Use 8-bit packing** for size reduction (if precision loss is acceptable)
2. **Use Blosc compression** for speed improvement
3. **Combination approaches** for balanced benefits (Packing + Blosc)
4. **Apply selectively** - pack only certain variables rather than entire dataset

Performance vs. Size Trade-offs
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Often, the most valuable benefit is **processing speed improvements** rather than dramatic size reductions. Consider:

- Reduced disk I/O often matters more than file size for performance
- Compression algorithms speed up I/O operations
- Focus on chunking strategy - often more impactful than compression

API Reference
-------------

.. toctree::
   :maxdepth: 2

   api
   modules

Examples
--------

.. toctree::
   :maxdepth: 1

   examples
   quickstart
   installation
   configuration
   cli
   chunking
   packing
   analysis
   parallel
   datamesh

Additional Information
----------------------

.. toctree::
   :maxdepth: 1

   contributing
   changelog
   docker

License
-------

MIT License

Indices and Tables
------------------

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
