Configuration Management
========================

zarrio provides flexible configuration management using Pydantic models for type safety and validation.

Pydantic Models
----------------

All configuration in zarrio is managed through Pydantic models, which provide:

- Type safety with automatic validation
- Clear error messages for invalid configurations
- Support for nested configurations
- Serialization to/from YAML and JSON

ZarrConverterConfig
--------------------

The main configuration class is `ZarrConverterConfig`:

.. code-block:: python

    from zarrio.models import ZarrConverterConfig

    config = ZarrConverterConfig(
        chunking={"time": 100, "lat": 50, "lon": 100},
        compression={"method": "blosc:zstd:3"},
        packing={"enabled": True, "bits": 16},
        time={"dim": "time", "append_dim": "time"},
        variables={"include": ["temperature"], "exclude": ["humidity"]},
        target_chunk_size_mb=100,  # Configurable target chunk size
        attrs={"title": "Demo dataset", "source": "zarrio"}
    )

The ``ZarrConverterConfig`` supports the following fields:

- **chunking**: Chunking configuration (time, lat, lon, depth dimensions)
- **compression**: Compression settings (method, cname, clevel, shuffle)
- **packing**: Data packing configuration (enabled, bits)
- **time**: Time dimension configuration (dim, append_dim, global_start, global_end, freq)
- **variables**: Variable selection (include, exclude)
- **missing_data**: Missing data handling (check_vars, retries_on_missing, missing_check_vars)
- **datamesh**: Datamesh integration configuration
- **attrs**: Global attributes to add to the dataset
- **target_chunk_size_mb**: Target chunk size in MB for intelligent chunking (default: 50)
- **access_pattern**: Access pattern for chunking optimization ('temporal', 'spatial', 'balanced')
- **retries_on_missing**: Number of retries for missing data (backward compatibility)
- **missing_check_vars**: Variables to check for missing data (backward compatibility)

Chunking Configuration
-----------------------

Chunking can be configured through the `ChunkingConfig` model:

.. code-block:: python

    from zarrio.models import ChunkingConfig

    chunking = ChunkingConfig(
        time=100,
        lat=50,
        lon=100
    )

Packing Configuration
----------------------

Packing can be configured through the `PackingConfig` model:

.. code-block:: python

    from zarrio.models import PackingConfig

    packing = PackingConfig(
        enabled=True,
        bits=16
    )

Enhanced Packing Features
~~~~~~~~~~~~~~~~~~~~~~~~~

The enhanced packing functionality provides several improvements over basic packing:

Priority-Based Range Determination:
    The enhanced packing system uses a clear priority order for determining the min/max values used for packing:
    
    1. **Manual ranges** (if provided)
    2. **Variable attributes** (valid_min/valid_max)
    3. **Automatic calculation** from data (with warnings)

Manual Range Specification:
    Users can explicitly specify min/max ranges for variables:

    .. code-block:: python

        from zarrio.models import PackingConfig

        packing = PackingConfig(
            enabled=True,
            bits=16,
            manual_ranges={
                "temperature": {"min": -50, "max": 50},
                "pressure": {"min": 90000, "max": 110000}
            }
        )

Automatic Range Calculation with Buffer:
    When no ranges are provided, the system automatically calculates them from the data:

    .. code-block:: python

        from zarrio.models import PackingConfig

        packing = PackingConfig(
            enabled=True,
            bits=16,
            auto_buffer_factor=0.05  # 5% buffer
        )

Range Exceeded Validation:
    Optional checking to ensure data doesn't exceed specified ranges:

    .. code-block:: python

        from zarrio.models import PackingConfig

        packing = PackingConfig(
            enabled=True,
            bits=16,
            manual_ranges={"temperature": {"min": -50, "max": 50}},
            check_range_exceeded=True,
            range_exceeded_action="error"  # or "warn" or "ignore"
        )

Compression Configuration
--------------------------

Compression can be configured through the `CompressionConfig` model:

.. code-block:: python

    from zarrio.models import CompressionConfig

    compression = CompressionConfig(
        method="blosc:zstd:3",
        cname="zstd",
        clevel=3,
        shuffle="shuffle"
    )

Time Configuration
--------------------

Time handling can be configured through the `TimeConfig` model:

.. code-block:: python

    from zarrio.models import TimeConfig

    time_config = TimeConfig(
        dim="time",
        append_dim="time",
        global_start="2020-01-01",
        global_end="2023-12-31",
        freq="1D"
    )

Access Pattern Configuration
-----------------------------

The ``access_pattern`` field controls how intelligent chunking optimizes the data layout:

- **temporal**: Optimized for time series analysis (large time chunks, smaller spatial chunks)
- **spatial**: Optimized for spatial analysis (smaller time chunks, larger spatial chunks)
- **balanced**: Good performance for mixed workloads (moderate chunking across all dimensions)

Programmatic Configuration:

.. code-block:: python

    from zarrio.models import ZarrConverterConfig

    config = ZarrConverterConfig(
        access_pattern="temporal"  # Optimize for time series analysis
    )

YAML Configuration:

.. code-block:: yaml

    # config.yaml
    access_pattern: temporal
    chunking:
      time: 100
      lat: 50
      lon: 100

JSON Configuration:

.. code-block:: json

    {
      "access_pattern": "temporal",
      "chunking": {
        "time": 100,
        "lat": 50,
        "lon": 100
      }
    }

Variable handling can be configured through the `VariableConfig` model:

.. code-block:: python

    from zarrio.models import VariableConfig

    variables = VariableConfig(
        include=["temperature", "pressure"],
        exclude=["humidity"]
    )

Loading from Files
--------------------

Configuration can be loaded from YAML or JSON files:

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
    time:
      dim: time
      append_dim: time
      global_start: "2020-01-01"
      global_end: "2023-12-31"
      freq: "1D"
    variables:
      include:
        - temperature
        - pressure
      exclude:
        - humidity
    attrs:
      title: YAML Config Demo
      version: 1.0

Loading:
~~~~~~~~

.. code-block:: python

    from zarrio.models import ZarrConverterConfig

    config = ZarrConverterConfig.from_yaml_file("config.yaml")

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
        "dim": "time",
        "append_dim": "time",
        "global_start": "2020-01-01",
        "global_end": "2023-12-31",
        "freq": "1D"
      },
      "variables": {
        "include": ["temperature", "pressure"],
        "exclude": ["humidity"]
      },
      "attrs": {
        "title": "JSON Config Demo",
        "version": "1.0"
      }
    }

Loading:
~~~~~~~~

.. code-block:: python

    from zarrio.models import ZarrConverterConfig

    config = ZarrConverterConfig.from_json_file("config.json")

Programmatic Configuration
---------------------------

Configuration can also be created programmatically:

.. code-block:: python

    from zarrio.models import (
        ZarrConverterConfig,
        ChunkingConfig,
        PackingConfig,
        CompressionConfig,
        TimeConfig,
        VariableConfig
    )

    config = ZarrConverterConfig(
        chunking=ChunkingConfig(time=100, lat=50, lon=100),
        compression=CompressionConfig(method="blosc:zstd:3"),
        packing=PackingConfig(enabled=True, bits=16),
        time=TimeConfig(dim="time", append_dim="time"),
        variables=VariableConfig(include=["temperature"], exclude=["humidity"]),
        attrs={"title": "Programmatic Config Demo", "source": "zarrio"}
    )

Validation
----------

Pydantic models automatically validate configurations:

.. code-block:: python

    from zarrio.models import ZarrConverterConfig

    try:
        config = ZarrConverterConfig(
            chunking={"time": 1000, "lat": 1, "lon": 1}  # Will generate warnings
        )
    except ValidationError as e:
        print(f"Configuration error: {e}")

Missing Data Handling Configuration
-----------------------------------

zarrio provides robust missing data handling through the ``MissingDataConfig`` model:

.. code-block:: python

    from zarrio.models import ZarrConverterConfig, MissingDataConfig

    config = ZarrConverterConfig(
        missing_data=MissingDataConfig(
            missing_check_vars="all",      # Check all variables for missing data
            retries_on_missing=3           # Retry up to 3 times if missing data is detected
        )
    )

The ``MissingDataConfig`` supports the following fields:

- **missing_check_vars**: Variables to check for missing values ("all", None, or list of variable names) (default: "all")
- **retries_on_missing**: Number of retries if missing values are encountered (default: 0, range: 0-10)

Missing Data Detection
^^^^^^^^^^^^^^^^^^^^^^

The missing data detection system automatically checks for missing data after writing operations by comparing the source dataset with the written Zarr archive. This is particularly useful for detecting transient failures during parallel processing.

Retry Logic
^^^^^^^^^^^

When missing data is detected and retries are enabled, zarrio automatically retries the operation with exponential backoff:

1. First retry: 0.1 second delay
2. Second retry: 0.2 second delay
3. Third retry: 0.3 second delay
4. And so on...

This approach allows the system to recover from transient issues such as network instability, file system contention, or memory pressure.

Configuration Examples
^^^^^^^^^^^^^^^^^^^^^^

Check all variables with 2 retries:

.. code-block:: python

    config = ZarrConverterConfig(
        missing_data=MissingDataConfig(
            missing_check_vars="all",
            retries_on_missing=2
        )
    )

Check specific variables with 3 retries:

.. code-block:: python

    config = ZarrConverterConfig(
        missing_data=MissingDataConfig(
            missing_check_vars=["temperature", "pressure"],
            retries_on_missing=3
        )
    )

Disable missing data checking:

.. code-block:: python

    config = ZarrConverterConfig(
        missing_data=MissingDataConfig(
            missing_check_vars=None,
            retries_on_missing=0
        )
    )

YAML Configuration
^^^^^^^^^^^^^^^^^^

Missing data handling can also be configured in YAML files:

.. code-block:: yaml

    # config.yaml
    missing_data:
      missing_check_vars: "all"
      retries_on_missing: 3
    chunking:
      time: 100
      lat: 50
      lon: 100

CLI Usage
^^^^^^^^^

Missing data handling options are available through the CLI:

.. code-block:: bash

    # Convert with retry logic
    zarrio convert input.nc output.zarr --retries-on-missing 3

    # Append with retry logic
    zarrio append new_data.nc existing.zarr --retries-on-missing 2

    # Create template with retry logic
    zarrio create-template template.nc archive.zarr --retries-on-missing 1

    # Write region with retry logic
    zarrio write-region data.nc archive.zarr --retries-on-missing 2

Access Pattern Configuration
----------------------------

The ``access_pattern`` field controls how intelligent chunking optimizes the data layout for different access patterns:

- **temporal**: Optimized for time series analysis (large time chunks, smaller spatial chunks)
- **spatial**: Optimized for spatial analysis (smaller time chunks, larger spatial chunks)
- **balanced**: Good performance for mixed workloads (moderate chunking across all dimensions)

When ``intelligent_chunking`` is enabled in the ``create-template`` command, the access pattern determines how the chunking is calculated based on the full archive dimensions.

Programmatic Configuration:

.. code-block:: python

    from zarrio.models import ZarrConverterConfig

    config = ZarrConverterConfig(
        access_pattern="temporal"  # Optimize for time series analysis
    )

YAML Configuration:

.. code-block:: yaml

    # config.yaml
    access_pattern: temporal
    chunking:
      time: 100
      lat: 50
      lon: 100

JSON Configuration:

.. code-block:: json

    {
      "access_pattern": "temporal",
      "chunking": {
        "time": 100,
        "lat": 50,
        "lon": 100
      }
    }

CLI Usage:

.. code-block:: bash

    # Create template with intelligent chunking for temporal analysis
    zarrio create-template template.nc archive.zarr \\
        --global-start 2020-01-01 \\
        --global-end 2023-12-31 \\
        --intelligent-chunking \\
        --access-pattern temporal

    # Create template with intelligent chunking for spatial analysis
    zarrio create-template template.nc archive.zarr \\
        --global-start 2020-01-01 \\
        --global-end 2023-12-31 \\
        --intelligent-chunking \\
        --access-pattern spatial

    # Create template with intelligent chunking for balanced access
    zarrio create-template template.nc archive.zarr \\
        --global-start 2020-01-01 \\
        --global-end 2023-12-31 \\
        --intelligent-chunking \\
        --access-pattern balanced

Best Practices
^^^^^^^^^^^^^^

1. **Moderate Retries**: Use 2-3 retries for most use cases
2. **Aggressive Retries**: Increase for very large or unstable environments
3. **Variable Selection**: Specify ``missing_check_vars`` to focus on critical variables
4. **Monitoring**: Watch logs for frequent retries which might indicate underlying issues
5. **Disabled Retries**: Set to 0 for deterministic environments where failures should be immediate errors

Configuration with ZarrConverter
---------------------------------

Once you have a configuration, you can use it with ZarrConverter:

.. code-block:: python

    from zarrio import ZarrConverter
    from zarrio.models import ZarrConverterConfig

    config = ZarrConverterConfig(
        chunking={"time": 100, "lat": 50, "lon": 100},
        compression="blosc:zstd:3",
        packing=True
    )

    converter = ZarrConverter(config=config)
    converter.convert("input.nc", "output.zarr")

CLI Configuration
-------------------

Configuration files can also be used with the CLI:

.. code-block:: bash

    zarrio convert input.nc output.zarr --config config.yaml

Environment Variables
----------------------

Some configuration can also be set through environment variables:

.. code-block:: bash

    export ONZARR2_LOG_LEVEL=DEBUG
    export ONZARR2_CHUNK_SIZE_TIME=100
    export ONZARR2_COMPRESSION=blosc:zstd:3

Default Configuration
----------------------

zarrio provides sensible defaults for all configuration options:

.. code-block:: python

    from zarrio.models import ZarrConverterConfig

    # Default config
    config = ZarrConverterConfig()

    # Shows default values:
    # - chunking: {}
    # - compression: None
    # - packing: {"enabled": False, "bits": 16}
    # - time: {"dim": "time", "append_dim": "time"}
    # - variables: {"include": None, "exclude": None}
    # - attrs: {}

Best Practices
----------------

1. **Use Configuration Files**: For complex setups, use YAML or JSON configuration files
2. **Validate Early**: Let Pydantic validate your configurations before using them
3. **Type Safety**: Take advantage of type hints for better development experience
4. **Environment Overrides**: Use environment variables for deployment-specific settings
5. **Documentation**: Document your configuration files with comments

Example Configuration File with Comments:
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: yaml

    # config.yaml - Production configuration for zarrio
    #
    # This configuration is optimized for:
    # - Large climate datasets (1° global daily data)
    # - Balanced access patterns (time series and spatial analysis)
    # - High compression ratio
    # - Efficient storage

    # Chunking strategy optimized for balanced access patterns
    chunking:
      time: 100    # 100 time steps per chunk (about 3-4 months)
      lat: 50      # 50 latitude points per chunk (about 50°)
      lon: 100     # 100 longitude points per chunk (about 100°)

    # Compression settings (Blosc with Zstd level 3)
    compression:
      method: blosc:zstd:3
      cname: zstd
      clevel: 3
      shuffle: shuffle

    # Data packing (16-bit packing for float32 data)
    packing:
      enabled: true
      bits: 16

    # Time dimension configuration
    time:
      dim: time
      append_dim: time
      # global_start: "2020-01-01"  # Set at runtime
      # global_end: "2023-12-31"    # Set at runtime
      freq: "1D"

    # Variable selection
    variables:
      include:       # Only include these variables
        - temperature
        - pressure
        - humidity
        - wind_speed
      exclude:       # Exclude these variables (takes precedence)
        - unused_var

    # Global attributes to add
    attrs:
      title: "Production Climate Dataset"
      institution: "Oceanum"
      source: "zarrio"
      processing_date: "2023-01-01"
      version: "1.0"

    # Retry configuration for missing data
    retries_on_missing: 3
    missing_check_vars: all
