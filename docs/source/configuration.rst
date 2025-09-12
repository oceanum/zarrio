Configuration Management
========================

zarrify provides flexible configuration management using Pydantic models for type safety and validation.

Pydantic Models
----------------

All configuration in zarrify is managed through Pydantic models, which provide:

- Type safety with automatic validation
- Clear error messages for invalid configurations
- Support for nested configurations
- Serialization to/from YAML and JSON

ZarrConverterConfig
--------------------

The main configuration class is `ZarrConverterConfig`:

.. code-block:: python

    from zarrify.models import ZarrConverterConfig

    config = ZarrConverterConfig(
        chunking={"time": 100, "lat": 50, "lon": 100},
        compression={"method": "blosc:zstd:3"},
        packing={"enabled": True, "bits": 16},
        time={"dim": "time", "append_dim": "time"},
        variables={"include": ["temperature"], "exclude": ["humidity"]},
        attrs={"title": "Demo dataset", "source": "zarrify"}
    )

Chunking Configuration
-----------------------

Chunking can be configured through the `ChunkingConfig` model:

.. code-block:: python

    from zarrify.models import ChunkingConfig

    chunking = ChunkingConfig(
        time=100,
        lat=50,
        lon=100
    )

Packing Configuration
----------------------

Packing can be configured through the `PackingConfig` model:

.. code-block:: python

    from zarrify.models import PackingConfig

    packing = PackingConfig(
        enabled=True,
        bits=16
    )

Compression Configuration
--------------------------

Compression can be configured through the `CompressionConfig` model:

.. code-block:: python

    from zarrify.models import CompressionConfig

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

    from zarrify.models import TimeConfig

    time_config = TimeConfig(
        dim="time",
        append_dim="time",
        global_start="2020-01-01",
        global_end="2023-12-31",
        freq="1D"
    )

Variable Configuration
-----------------------

Variable handling can be configured through the `VariableConfig` model:

.. code-block:: python

    from zarrify.models import VariableConfig

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

    from zarrify.models import ZarrConverterConfig

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

    from zarrify.models import ZarrConverterConfig

    config = ZarrConverterConfig.from_json_file("config.json")

Programmatic Configuration
---------------------------

Configuration can also be created programmatically:

.. code-block:: python

    from zarrify.models import (
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
        attrs={"title": "Programmatic Config Demo", "source": "zarrify"}
    )

Validation
----------

Pydantic models automatically validate configurations:

.. code-block:: python

    from zarrify.models import PackingConfig

    # This will raise a validation error
    try:
        packing = PackingConfig(bits=12)  # Invalid bits value
    except Exception as e:
        print(f"Validation error: {e}")

    # This is valid
    packing = PackingConfig(bits=16)  # Valid bits value

Configuration with ZarrConverter
---------------------------------

Once you have a configuration, you can use it with ZarrConverter:

.. code-block:: python

    from zarrify import ZarrConverter
    from zarrify.models import ZarrConverterConfig

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

    zarrify convert input.nc output.zarr --config config.yaml

Environment Variables
----------------------

Some configuration can also be set through environment variables:

.. code-block:: bash

    export ONZARR2_LOG_LEVEL=DEBUG
    export ONZARR2_CHUNK_SIZE_TIME=100
    export ONZARR2_COMPRESSION=blosc:zstd:3

Default Configuration
----------------------

zarrify provides sensible defaults for all configuration options:

.. code-block:: python

    from zarrify.models import ZarrConverterConfig

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

    # config.yaml - Production configuration for zarrify
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
      source: "zarrify"
      processing_date: "2023-01-01"
      version: "1.0"

    # Retry configuration for missing data
    retries_on_missing: 3
    missing_check_vars: all
