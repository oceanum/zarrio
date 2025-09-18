Usage Examples
============

This section provides practical examples of using zarrio for various scenarios.

Basic Conversion
------------------

Simple conversion of a NetCDF file to Zarr format:

.. code-block:: python

    from zarrio import convert_to_zarr

    # Convert a single NetCDF file to Zarr
    convert_to_zarr("input.nc", "output.zarr")

    # Convert with basic options
    convert_to_zarr(
        "input.nc",
        "output.zarr",
        chunking={"time": 100, "lat": 50, "lon": 100},
        compression="blosc:zstd:3",
        packing=True,
        packing_bits=16
    )

Advanced Conversion with Class-Based API
------------------------------------------

For more control, use the class-based API:

.. code-block:: python

    from zarrio import ZarrConverter
    from zarrio.models import ZarrConverterConfig

    # Create configuration
    config = ZarrConverterConfig(
        chunking=ChunkingConfig(time=100, lat=50, lon=100),
        compression=CompressionConfig(method="blosc:zstd:3"),
        packing=PackingConfig(enabled=True, bits=16),
        attrs={"title": "Demo dataset", "source": "zarrio"}
    )

    # Create converter
    converter = ZarrConverter(config=config)

    # Convert data
    converter.convert("input.nc", "output.zarr")

Command-Line Interface
-----------------------

zarrio provides a powerful command-line interface:

.. code-block:: bash

    # Convert NetCDF to Zarr
    zarrio convert input.nc output.zarr

    # Convert with chunking
    zarrio convert input.nc output.zarr --chunking "time:100,lat:50,lon:100"

    # Convert with compression
    zarrio convert input.nc output.zarr --compression "blosc:zstd:3"

    # Convert with data packing
    zarrio convert input.nc output.zarr --packing --packing-bits 16

    # Convert with variable selection
    zarrio convert input.nc output.zarr --variables "temperature,pressure"

    # Convert with variable exclusion
    zarrio convert input.nc output.zarr --drop-variables "humidity"

    # Convert with additional attributes
    zarrio convert input.nc output.zarr --attrs '{"title": "Demo dataset", "source": "zarrio"}'

Parallel Processing
-------------------

One of the key features of zarrio is parallel processing support:

Template Creation
~~~~~~~~~~~~~~~~~~~

First, create a template Zarr archive covering the full time range:

.. code-block:: python

    from zarrio import ZarrConverter

    # Create converter
    converter = ZarrConverter(
        chunking=ChunkingConfig(time=100, lat=50, lon=100),
        compression=CompressionConfig(method="blosc:zstd:3"),
        packing=PackingConfig(enabled=True, bits=16)
    )

    # Create template covering full time range
    converter.create_template(
        template_dataset=template_ds,
        output_path="archive.zarr",
        global_start="2020-01-01",
        global_end="2023-12-31",
        compute=False  # Metadata only, no data computation
    )

Region Writing
~~~~~~~~~~~~~

Then write regions in parallel processes:

.. code-block:: python

    # Process 1: Write first region
    converter.write_region("data_2020.nc", "archive.zarr")

    # Process 2: Write second region
    converter.write_region("data_2021.nc", "archive.zarr")

    # Process 3: Write third region
    converter.write_region("data_2022.nc", "archive.zarr")

    # Process 4: Write fourth region
    converter.write_region("data_2023.nc", "archive.zarr")

CLI Parallel Processing
~~~~~~~~~~~~~~~~~~~~~~

You can also use the CLI for parallel processing:

.. code-block:: bash

    # Create template for parallel writing
    zarrio create-template template.nc archive.zarr \\
        --global-start 2020-01-01 \\
        --global-end 2023-12-31

    # Write regions in parallel processes
    zarrio write-region data1.nc archive.zarr  # Process 1
    zarrio write-region data2.nc archive.zarr  # Process 2
    zarrio write-region data3.nc archive.zarr  # Process 3
    zarrio write-region data4.nc archive.zarr  # Process 4

Data Appending
-------------

Append new data to existing Zarr stores:

.. code-block:: python

    from zarrio import append_to_zarr

    # Append data to existing Zarr store
    append_to_zarr("new_data.nc", "existing.zarr")

    # Append with options
    append_to_zarr(
        "new_data.nc",
        "existing.zarr",
        chunking={"time": 50, "lat": 25, "lon": 50},
        variables=["temperature", "pressure"],
        drop_variables=["humidity"]
    )

Class-Based Appending
~~~~~~~~~~~~~~~~~~~

.. code-block:: python

    from zarrio import ZarrConverter

    # Create converter
    converter = ZarrConverter(
        chunking=ChunkingConfig(time=50, lat=25, lon=50)
    )

    # Append data
    converter.append("new_data.nc", "existing.zarr")

CLI Appending
~~~~~~~~~~~

.. code-block:: bash

    # Append data to existing Zarr store
    zarrio append new_data.nc existing.zarr

    # Append with options
    zarrio append new_data.nc existing.zarr \\
        --chunking "time:50,lat:25,lon:50" \\
        --variables "temperature,pressure" \\
        --drop-variables "humidity"

Configuration Files
-----------------

Use YAML or JSON configuration files:

YAML Configuration
~~~~~~~~~~~~~~~~

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

Usage:
~~~~~~

.. code-block:: python

    from zarrio import ZarrConverter

    # Load from YAML file
    converter = ZarrConverter.from_config_file("config.yaml")
    converter.convert("input.nc", "output.zarr")

.. code-block:: bash

    # Use with CLI
    zarrio convert input.nc output.zarr --config config.yaml

JSON Configuration
~~~~~~~~~~~~~~~~

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
      "variables": {
        "include": ["temperature", "pressure"],
        "exclude": ["humidity"]
      },
      "attrs": {
        "title": "JSON Config Demo",
        "version": "1.0"
      }
    }

Usage:
~~~~~~

.. code-block:: python

    from zarrio import ZarrConverter

    # Load from JSON file
    converter = ZarrConverter.from_config_file("config.json")
    converter.convert("input.nc", "output.zarr")

.. code-block:: bash

    # Use with CLI
    zarrio convert input.nc output.zarr --config config.json

Intelligent Chunking
------------------

zarrio provides automatic chunking analysis:

.. code-block:: python

    from zarrio import convert_to_zarr

    # No chunking specified - automatic analysis
    convert_to_zarr(
        "climate_data.nc",
        "climate_data.zarr",
        access_pattern="balanced"  # Optimize for mixed workloads
    )

    # Temporal analysis optimized
    convert_to_zarr(
        "climate_data.nc",
        "climate_data.zarr",
        access_pattern="temporal"  # Optimize for time series analysis
    )

    # Spatial analysis optimized
    convert_to_zarr(
        "climate_data.nc",
        "climate_data.zarr",
        access_pattern="spatial"  # Optimize for spatial analysis
    )

Advanced Features
----------------

Retry Logic for Missing Data
~~~~~~~~~~~~~~~~~~~~~~~~~

Handle transient issues with automatic retries:

.. code-block:: python

    from zarrio import ZarrConverter
    from zarrio.models import ZarrConverterConfig

    # Configure retries for missing data
    config = ZarrConverterConfig(
        retries_on_missing=3,  # Retry up to 3 times
        missing_check_vars="all"
    )

    converter = ZarrConverter(config=config)
    converter.write_region("data.nc", "archive.zarr")

Data Packing with Validation
~~~~~~~~~~~~~~~~~~~~~~~~~

Pack data with automatic validation and warnings:

.. code-block:: python

    from zarrio import ZarrConverter
    from zarrio.models import ZarrConverterConfig, PackingConfig

    # Enable data packing with validation
    config = ZarrConverterConfig(
        packing=PackingConfig(enabled=True, bits=16)
    )

    converter = ZarrConverter(config=config)

    # Add valid range attributes for packing
    ds = xr.open_dataset("input.nc")
    ds["temperature"].attrs["valid_min"] = 0.0
    ds["temperature"].attrs["valid_max"] = 100.0
    ds["pressure"].attrs["valid_min"] = 900.0
    ds["pressure"].attrs["valid_max"] = 1100.0
    ds.to_netcdf("input_with_valid_range.nc")

    # Convert with packing
    converter.convert("input_with_valid_range.nc", "output.zarr")

Complete Workflow Example
-----------------------

Here's a complete example showing a typical workflow:

.. code-block:: python

    import xarray as xr
    import numpy as np
    import pandas as pd
    from zarrio import ZarrConverter
    from zarrio.models import (
        ZarrConverterConfig,
        ChunkingConfig,
        PackingConfig,
        CompressionConfig
    )

    # 1. Create sample data (in practice, this would come from NetCDF files)
    def create_sample_data(filename: str, start_date: str, periods: int) -> str:
        """Create sample climate data."""
        times = pd.date_range(start_date, periods=periods)
        lats = np.linspace(-90, 90, 180)
        lons = np.linspace(-180, 180, 360)

        # Create realistic climate data
        np.random.seed(42)  # For reproducible results
        temperature = 20 + 15 * np.sin(2 * np.pi * np.arange(periods) / 365)  # Seasonal cycle
        temperature = temperature[:, np.newaxis, np.newaxis] + 10 * np.random.random([periods, 180, 360])

        pressure = 1013 + 50 * np.random.random([periods, 180, 360])

        # Create dataset
        ds = xr.Dataset(
            {
                "temperature": (("time", "lat", "lon"), temperature),
                "pressure": (("time", "lat", "lon"), pressure * 1000),
            },
            coords={
                "time": times,
                "lat": lats,
                "lon": lons,
            },
        )

        # Add metadata
        ds.attrs["title"] = "Sample Climate Dataset"
        ds.attrs["institution"] = "zarrio Demo"
        ds["temperature"].attrs["units"] = "degC"
        ds["pressure"].attrs["units"] = "hPa"

        # Save as NetCDF
        ds.to_netcdf(filename)
        return filename

    # 2. Create sample data files for demonstration
    with tempfile.TemporaryDirectory() as tmpdir:
        # Create annual data files
        files = []
        for year in range(2020, 2024):
            ncfile = os.path.join(tmpdir, f"data_{year}.nc")
            create_sample_data(ncfile, f"{year}-01-01", 365)
            files.append(ncfile)

        # 3. Create template for parallel writing
        config = ZarrConverterConfig(
            chunking=ChunkingConfig(time=100, lat=50, lon=100),
            compression=CompressionConfig(method="blosc:zstd:3"),
            packing=PackingConfig(enabled=True, bits=16),
            retries_on_missing=3,
            missing_check_vars="all"
        )

        converter = ZarrConverter(config=config)

        # Create template covering full time range
        zarr_archive = os.path.join(tmpdir, "climate_archive.zarr")
        template_ds = xr.open_dataset(files[0])
        converter.create_template(
            template_dataset=template_ds,
            output_path=zarr_archive,
            global_start="2020-01-01",
            global_end="2023-12-31",
            compute=False
        )

        # 4. Write regions in parallel (simulated)
        for ncfile in files:
            print(f"Writing {os.path.basename(ncfile)}...")
            converter.write_region(ncfile, zarr_archive)

        # 5. Verify the final archive
        final_ds = xr.open_zarr(zarr_archive)
        print(f"Final archive: {len(final_ds.time)} time steps")
        print(f"Variables: {list(final_ds.data_vars.keys())}")

        print("Complete workflow example finished successfully!")

Error Handling
---------------

zarrio provides comprehensive error handling:

.. code-block:: python

    from zarrio.exceptions import ConversionError, PackingError, ConfigurationError

    try:
        convert_to_zarr("input.nc", "output.zarr")
    except ConversionError as e:
        print(f"Conversion failed: {e}")
    except PackingError as e:
        print(f"Packing failed: {e}")
    except ConfigurationError as e:
        print(f"Configuration error: {e}")
    except Exception as e:
        print(f"Unexpected error: {e}")

Performance Optimization
-------------------------

Optimize for different scenarios:

.. code-block:: python

    # For large datasets with limited memory
    config = ZarrConverterConfig(
        chunking=ChunkingConfig(time=50, lat=25, lon=50),  # Smaller chunks
        compression=CompressionConfig(method="blosc:zstd:1")  # Lower compression for speed
    )

    # For high-compression scenarios
    config = ZarrConverterConfig(
        chunking=ChunkingConfig(time=200, lat=100, lon=200),  # Larger chunks for better compression
        compression=CompressionConfig(method="blosc:zstd:9")  # Higher compression
        packing=PackingConfig(enabled=True, bits=8)  # 8-bit packing for maximum compression
    )

    # For parallel processing
    config = ZarrConverterConfig(
        chunking=ChunkingConfig(time=100, lat=50, lon=100),  # Balanced chunks
        compression=CompressionConfig(method="blosc:zstd:3"),  # Balanced compression
        packing=PackingConfig(enabled=True, bits=16),  # 16-bit packing for good balance
        retries_on_missing=3  # Enable retries for parallel reliability
    )

Logging and Debugging
-----------------------

Enable detailed logging for debugging:

.. code-block:: python

    import logging

    # Setup logging
    logging.basicConfig(
        level=logging.DEBUG,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )

    # Convert with verbose logging
    convert_to_zarr("input.nc", "output.zarr")

.. code-block:: bash

    # CLI with verbose logging
    zarrio convert input.nc output.zarr -vvv

The logs will show:
- Processing steps
- Configuration validation
- Chunking analysis
- Compression and packing
- I/O operations
- Performance metrics
- Error details

Datamesh Integration
--------------------

zarrio supports integration with Oceanum's Datamesh platform:

.. code-block:: python

    from zarrio import ZarrConverter, ZarrConverterConfig

    # Configure for datamesh
    config = ZarrConverterConfig(
        datamesh={
            "datasource": {
                "id": "my_climate_data",
                "name": "My Climate Data",
                "description": "Climate data converted with zarrio",
                "coordinates": {"x": "lon", "y": "lat", "t": "time"},
                "details": "https://example.com",
                "tags": ["climate", "zarrio", "datamesh"],
            },
            "token": "your_datamesh_token",
            "service": "https://datamesh-v1.oceanum.io",
        },
        chunking={"time": 100, "lat": 50, "lon": 100},
        compression={"method": "blosc:zstd:3"},
    )

    # Create converter
    converter = ZarrConverter(config=config)

    # Convert data directly to datamesh (no output_path needed)
    converter.convert("input.nc")

CLI Datamesh Integration
~~~~~~~~~~~~~~~~~~~~~~~~

Use the CLI with datamesh:

.. code-block:: bash

    # Convert to datamesh datasource
    zarrio convert input.nc \
      --datamesh-datasource '{"id":"my_climate_data","name":"My Climate Data","coordinates":{"x":"lon","y":"lat","t":"time"}}' \
      --datamesh-token $DATAMESH_TOKEN

    # Create template for parallel writing
    zarrio create-template template.nc \
      --datamesh-datasource '{"id":"my_climate_data","name":"My Climate Data","coordinates":{"x":"lon","y":"lat","t":"time"}}' \
      --datamesh-token $DATAMESH_TOKEN \
      --global-start 2023-01-01 \
      --global-end 2023-12-31

    # Write region to datamesh datasource
    zarrio write-region data.nc \
      --datamesh-datasource '{"id":"my_climate_data","name":"My Climate Data","coordinates":{"x":"lon","y":"lat","t":"time"}}' \
      --datamesh-token $DATAMESH_TOKEN

Configuration File with Datamesh
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Use YAML configuration with datamesh:

.. code-block:: yaml

    # config.yaml
    chunking:
      time: 100
      lat: 50
      lon: 100
    compression:
      method: blosc:zstd:3
    datamesh:
      datasource:
        id: my_climate_data
        name: My Climate Data
        description: Climate data converted with zarrio
        coordinates:
          x: lon
          y: lat
          t: time
        details: https://example.com
        tags:
          - climate
          - zarrio
          - datamesh
      token: your_datamesh_token
      service: https://datamesh-v1.oceanum.io

.. code-block:: python

    # Load from YAML file
    converter = ZarrConverter.from_config_file("config.yaml")
    converter.convert("input.nc")

.. code-block:: bash

    # Use with CLI
    zarrio convert input.nc --config config.yaml
