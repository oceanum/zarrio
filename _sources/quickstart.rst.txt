Quickstart Guide
================

This guide will help you get started with zarrio quickly.

Basic Usage
-----------

The simplest way to use zarrio is through its functional API:

.. code-block:: python

    from zarrio import convert_to_zarr

    # Convert a single NetCDF file to Zarr
    convert_to_zarr("input.nc", "output.zarr")

This will automatically handle the conversion with sensible defaults.

Advanced Usage
--------------

For more control, you can use the class-based API:

.. code-block:: python

    from zarrio import ZarrConverter

    # Create converter with custom settings
    converter = ZarrConverter(
        chunking={"time": 100, "lat": 50, "lon": 100},
        compression="blosc:zstd:3",
        packing=True,
        packing_bits=16,
        target_chunk_size_mb=100  # Configure target chunk size for your environment
    )

    # Convert data
    converter.convert("input.nc", "output.zarr")

Environment-specific chunking:
- Local development: 10-25 MB chunks
- Production servers: 50-100 MB chunks  
- Cloud environments: 100-200 MB chunks

Command Line Interface
----------------------

zarrio also provides a powerful command-line interface:

.. code-block:: bash

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

    # Convert with automatic range calculation
    zarrio convert input.nc output.zarr --packing \
        --packing-auto-buffer-factor 0.05

Parallel Writing
----------------

One of the key features of zarrio is parallel writing support:

.. code-block:: python

    from zarrio import ZarrConverter

    # 1. Create template covering full time range
    converter = ZarrConverter()
    converter.create_template(
        template_dataset=template_ds,
        output_path="archive.zarr",
        global_start="2020-01-01",
        global_end="2023-12-31",
        compute=False  # Metadata only
    )

    # 2. Write regions in parallel processes
    converter.write_region("data_2020.nc", "archive.zarr")  # Process 1
    converter.write_region("data_2021.nc", "archive.zarr")  # Process 2
    converter.write_region("data_2022.nc", "archive.zarr")  # Process 3
    converter.write_region("data_2023.nc", "archive.zarr")  # Process 4

Configuration Files
------------------

You can also use configuration files (YAML or JSON):

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

Then use it with the CLI:

.. code-block:: bash

    zarrio convert input.nc output.zarr --config config.yaml

Or programmatically:

.. code-block:: python

    from zarrio import ZarrConverter

    converter = ZarrConverter.from_config_file("config.yaml")
    converter.convert("input.nc", "output.zarr")

Next Steps
----------

- Explore the :doc:`api` documentation for detailed API reference
- Learn about :doc:`cli` options
- Understand :doc:`configuration` management
- Discover :doc:`parallel` writing capabilities
- Review :doc:`examples` for more use cases