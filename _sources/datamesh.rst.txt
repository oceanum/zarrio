Datamesh Integration
====================

The zarrio library supports integration with Oceanum's Datamesh platform, allowing you to write Zarr data directly to Datamesh datasources.

Configuration
-------------

To use Datamesh integration, you need to configure the ``datamesh`` section in your ``ZarrConverterConfig``:

.. code-block:: python

    from zarrio import ZarrConverterConfig

    config = ZarrConverterConfig(
        datamesh={
            "datasource": {
                "id": "my_datasource",
                "name": "My Data",
                "description": "My dataset",
                "coordinates": {"x": "longitude", "y": "latitude", "t": "time"},
                "details": "https://example.com",
                "tags": ["zarrio", "datamesh"]
            },
            "token": "your_datamesh_token",
            "service": "https://datamesh-v1.oceanum.io"  # Optional, defaults to this value
        }
    )

Using with the API
------------------

.. code-block:: python

    from zarrio import ZarrConverter

    # Create converter with datamesh configuration
    converter = ZarrConverter(config=config)

    # Convert data to datamesh (output_path is optional when using datamesh)
    converter.convert("input.nc")

Using with the CLI
------------------

You can also use the CLI with datamesh:

.. code-block:: bash

    # Convert to datamesh datasource
    zarrio convert input.nc \\
      --datamesh-datasource '{"id":"my_datasource","name":"My Data","coordinates":{"x":"longitude","y":"latitude","t":"time"}}' \\
      --datamesh-token $DATAMESH_TOKEN

    # Create template for parallel writing
    zarrio create-template template.nc \\
      --datamesh-datasource '{"id":"my_datasource","name":"My Data","coordinates":{"x":"longitude","y":"latitude","t":"time"}}' \\
      --datamesh-token $DATAMESH_TOKEN \\
      --global-start 2023-01-01 \\
      --global-end 2023-12-31

    # Write region to datamesh datasource
    zarrio write-region data.nc \\
      --datamesh-datasource '{"id":"my_datasource","name":"My Data","coordinates":{"x":"longitude","y":"latitude","t":"time"}}' \\
      --datamesh-token $DATAMESH_TOKEN

Datamesh Datasource Configuration
---------------------------------

The ``DatameshDatasource`` model supports the following fields:

- ``id`` (required): The unique identifier for the datasource
- ``name``: Human-readable name for the datasource
- ``description``: Description of the datasource
- ``coordinates``: Coordinate mapping (e.g., ``{"x": "longitude", "y": "latitude", "t": "time"}``)
- ``details``: URL with more details about the datasource
- ``tags``: Tags associated with the datasource
- ``driver``: Driver to use for datamesh datasource (defaults to "vzarr")
- ``dataschema``: Explicit schema for the datasource
- ``geometry``: Explicit geometry for the datasource
- ``tstart``: Explicit start time for the datasource
- ``tend``: Explicit end time for the datasource

Installation
------------

To use datamesh functionality, you need to install the optional datamesh dependencies:

.. code-block:: bash

    pip install zarrio[datamesh]

Advanced Usage
--------------

Configuration File Support
^^^^^^^^^^^^^^^^^^^^^^^^^

You can also configure datamesh integration using YAML or JSON configuration files:

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

Metadata Management
^^^^^^^^^^^^^^^^^^^

When writing to datamesh, zarrio automatically manages metadata:

- **Schema**: Automatically generated from the dataset structure
- **Geometry**: Calculated from coordinate variables
- **Time Range**: Extracted from the time dimension

You can override any of these by explicitly setting them in your configuration:

.. code-block:: python

    config = ZarrConverterConfig(
        datamesh={
            "datasource": {
                "id": "my_datasource",
                "name": "My Data",
                "dataschema": {"custom": "schema"},  # Override auto-generated schema
                "geometry": {"type": "Point", "coordinates": [0, 0]},  # Override auto-generated geometry
                "tstart": "2023-01-01T00:00:00",  # Override auto-detected start time
                "tend": "2023-12-31T23:59:59"  # Override auto-detected end time
            },
            "token": "your_datamesh_token"
        }
    )

Parallel Writing with Datamesh
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

All parallel writing features work with datamesh:

.. code-block:: python

    from zarrio import ZarrConverter

    # Create converter with datamesh configuration
    converter = ZarrConverter(
        chunking={"time": 100, "lat": 50, "lon": 100},
        compression={"method": "blosc:zstd:3"},
        datamesh={
            "datasource": {
                "id": "my_parallel_data",
                "name": "Parallel Climate Data",
                "coordinates": {"x": "lon", "y": "lat", "t": "time"}
            },
            "token": "your_datamesh_token"
        }
    )

    # Create template covering full time range
    template_ds = xr.open_dataset("template.nc")
    converter.create_template(
        template_dataset=template_ds,
        global_start="2020-01-01",
        global_end="2023-12-31",
        compute=False  # Metadata only
    )

    # Write regions in parallel processes
    converter.write_region("data_2020.nc")  # Process 1
    converter.write_region("data_2021.nc")  # Process 2
    converter.write_region("data_2022.nc")  # Process 3
    converter.write_region("data_2023.nc")  # Process 4

.. code-block:: bash

    # CLI equivalent
    zarrio create-template template.nc \\
      --datamesh-datasource '{"id":"my_parallel_data","name":"Parallel Climate Data","coordinates":{"x":"lon","y":"lat","t":"time"}}' \\
      --datamesh-token $DATAMESH_TOKEN \\
      --global-start 2020-01-01 \\
      --global-end 2023-12-31

    # In parallel processes:
    zarrio write-region data_2020.nc --datamesh-token $DATAMESH_TOKEN  # Process 1
    zarrio write-region data_2021.nc --datamesh-token $DATAMESH_TOKEN  # Process 2
    zarrio write-region data_2022.nc --datamesh-token $DATAMESH_TOKEN  # Process 3
    zarrio write-region data_2023.nc --datamesh-token $DATAMESH_TOKEN  # Process 4

Error Handling
^^^^^^^^^^^^^^

Datamesh integration includes proper error handling:

.. code-block:: python

    from zarrio.exceptions import ConversionError

    try:
        converter.convert("input.nc")
    except ConversionError as e:
        print(f"Conversion failed: {e}")
    except Exception as e:
        print(f"Unexpected error: {e}")

Best Practices
--------------

1. **Token Management**: Store your datamesh token securely, preferably as an environment variable:

   .. code-block:: bash

       export DATAMESH_TOKEN="your_actual_token_here"
       zarrio convert input.nc --datamesh-token $DATAMESH_TOKEN ...

2. **Datasource IDs**: Use descriptive, unique IDs for your datasources.

3. **Coordinate Mapping**: Ensure your coordinate mapping matches your dataset's variable names.

4. **Testing**: Test your configuration with small datasets before processing large amounts of data.

5. **Metadata**: Provide meaningful names, descriptions, and tags to make your datasources discoverable.

Example Workflow
----------------

Here's a complete example workflow:

.. code-block:: python

    import os
    from zarrio import ZarrConverter, ZarrConverterConfig

    # 1. Configure for datamesh
    config = ZarrConverterConfig(
        chunking={"time": 100, "lat": 50, "lon": 100},
        compression={"method": "blosc:zstd:3"},
        packing={"enabled": True, "bits": 16},
        datamesh={
            "datasource": {
                "id": "climate_data_2023",
                "name": "Climate Data 2023",
                "description": "Global climate data for 2023",
                "coordinates": {"x": "longitude", "y": "latitude", "t": "time"},
                "details": "https://example.com/climate-data-2023",
                "tags": ["climate", "2023", "global", "temperature", "pressure"]
            },
            "token": os.getenv("DATAMESH_TOKEN"),  # Use environment variable
            "service": "https://datamesh-v1.oceanum.io"
        }
    )

    # 2. Create converter
    converter = ZarrConverter(config=config)

    # 3. Convert data
    converter.convert("climate_data_2023.nc")

    print("Data successfully written to datamesh!")