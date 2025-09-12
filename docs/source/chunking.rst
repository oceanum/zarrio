Chunking Strategies
====================

zarrify provides intelligent chunking analysis and recommendations to optimize performance for different access patterns.

Understanding Chunking
------------------------

Chunking is the process of dividing large datasets into smaller, more manageable pieces for efficient storage and retrieval. In Zarr format, chunks are the fundamental unit of storage and access.

Why Chunking Matters:
~~~~~~~~~~~~~~~~~~~~~

1. **I/O Performance**: Chunks that align with your access patterns improve performance
2. **Memory Usage**: Smaller chunks reduce memory requirements
3. **Compression**: Larger chunks often compress better
4. **Parallel Processing**: Chunks are the unit of parallelization
5. **Network Transfer**: Appropriate chunk sizes optimize network I/O

Chunk Size Considerations:
~~~~~~~~~~~~~~~~~~~~~~~~~~

- **Too Small**: Increases metadata overhead and reduces compression efficiency
- **Too Large**: Increases memory usage and reduces parallelization benefits
- **Just Right**: Balances all factors for optimal performance

Recommended Chunk Sizes:
~~~~~~~~~~~~~~~~~~~~~~~~~

- **Minimum**: 1 MB per chunk
- **Target**: 10-100 MB per chunk
- **Maximum**: 100 MB per chunk

Intelligent Chunking Analysis
-------------------------------

zarrify automatically analyzes your dataset and provides chunking recommendations based on expected access patterns:

.. code-block:: python

    from zarrify import convert_to_zarr

    # No chunking specified - automatic analysis
    convert_to_zarr(
        "climate_data.nc",
        "climate_data.zarr",
        access_pattern="balanced"  # Optimize for mixed workloads
    )

The system analyzes:
- Dataset dimensions and sizes
- Data type and element size
- Expected access patterns
- Storage characteristics

Access Pattern Optimization
----------------------------

Different access patterns require different chunking strategies:

Temporal Analysis
~~~~~~~~~~~~~~~~~~

Optimized for time series extraction (specific locations over long time periods):

.. code-block:: python

    # Temporal analysis optimized
    temporal_config = ZarrConverterConfig(
        chunking=ChunkingConfig(
            time=100,   # Large time chunks (e.g., 100 time steps)
            lat=30,     # Smaller spatial chunks
            lon=60      # Smaller spatial chunks
        ),
        attrs={"access_pattern": "temporal_analysis"}
    )

    converter = ZarrConverter(config=temporal_config)
    converter.convert("climate_data.nc", "temporal_archive.zarr")

Benefits:
- Fewer I/O operations for long time series
- Efficient access to temporal data
- Good compression for time-aligned data

Spatial Analysis
~~~~~~~~~~~~~~~~~~~

Optimized for spatial analysis (maps at specific times):

.. code-block:: python

    # Spatial analysis optimized
    spatial_config = ZarrConverterConfig(
        chunking=ChunkingConfig(
            time=20,    # Smaller time chunks
            lat=100,    # Large spatial chunks
            lon=100     # Large spatial chunks
        ),
        attrs={"access_pattern": "spatial_analysis"}
    )

    converter = ZarrConverter(config=spatial_config)
    converter.convert("climate_data.nc", "spatial_archive.zarr")

Benefits:
- Efficient spatial subsetting
- Better cache locality for spatial operations
- Optimized for map generation

Balanced Approach
~~~~~~~~~~~~~~~~~~~~

Good performance for mixed workloads:

.. code-block:: python

    # Balanced approach
    balanced_config = ZarrConverterConfig(
        chunking=ChunkingConfig(
            time=50,    # Moderate time chunks
            lat=50,     # Moderate spatial chunks
            lon=50      # Moderate spatial chunks
        ),
        attrs={"access_pattern": "balanced"}
    )

    converter = ZarrConverter(config=balanced_config)
    converter.convert("climate_data.nc", "balanced_archive.zarr")

Benefits:
- Reasonable performance for diverse access patterns
- Good compromise between temporal and spatial access
- Suitable for exploratory analysis

Chunking Recommendations by Resolution
---------------------------------------

Low Resolution (1° or coarser)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

    # For low-resolution global data (e.g., 1° global daily)
    low_res_config = ZarrConverterConfig(
        chunking=ChunkingConfig(
            time=200,   # Larger time chunks acceptable
            lat=90,     # Latitude chunks (~1° per chunk)
            lon=180     # Longitude chunks (~1° per chunk)
        )
    )

Medium Resolution (0.25° to 1°)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

    # For medium-resolution regional data (e.g., 0.25° regional daily)
    med_res_config = ZarrConverterConfig(
        chunking=ChunkingConfig(
            time=100,   # Moderate time chunks
            lat=50,     # Latitude chunks (~1.8° per chunk)
            lon=100     # Longitude chunks (~1.8° per chunk)
        )
    )

High Resolution (0.1° or finer)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

    # For high-resolution local data (e.g., 0.1° local hourly)
    high_res_config = ZarrConverterConfig(
        chunking=ChunkingConfig(
            time=50,    # Smaller time chunks to limit size
            lat=25,     # Latitude chunks (~2.5° per chunk)
            lon=50      # Longitude chunks (~5° per chunk)
        )
    )

Chunking Validation
---------------------

zarrify validates user-provided chunking and provides warnings for suboptimal configurations:

.. code-block:: python

    # Suboptimal chunking that will generate warnings
    bad_config = ZarrConverterConfig(
        chunking=ChunkingConfig(
            time=1000,  # Too large
            lat=1,      # Too small
            lon=1       # Too small
        )
    )

    converter = ZarrConverter(config=bad_config)
    # Logs warnings about:
    # - Chunk size (3.8 MB) exceeds maximum recommended size (100 MB)
    # - Chunk size (0.0 MB) is below minimum recommended size (1 MB)

Best Practices
----------------

1. **Match Access Patterns**: Align chunks with your typical data access
2. **Consider Compression**: Larger chunks often compress better
3. **Balance Chunk Count**: Too many chunks increase metadata overhead
4. **Memory Constraints**: Ensure chunks fit comfortably in memory
5. **Storage Backend**: Consider characteristics of your storage system

Example Best Practices:
~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

    # Good chunking for climate data
    climate_config = ZarrConverterConfig(
        chunking=ChunkingConfig(
            time=100,   # About 3-4 months of daily data
            lat=50,     # About 50° of latitude
            lon=100     # About 100° of longitude
        )
    )

    # Good chunking for high-resolution data
    high_res_config = ZarrConverterConfig(
        chunking=ChunkingConfig(
            time=24,    # About 1 day of hourly data
            lat=25,     # About 2.5° of latitude
            lon=50      # About 5° of longitude
        )
    )

    # Good chunking for very large dimensions
    large_dim_config = ZarrConverterConfig(
        chunking=ChunkingConfig(
            time=50,    # Balance I/O and memory
            lat=100,    # Larger chunks for better compression
            lon=100     # Larger chunks for better compression
        )
    )

Chunking with Parallel Processing
-----------------------------------

When using parallel processing, consider chunking that aligns with your parallelization strategy:

.. code-block:: python

    # Template creation with chunking
    converter = ZarrConverter(
        config=ZarrConverterConfig(
            chunking=ChunkingConfig(
                time=100,   # Large time chunks for temporal analysis
                lat=50,     # Moderate spatial chunks
                lon=100     # Moderate spatial chunks
            )
        )
    )

    # Create template for parallel writing
    converter.create_template(
        template_dataset=template_ds,
        output_path="parallel_archive.zarr",
        global_start="2020-01-01",
        global_end="2023-12-31",
        compute=False
    )

    # Each parallel process writes to different time regions
    # but with the same chunking strategy for consistency

Configuration File Example
----------------------------

YAML configuration with chunking recommendations:

.. code-block:: yaml

    # config.yaml
    chunking:
      time: 150      # About 5 months of daily data
      lat: 60        # About 60° of latitude
      lon: 120       # About 120° of longitude
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
        - humidity
      exclude:
        - unused_var
    attrs:
      title: YAML Config with Chunking
      version: 1.0
      access_pattern: balanced
    time:
      dim: time
      append_dim: time
      global_start: "2020-01-01"
      global_end: "2023-12-31"
      freq: "1D"

CLI Usage
---------

Command-line interface with chunking:

.. code-block:: bash

    # Convert with chunking
    zarrify convert input.nc output.zarr \
        --chunking "time:100,lat:50,lon:100"

    # Convert with configuration file
    zarrify convert input.nc output.zarr \
        --config config.yaml

    # Convert with automatic analysis
    zarrify convert input.nc output.zarr \
        --access-pattern balanced

Performance Monitoring
------------------------

Monitor chunking performance:

.. code-block:: python

    import logging

    # Enable performance logging
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )

    # Convert with verbose logging
    converter = ZarrConverter(
        config=ZarrConverterConfig(
            chunking=ChunkingConfig(time=100, lat=50, lon=100)
        )
    )

    converter.convert("input.nc", "output.zarr")

The logs will show:
- Chunk size information
- Compression ratios
- I/O performance metrics
- Memory usage statistics

Troubleshooting
----------------

Common chunking issues and solutions:

1. **Poor Performance**: Check if chunks align with access patterns
2. **Memory Issues**: Reduce chunk sizes
3. **Metadata Overhead**: Increase chunk sizes
4. **Compression Problems**: Adjust chunk sizes for better ratios

Example Troubleshooting:
~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

    # Enable debug logging for chunking analysis
    import logging
    logging.basicConfig(level=logging.DEBUG)

    # Convert with verbose chunking analysis
    convert_to_zarr(
        "input.nc",
        "output.zarr",
        access_pattern="balanced",
        chunking={"time": 100, "lat": 50, "lon": 100}
    )

This will provide detailed information about:
- Chunk size calculations
- Memory usage estimates
- Compression effectiveness
- Performance recommendations
