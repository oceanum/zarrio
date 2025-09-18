Chunking Strategies
====================

zarrio provides intelligent chunking analysis and recommendations to optimize performance for different access patterns. This document explains both the conceptual approach and detailed mathematical calculations used for each access pattern.

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

Configurable Target Chunk Size
-------------------------------

zarrio allows you to configure the target chunk size for different environments:

.. code-block:: python

    from zarrio.chunking import get_chunk_recommendation

    # Configure target chunk size (default is 50 MB)
    recommendation = get_chunk_recommendation(
        dimensions={"time": 1000, "lat": 500, "lon": 1000},
        dtype_size_bytes=4,
        access_pattern="balanced",
        target_chunk_size_mb=100  # 100 MB target chunks
    )

Environment-specific recommendations:
- **Local development**: 10-25 MB chunks
- **Production servers**: 50-100 MB chunks
- **Cloud environments**: 100-200 MB chunks

Configuration Methods:
~~~~~~~~~~~~~~~~~~~~~~

1. **Function Arguments**:
   ``get_chunk_recommendation(..., target_chunk_size_mb=100)``

2. **Environment Variables**:
   ``ZARRIFY_TARGET_CHUNK_SIZE_MB=200``

3. **ZarrConverter Configuration**:
   .. code-block:: python
   
       from zarrio.models import ZarrConverterConfig
       
       config = ZarrConverterConfig(target_chunk_size_mb=100)

Intelligent Chunking Analysis
-------------------------------

zarrio automatically analyzes your dataset and provides chunking recommendations based on expected access patterns. The system performs detailed mathematical calculations to optimize chunk sizes for your specific data characteristics.

When creating templates for parallel processing with global start and end times, zarrio can perform intelligent chunking based on the full archive dimensions rather than just the template dataset. This ensures optimal chunking for the entire archive.

The system analyzes:
- Dataset dimensions and sizes
- Data type and element size
- Expected access patterns
- Storage characteristics

Detailed calculations for each access pattern are explained in the Access Pattern Optimization section below.

Access Pattern Optimization
----------------------------

Different access patterns require different chunking strategies. The calculations for each pattern are detailed below:

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

How it works:
- Calculates large time chunks (~10% of total time steps, capped at 100)
- Distributes remaining space across spatial dimensions with a minimum of 10 elements per dimension
- Optimizes for extracting long time series at specific locations

Detailed Calculation:
The temporal focus algorithm uses the following mathematical approach:

1. **Time Chunk Calculation**:
   ``time_chunk = min(100, max(10, time_dimension_size // 10))``

2. **Spatial Chunk Calculation**:
   ``target_elements = (target_chunk_size_mb * 1024²) / dtype_size_bytes``
   ``spatial_elements_per_dim = target_elements / time_chunk``
   ``spatial_chunk_per_dim = (spatial_elements_per_dim)^(1/num_spatial_dims)``
   ``spatial_chunk = max(10, min(spatial_chunk_per_dim, spatial_dimension_size))``

Example:
For a dataset with 1000 time steps, 180 lat points, 360 lon points:
- time_chunk = min(100, max(10, 1000//10)) = 100
- spatial_elements_per_dim = (50 * 1024² / 4) / 100 = 131,072
- spatial_chunk_per_dim = √131,072 ≈ 362
- lat_chunk = min(362, 180) = 180
- lon_chunk = min(362, 360) = 360

Result: time=100, lat=180, lon=360 chunks

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

How it works:
- Calculates small time chunks (~2% of total time steps, capped at 20)
- Distributes remaining space across spatial dimensions with a minimum of 50 elements per dimension
- Optimizes for extracting spatial maps at specific time steps

Detailed Calculation:
The spatial focus algorithm uses the following mathematical approach:

1. **Time Chunk Calculation**:
   ``time_chunk = min(20, max(5, time_dimension_size // 50))``

2. **Spatial Chunk Calculation**:
   ``target_elements = (target_chunk_size_mb * 1024²) / dtype_size_bytes``
   ``spatial_elements_per_dim = target_elements / time_chunk``
   ``spatial_chunk_per_dim = (spatial_elements_per_dim)^(1/num_spatial_dims)``
   ``spatial_chunk = max(50, min(spatial_chunk_per_dim, spatial_dimension_size))``

Example:
For a dataset with 365 time steps, 720 lat points, 1440 lon points:
- time_chunk = min(20, max(5, 365//50)) = min(20, 7) = 7
- spatial_elements_per_dim = (50 * 1024² / 4) / 7 = 1,872,457
- spatial_chunk_per_dim = √1,872,457 ≈ 1,368
- lat_chunk = min(1,368, 720) = 720
- lon_chunk = min(1,368, 1440) = 1,368

Result: time=7, lat=720, lon=1368 chunks

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

How it works:
- Calculates moderate time chunks (~5% of total time steps, capped at 50)
- Distributes remaining space across spatial dimensions with a minimum of 30 elements per dimension
- Provides a balanced approach for mixed access patterns

Detailed Calculation:
The balanced approach algorithm uses the following mathematical approach:

1. **Time Chunk Calculation**:
   ``time_chunk = min(50, max(10, time_dimension_size // 20))``

2. **Spatial Chunk Calculation**:
   ``target_elements = (target_chunk_size_mb * 1024²) / dtype_size_bytes``
   ``spatial_elements_per_dim = target_elements / time_chunk``
   ``spatial_chunk_per_dim = (spatial_elements_per_dim)^(1/num_spatial_dims)``
   ``spatial_chunk = max(30, min(spatial_chunk_per_dim, spatial_dimension_size))``

Example:
For a dataset with 1825 time steps, 360 lat points, 720 lon points:
- time_chunk = min(50, max(10, 1825//20)) = min(50, 91) = 50
- spatial_elements_per_dim = (50 * 1024² / 4) / 50 = 262,144
- spatial_chunk_per_dim = √262,144 ≈ 512
- lat_chunk = min(512, 360) = 360
- lon_chunk = min(512, 720) = 512

Result: time=50, lat=360, lon=512 chunks

Special Cases
~~~~~~~~~~~~~

No Time Dimension:
When no time dimension is detected, the system distributes chunks evenly across all dimensions:
``elements_per_dim = (target_elements)^(1/num_dimensions)``
``chunk_size = min(50, max(10, elements_per_dim))`` for balanced/normal access
``chunk_size = min(100, max(30, elements_per_dim))`` for spatial focus

Single Dimension:
For single-dimensional datasets, the system creates chunks of approximately the target size:
``chunk_size = min(target_elements, dimension_size)``

Validation Rules
~~~~~~~~~~~~~~~~

The system validates all chunking recommendations and flags issues:

1. **Small Chunk Warning**: Chunks < 1 MB may cause metadata overhead
2. **Large Chunk Warning**: Chunks > 100 MB may cause memory issues
3. **Dimension Mismatch**: Chunk sizes larger than dimensions are clipped
4. **Inefficient Chunking**: Very small chunks in large dimensions trigger recommendations

Configuration Options
~~~~~~~~~~~~~~~~~~~~~

Target Chunk Size:
You can configure the target chunk size in multiple ways:

1. **Function Parameter**:
   ``get_chunk_recommendation(..., target_chunk_size_mb=100)``

2. **Environment Variable**:
   ``ZARRIFY_TARGET_CHUNK_SIZE_MB=200``

3. **ZarrConverter Configuration**:
   .. code-block:: python
   
       config = ZarrConverterConfig(target_chunk_size_mb=100)

Environment-Specific Recommendations:
- **Local development**: 10-25 MB chunks
- **Production servers**: 50-100 MB chunks
- **Cloud environments**: 100-200 MB chunks

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

zarrio validates user-provided chunking and provides warnings for suboptimal configurations:

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
    zarrio convert input.nc output.zarr \
        --chunking "time:100,lat:50,lon:100"

    # Convert with configuration file
    zarrio convert input.nc output.zarr \
        --config config.yaml

    # Convert with automatic analysis
    zarrio convert input.nc output.zarr \
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

Practical Examples by Resolution
--------------------------------

The following examples show how the chunking calculations work with different data resolutions:

Low Resolution (1° or coarser)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

For a global daily dataset with 10 years of data at 1° resolution:
- Dimensions: time=3650, lat=180, lon=360
- Temporal focus: time=365, lat=180, lon=360
- Spatial focus: time=73, lat=180, lon=360
- Balanced: time=183, lat=180, lon=360

Medium Resolution (0.25° to 1°)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

For a regional daily dataset with 5 years of data at 0.25° resolution:
- Dimensions: time=1825, lat=720, lon=1440
- Temporal focus: time=182, lat=360, lon=720
- Spatial focus: time=37, lat=720, lon=1440
- Balanced: time=91, lat=540, lon=1080

High Resolution (0.1° or finer)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

For a local hourly dataset with 1 year of data at 0.1° resolution:
- Dimensions: time=8760, lat=1800, lon=3600
- Temporal focus: time=100, lat=300, lon=600
- Spatial focus: time=20, lat=900, lon=1800
- Balanced: time=50, lat=600, lon=1200
