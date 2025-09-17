Analysis Tool
=============

The `analyze` command in zarrify provides comprehensive analysis of NetCDF files to help users optimize their Zarr conversion process. It examines the dataset and provides recommendations for chunking, packing, and compression options.

Usage
-----

Basic analysis:

.. code-block:: bash

    zarrify analyze input.nc

Analysis with performance testing:

.. code-block:: bash

    zarrify analyze input.nc --test-performance

Analysis with custom target chunk size:

.. code-block:: bash

    zarrify analyze input.nc --target-chunk-size-mb 100

Interactive configuration setup:

.. code-block:: bash

    zarrify analyze input.nc --interactive

Features
--------

Dataset Information
~~~~~~~~~~~~~~~~~~~

The analysis tool provides detailed information about the dataset:

- Dimensions and their sizes
- Variables and their data types
- Coordinates
- Size estimates for each variable and the total dataset

Chunking Analysis
~~~~~~~~~~~~~~~~~

The tool analyzes the dataset and provides recommendations for three access patterns:

1. **Temporal Access Pattern**: Optimized for time series analysis
2. **Spatial Access Pattern**: Optimized for spatial analysis  
3. **Balanced Access Pattern**: Good for mixed workloads

For each pattern, it recommends:

- Chunk sizes for each dimension
- Estimated chunk size in MB
- Notes about the optimization strategy

Packing Analysis
~~~~~~~~~~~~~~~~

The analysis identifies:

- Variables that already have valid_min/valid_max attributes
- Variables that are missing valid range attributes
- Recommendations for adding attributes for optimal packing

Compression Analysis
~~~~~~~~~~~~~~~~~~~~

The tool lists common compression options with their characteristics:

- blosc:zstd:1 - Fast compression, good balance
- blosc:zstd:3 - Higher compression, slower
- blosc:lz4:1 - Very fast compression
- blosc:lz4:3 - Higher compression, slower

Performance Testing
~~~~~~~~~~~~~~~~~~~

When using the ``--test-performance`` flag, the tool analyzes the data characteristics and provides theoretical benefits of different compression and packing options:

- **Theoretical Benefits**: Calculates potential size reductions based on data types
- **Typical Compression Ratios**: Shows empirical compression ratios for common options
- **Performance Considerations**: Explains trade-offs between compression and performance
- **Recommendations**: Advises using ``--run-tests`` for real-world measurements

When using the ``--run-tests`` flag, the tool runs actual conversion tests on a subset of the data to measure real-world benefits:

- **No compression, no packing**: Baseline for comparison
- **Packing 16-bit**: 16-bit packing for floating-point data
- **Packing 8-bit**: 8-bit packing for maximum compression
- **Blosc Zstd Level 1**: Fast compression with good ratio
- **Blosc Zstd Level 3**: Higher compression, slower
- **Blosc LZ4 Level 1**: Very fast compression
- **Packing + Blosc Zstd**: Combined packing and compression

For each configuration, the tool measures:

- **Output size**: Actual disk space used
- **Processing time**: Time taken to perform the conversion
- **Compression ratio**: Size reduction compared to baseline
- **Performance impact**: Time increase compared to baseline

Example output (Theoretical):

.. code-block:: text

    Performance Analysis (Theoretical):
    -----------------------------------
    Analyzing compression and packing for variable: temperature
      Original data: 2.45 MB (float64)

    Theoretical Benefits:
    -------------------
      16-bit packing: 1.23 MB (2.0x smaller)
      8-bit packing: 0.62 MB (4.0x smaller)

    Typical Compression Ratios:
    -------------------------
      Blosc Zstd Level 1: 2-3x smaller (fast)
      Blosc Zstd Level 3: 3-5x smaller (slower)
      Blosc LZ4 Level 1: 2-2.5x smaller (very fast)
      Blosc LZ4 Level 3: 2.5-3.5x smaller (fast)
      Packing + Blosc Zstd: 5-10x smaller (combined benefits)

    Performance Considerations:
    -------------------------
      Packing adds CPU overhead during conversion
      Compression adds CPU overhead during read/write
      Higher compression levels = more CPU overhead
      Smaller chunks = more metadata overhead
      Larger chunks = more memory usage during processing

    Recommendation: Use --run-tests to measure real-world performance
    for your specific data and use case.

Example output (Actual):

.. code-block:: text

    Performance Testing (Actual):
    ----------------------------
    Testing compression and packing on variable: temperature
      No compression, no packing: 2.45 MB in 1.23s
      Packing 16-bit: 1.23 MB in 1.45s
      Packing 8-bit: 0.62 MB in 1.67s
      Blosc Zstd Level 1: 0.85 MB in 2.34s
      Blosc Zstd Level 3: 0.65 MB in 3.45s
      Blosc LZ4 Level 1: 0.92 MB in 1.78s
      Packing + Blosc Zstd: 0.45 MB in 2.67s

    Performance Comparison:
    ----------------------
      No compression, no packing:
        Size: 2.45 MB (1.0x smaller)
        Time: 1.23s (1.0x slower)
      Packing 16-bit:
        Size: 1.23 MB (2.0x smaller)
        Time: 1.45s (1.2x slower)
      Packing 8-bit:
        Size: 0.62 MB (4.0x smaller)
        Time: 1.67s (1.4x slower)
      Blosc Zstd Level 1:
        Size: 0.85 MB (2.9x smaller)
        Time: 2.34s (1.9x slower)
      Blosc Zstd Level 3:
        Size: 0.65 MB (3.8x smaller)
        Time: 3.45s (2.8x slower)
      Blosc LZ4 Level 1:
        Size: 0.92 MB (2.7x smaller)
        Time: 1.78s (1.4x slower)
      Packing + Blosc Zstd:
        Size: 0.45 MB (5.4x smaller)
        Time: 2.67s (2.2x slower)

Interactive Mode
----------------

When using the `--interactive` flag, the tool guides users through setting up a configuration:

1. **Chunking Configuration**: Select from recommended access patterns or specify custom chunks
2. **Packing Configuration**: Enable packing, choose bit width, and handle variables without valid ranges
3. **Compression Configuration**: Select from common compression options
4. **Configuration Export**: Save the configuration to a YAML file

Example Output
--------------

.. code-block:: text

    zarrify Analysis Tool
    ==================================================
    Analyzing file: sample.nc

    Loading dataset...
    Dataset loaded successfully!

    Dataset Information:
    --------------------
    Dimensions: {'time': 100, 'lat': 180, 'lon': 360}
    Variables: ['temperature', 'pressure']
    Coordinates: ['time', 'lat', 'lon']

    Data Type Information:
    --------------------
    temperature: float64 (8 bytes/element)
      Shape: (100, 180, 360)
      Size estimate: 49.44 MB
    pressure: float64 (8 bytes/element)
      Shape: (100, 180, 360)
      Size estimate: 49.44 MB
    Total dataset size estimate: 98.88 MB

    Chunking Analysis:
    ----------------
    Temporal Access Pattern:
      Recommended chunks: {'time': 10, 'lat': 180, 'lon': 360}
      Estimated chunk size: 2.47 MB
      Notes: Optimized for time series analysis

    Spatial Access Pattern:
      Recommended chunks: {'time': 5, 'lat': 180, 'lon': 360}
      Estimated chunk size: 1.24 MB
      Notes: Optimized for spatial analysis

    Balanced Access Pattern:
      Recommended chunks: {'time': 10, 'lat': 180, 'lon': 360}
      Estimated chunk size: 2.47 MB
      Notes: Balanced for mixed access patterns

    Packing Analysis:
    ----------------
    Variables with valid range attributes: ['temperature']
    Variables without valid range attributes: ['pressure']

    Recommendation: Consider adding valid_min/valid_max attributes to variables
    for optimal packing.

    Compression Analysis:
    --------------------
    Common compression options:
    1. blosc:zstd:1 - Fast compression, good balance
    2. blosc:zstd:3 - Higher compression, slower
    3. blosc:lz4:1 - Very fast compression
    4. blosc:lz4:3 - Higher compression, slower

    Performance Testing:
    ------------------
    Testing compression and packing on variable: temperature
      No compression, no packing: 2.45 MB in 1.23s
      Packing 16-bit: 1.23 MB in 1.45s
      Packing 8-bit: 0.62 MB in 1.67s
      Blosc Zstd Level 1: 0.85 MB in 2.34s
      Blosc Zstd Level 3: 0.65 MB in 3.45s
      Blosc LZ4 Level 1: 0.92 MB in 1.78s
      Packing + Blosc Zstd: 0.45 MB in 2.67s

    Performance Comparison:
    ----------------------
      No compression, no packing:
        Size: 2.45 MB (1.0x smaller)
        Time: 1.23s (1.0x slower)
      Packing 16-bit:
        Size: 1.23 MB (2.0x smaller)
        Time: 1.45s (1.2x slower)
      Packing 8-bit:
        Size: 0.62 MB (4.0x smaller)
        Time: 1.67s (1.4x slower)
      Blosc Zstd Level 1:
        Size: 0.85 MB (2.9x smaller)
        Time: 2.34s (1.9x slower)
      Blosc Zstd Level 3:
        Size: 0.65 MB (3.8x smaller)
        Time: 3.45s (2.8x slower)
      Blosc LZ4 Level 1:
        Size: 0.92 MB (2.7x smaller)
        Time: 1.78s (1.4x slower)
      Packing + Blosc Zstd:
        Size: 0.45 MB (5.4x smaller)
        Time: 2.67s (2.2x slower)

Best Practices
--------------

1. **Use Interactive Mode**: For new users, the interactive mode provides guided setup
2. **Consider Access Patterns**: Choose the access pattern that matches your primary use case
3. **Add Valid Ranges**: Add valid_min/valid_max attributes to variables for optimal packing
4. **Test Compression**: Experiment with different compression options for your data
5. **Use Performance Testing**: Run performance tests to see real-world benefits
6. **Save Configurations**: Save recommended configurations for reuse

Environment-Specific Recommendations
------------------------------------

Target chunk sizes can be optimized for different environments:

- **Local Development**: 10-25 MB chunks
- **Production Servers**: 50-100 MB chunks  
- **Cloud Environments**: 100-200 MB chunks

Use the `--target-chunk-size-mb` option to customize recommendations for your environment.