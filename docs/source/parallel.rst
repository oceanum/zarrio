Parallel Processing
==================

One of the key features of zarrify is its support for parallel processing of large datasets through template creation and region writing.

Overview
--------

When dealing with thousands of NetCDF files, it's often more efficient to process them in parallel rather than sequentially. zarrify provides a robust framework for this:

1. **Template Creation**: Create a Zarr archive with full time range but no data (`compute=False`)
2. **Region Writing**: Write data from individual NetCDF files to specific regions
3. **Parallel Execution**: Multiple processes can write to different regions simultaneously

This approach enables processing thousands of NetCDF files in parallel, which is essential for large-scale data conversion workflows.

Template Creation
-----------------

The first step is to create a template Zarr archive that covers the full time range but contains no data:

.. code-block:: python

    from zarrify import ZarrConverter

    # Create converter
    converter = ZarrConverter(
        chunking={"time": 100, "lat": 50, "lon": 100},
        compression="blosc:zstd:3",
        packing=True
    )

    # Create template covering full time range
    converter.create_template(
        template_dataset=template_ds,
        output_path="archive.zarr",
        global_start="2020-01-01",
        global_end="2023-12-31",
        compute=False  # Metadata only, no data computation
    )

The `compute=False` parameter is crucial - it creates the metadata and structure of the Zarr archive without computing or storing any actual data.

Parameters:
~~~~~~~~~~~

- **template_dataset**: Dataset to use as template for structure and metadata
- **output_path**: Path to output Zarr store
- **global_start**: Start time for the full archive
- **global_end**: End time for the full archive
- **freq**: Frequency for time coordinate (inferred from template if not provided)
- **compute**: Whether to compute immediately (False for template only)
- **intelligent_chunking**: Whether to perform intelligent chunking based on full archive dimensions
- **access_pattern**: Access pattern for chunking optimization ("temporal", "spatial", "balanced")

Region Writing
-------------

After creating the template, you can write data from individual NetCDF files to specific regions:

.. code-block:: python

    # Write regions in parallel processes
    converter.write_region("data_2020.nc", "archive.zarr")  # Process 1
    converter.write_region("data_2021.nc", "archive.zarr")  # Process 2
    converter.write_region("data_2022.nc", "archive.zarr")  # Process 3
    converter.write_region("data_2023.nc", "archive.zarr")  # Process 4

Each process works independently on different files, writing to different regions of the same Zarr archive.

Automatic Region Determination
------------------------------

zarrify can automatically determine the region for writing based on the time coordinates in the source file:

.. code-block:: python

    # No region specified - automatically determined
    converter.write_region("data_2020.nc", "archive.zarr")

The system compares the time range in the source file with the time coordinates in the existing Zarr archive and determines where the data should be written.

Manual Region Specification
--------------------------

You can also specify regions manually:

.. code-block:: python

    # Manual region specification
    region = {"time": slice(0, 100), "lat": slice(None), "lon": slice(None)}
    converter.write_region("data.nc", "archive.zarr", region=region)

CLI Support
----------

The parallel processing workflow is also supported through the CLI:

Template Creation:
~~~~~~~~~~~~~~~~~~

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

Region Writing:
~~~~~~~~~~~~~~~

.. code-block:: bash

    # Write regions in parallel processes
    zarrify write-region data_2020.nc archive.zarr  # Process 1
    zarrify write-region data_2021.nc archive.zarr  # Process 2
    zarrify write-region data_2022.nc archive.zarr  # Process 3
    zarrify write-region data_2023.nc archive.zarr  # Process 4

Complete Parallel Workflow
------------------------

Here's a complete example of processing thousands of NetCDF files in parallel:

.. code-block:: python

    import multiprocessing
    import os
    from zarrify import ZarrConverter

    def process_file(args):
        """Process a single NetCDF file."""
        input_file, zarr_path = args
        
        # Create converter for this process
        converter = ZarrConverter(
            chunking={"time": 100, "lat": 50, "lon": 100},
            compression="blosc:zstd:3",
            packing=True
        )
        
        # Write region
        converter.write_region(input_file, zarr_path)
        
        return f"Processed {input_file}"

    def parallel_processing_example():
        """Example of parallel processing workflow."""
        # 1. Create template
        converter = ZarrConverter()
        converter.create_template(
            template_dataset=template_ds,
            output_path="large_archive.zarr",
            global_start="2020-01-01",
            global_end="2023-12-31",
            compute=False
        )
        
        # 2. Prepare file list
        netcdf_files = [
            "data_2020_001.nc", "data_2020_002.nc", ..., "data_2023_365.nc"
        ]
        zarr_path = "large_archive.zarr"
        
        # 3. Process files in parallel
        with multiprocessing.Pool(processes=4) as pool:
            args_list = [(nc_file, zarr_path) for nc_file in netcdf_files]
            results = pool.map(process_file, args_list)
        
        print("All files processed successfully!")

Containerized Parallel Processing
--------------------------------

For even easier deployment and management, you can also run parallel processing workflows using Docker containers:

.. code-block:: bash

    # Create template using Docker
    docker run --rm -v $(pwd):/data zarrify:latest create-template /data/template.nc /data/archive.zarr \\
        --global-start 2020-01-01 \\
        --global-end 2023-12-31

    # Process multiple files in parallel containers
    docker run --rm -v $(pwd):/data zarrify:latest write-region /data/data_2020.nc /data/archive.zarr &
    docker run --rm -v $(pwd):/data zarrify:latest write-region /data/data_2021.nc /data/archive.zarr &
    docker run --rm -v $(pwd):/data zarrify:latest write-region /data/data_2022.nc /data/archive.zarr &
    docker run --rm -v $(pwd):/data zarrify:latest write-region /data/data_2023.nc /data/archive.zarr &

    # Wait for all processes to complete
    wait

This approach provides additional benefits such as:

1. **Environment Consistency**: Ensures identical environments across all processes
2. **Dependency Isolation**: No conflicts with host system dependencies
3. **Easy Deployment**: Simplifies deployment to cloud or cluster environments
4. **Resource Management**: Better control over resource allocation
5. **Security**: Process isolation for added security

See :doc:`docker` for more details on Docker usage.

Benefits
--------

1. **Scalability**: Process thousands of NetCDF files in parallel
2. **Efficiency**: No need to load all data into memory at once
3. **Reliability**: Independent processes reduce risk of total failure
4. **Flexibility**: Each process can work on different files simultaneously
5. **Performance**: Dramatic speedup for large datasets

Error Handling
--------------

zarrify includes robust error handling for parallel processing:

.. code-block:: python

    from zarrify import ZarrConverter
    from zarrify.models import ZarrConverterConfig, ChunkingConfig, CompressionConfig, PackingConfig

    def robust_process_file(args):
        """Process a single NetCDF file with error handling."""
        input_file, zarr_path = args
        
        try:
            # Create converter with retry logic
            converter = ZarrConverter(
                config=ZarrConverterConfig(
                    chunking=ChunkingConfig(time=100, lat=50, lon=100),
                    compression=CompressionConfig(method="blosc:zstd:3"),
                    packing=PackingConfig(enabled=True, bits=16),
                    retries_on_missing=3,  # Retry up to 3 times
                    missing_check_vars="all"
                )
            )
            
            # Write region with retry logic
            converter.write_region(input_file, zarr_path)
            
            return f"Successfully processed {input_file}"
            
        except RetryLimitExceededError as e:
            return f"Failed after retries: {input_file} - {e}"
        except ConversionError as e:
            return f"Conversion failed: {input_file} - {e}"
        except Exception as e:
            return f"Unexpected error: {input_file} - {e}"

Monitoring and Logging
--------------------

zarrify provides comprehensive logging for parallel processing:

.. code-block:: python

    import logging

    # Setup logging
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )

    # Each process will log its activities
    converter = ZarrConverter()
    converter.write_region("data.nc", "archive.zarr")

The logs will show:
- Process startup and completion
- Region determination
- Data writing progress
- Error conditions and retries
- Performance metrics

Best Practices
-------------

1. **Template First**: Always create the template before starting parallel processes
2. **Consistent Chunking**: Use the same chunking strategy across all processes
3. **Error Handling**: Implement robust error handling with retries
4. **Logging**: Enable logging to monitor progress and debug issues
5. **Resource Management**: Monitor memory and CPU usage in parallel processes
6. **Validation**: Verify the final archive after all processes complete

Example with Validation:
~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

    def validate_final_archive(zarr_path):
        """Validate the final archive after parallel processing."""
        import xarray as xr
        
        try:
            # Open final archive
            ds = xr.open_zarr(zarr_path)
            
            # Check time range
            start_time = ds.time.to_index()[0]
            end_time = ds.time.to_index()[-1]
            print(f"Archive time range: {start_time} to {end_time}")
            
            # Check data completeness
            total_time_steps = len(ds.time)
            print(f"Total time steps: {total_time_steps}")
            
            # Check variables
            variables = list(ds.data_vars.keys())
            print(f"Variables: {variables}")
            
            # Check data integrity (basic checks)
            for var in variables:
                data_min = float(ds[var].min().values)
                data_max = float(ds[var].max().values)
                print(f"{var}: min={data_min:.3f}, max={data_max:.3f}")
            
            print("Archive validation completed successfully!")
            return True
            
        except Exception as e:
            print(f"Archive validation failed: {e}")
            return False

    # After parallel processing
    if validate_final_archive("large_archive.zarr"):
        print("Parallel processing completed successfully!")
    else:
        print("Parallel processing completed with issues!")

Performance Considerations
------------------------

1. **Chunking Strategy**: Choose chunking that aligns with your access patterns
2. **Compression**: Use appropriate compression for your data characteristics
3. **Memory Usage**: Monitor memory usage in parallel processes
4. **I/O Patterns**: Consider storage system characteristics (local vs cloud)
5. **Process Count**: Optimize the number of parallel processes for your system

Example Performance Tuning:
~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

    # Optimize for your system
    converter = ZarrConverter(
        config=ZarrConverterConfig(
            chunking=ChunkingConfig(
                time=100,    # Balance between I/O and memory
                lat=50,      # Consider spatial access patterns
                lon=100      # Consider spatial access patterns
            ),
            compression=CompressionConfig(
                method="blosc:zstd:2",  # Balance between speed and compression
                clevel=2,
                shuffle="shuffle"
            ),
            packing=PackingConfig(
                enabled=True,
                bits=16      # 16-bit packing for good compression
            )
        )
    )

Cloud Storage Considerations
--------------------------

When using cloud storage (S3, GCS, etc.):

1. **Network I/O**: Minimize network round trips
2. **Chunk Size**: Larger chunks for cloud storage to reduce request overhead
3. **Concurrency**: Limit concurrent processes to avoid overwhelming the service
4. **Error Handling**: Implement robust retry logic for network failures

.. code-block:: python

    # Cloud-optimized configuration
    converter = ZarrConverter(
        config=ZarrConverterConfig(
            chunking=ChunkingConfig(
                time=200,    # Larger chunks for cloud storage
                lat=100,     # Larger spatial chunks
                lon=200      # Larger spatial chunks
            ),
            compression=CompressionConfig(
                method="blosc:zstd:3",  # Higher compression for network transfer
                clevel=3,
                shuffle="shuffle"
            ),
            packing=PackingConfig(
                enabled=True,
                bits=16
            )
        )
    )

Troubleshooting
-------------

Common issues and solutions:

1. **Region Conflicts**: Ensure processes write to different regions
2. **Memory Issues**: Monitor memory usage and adjust chunking
3. **Network Errors**: Implement retry logic for cloud storage
4. **Data Integrity**: Use validation to check final archive
5. **Performance**: Profile and optimize chunking strategy

Example Troubleshooting:
~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

    # Add detailed logging for troubleshooting
    import logging
    
    # Enable debug logging
    logging.basicConfig(
        level=logging.DEBUG,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
    
    # Create converter with detailed logging
    converter = ZarrConverter(
        config=ZarrConverterConfig(
            chunking=ChunkingConfig(time=100, lat=50, lon=100),
            compression=CompressionConfig(method="blosc:zstd:3"),
            packing=PackingConfig(enabled=True, bits=16),
            retries_on_missing=3,
            missing_check_vars="all"
        )
    )
    
    # Write region with verbose logging
    converter.write_region("data.nc", "archive.zarr")

This will provide detailed information about:
- Region determination
- Data processing steps
- Compression and packing
- Retry attempts
- Error conditions