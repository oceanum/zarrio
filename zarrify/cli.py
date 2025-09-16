"""
Command-line interface for zarrify with Pydantic configuration support.
"""

import argparse
import json
import logging
import sys
from pathlib import Path
from typing import Any, Dict

import xarray as xr
import yaml

from .__init__ import __version__
from .core import ZarrConverter
from .models import ZarrConverterConfig, load_config_from_file
from .packing import Packer

logger = logging.getLogger(__name__)


def setup_logging(verbosity: int = 0) -> None:
    """Setup logging based on verbosity level."""
    levels = [logging.WARNING, logging.INFO, logging.DEBUG]
    level = levels[min(verbosity, len(levels) - 1)]
    logging.basicConfig(
        level=level, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )


def parse_chunking(chunking_str: str) -> Dict[str, int]:
    """Parse chunking string to dictionary."""
    if not chunking_str:
        return {}

    chunking = {}
    for part in chunking_str.split(","):
        if ":" in part:
            dim, size = part.split(":")
            chunking[dim.strip()] = int(size.strip())
    return chunking


def convert_command(args: argparse.Namespace) -> None:
    """Handle convert command."""
    # Parse chunking
    chunking = parse_chunking(args.chunking)

    # Load config if provided
    config = None
    if args.config:
        config = load_config_from_file(args.config)

    # Override config with command line arguments
    config_dict = {}
    if config:
        config_dict = config.model_dump()

    if chunking:
        config_dict.setdefault("chunking", {}).update(chunking)
    if args.compression:
        config_dict.setdefault("compression", {})["method"] = args.compression
    if args.packing:
        config_dict.setdefault("packing", {})["enabled"] = True
    if args.packing_bits:
        config_dict.setdefault("packing", {})["bits"] = args.packing_bits
    if args.packing_manual_ranges:
        import json

        config_dict.setdefault("packing", {})["manual_ranges"] = json.loads(
            args.packing_manual_ranges
        )
    if args.packing_auto_buffer_factor:
        config_dict.setdefault("packing", {})[
            "auto_buffer_factor"
        ] = args.packing_auto_buffer_factor
    if not args.packing_check_range_exceeded:
        config_dict.setdefault("packing", {})["check_range_exceeded"] = False
    if args.packing_range_exceeded_action:
        config_dict.setdefault("packing", {})[
            "range_exceeded_action"
        ] = args.packing_range_exceeded_action
    if args.time_dim:
        config_dict.setdefault("time", {})["dim"] = args.time_dim
    if args.target_chunk_size_mb:
        config_dict["target_chunk_size_mb"] = args.target_chunk_size_mb
    if args.attrs:
        config_dict["attrs"] = json.loads(args.attrs)

    # Add datamesh config if provided
    if args.datamesh_datasource:
        config_dict.setdefault("datamesh", {})
        config_dict["datamesh"]["datasource"] = json.loads(args.datamesh_datasource)
        if args.datamesh_token:
            config_dict["datamesh"]["token"] = args.datamesh_token
        if args.datamesh_service:
            config_dict["datamesh"]["service"] = args.datamesh_service

    # Create converter with config
    converter_config = ZarrConverterConfig(**config_dict)
    converter = ZarrConverter(config=converter_config)

    # Parse variables
    variables = args.variables.split(",") if args.variables else None
    drop_variables = args.drop_variables.split(",") if args.drop_variables else None

    # Perform conversion
    converter.convert(
        input_path=args.input,
        output_path=args.output,
        variables=variables,
        drop_variables=drop_variables,
    )

    logger.info("Conversion completed successfully")


def append_command(args: argparse.Namespace) -> None:
    """Handle append command."""
    # Parse chunking
    chunking = parse_chunking(args.chunking)

    # Load config if provided
    config = None
    if args.config:
        config = load_config_from_file(args.config)

    # Override config with command line arguments
    config_dict = {}
    if config:
        config_dict = config.model_dump()

    if chunking:
        config_dict.setdefault("chunking", {}).update(chunking)
    if args.append_dim:
        config_dict.setdefault("time", {})["append_dim"] = args.append_dim
    if args.time_dim:
        config_dict.setdefault("time", {})["dim"] = args.time_dim
    if args.target_chunk_size_mb:
        config_dict["target_chunk_size_mb"] = args.target_chunk_size_mb

    # Add datamesh config if provided
    if args.datamesh_datasource:
        config_dict.setdefault("datamesh", {})
        config_dict["datamesh"]["datasource"] = json.loads(args.datamesh_datasource)
        if args.datamesh_token:
            config_dict["datamesh"]["token"] = args.datamesh_token
        if args.datamesh_service:
            config_dict["datamesh"]["service"] = args.datamesh_service

    # Create converter with config
    converter_config = ZarrConverterConfig(**config_dict)
    converter = ZarrConverter(config=converter_config)

    # Parse variables
    variables = args.variables.split(",") if args.variables else None
    drop_variables = args.drop_variables.split(",") if args.drop_variables else None

    # Perform append
    converter.append(
        input_path=args.input,
        zarr_path=args.zarr,
        variables=variables,
        drop_variables=drop_variables,
    )

    logger.info("Append completed successfully")


def create_template_command(args: argparse.Namespace) -> None:
    """Handle create-template command."""
    # Parse chunking
    chunking = parse_chunking(args.chunking)

    # Load config if provided
    config = None
    if args.config:
        config = load_config_from_file(args.config)

    # Override config with command line arguments
    config_dict = {}
    if config:
        config_dict = config.model_dump()

    if chunking:
        config_dict.setdefault("chunking", {}).update(chunking)
    if args.compression:
        config_dict.setdefault("compression", {})["method"] = args.compression
    if args.packing:
        config_dict.setdefault("packing", {})["enabled"] = True
    if args.packing_bits:
        config_dict.setdefault("packing", {})["bits"] = args.packing_bits
    if args.packing_manual_ranges:
        import json

        config_dict.setdefault("packing", {})["manual_ranges"] = json.loads(
            args.packing_manual_ranges
        )
    if args.packing_auto_buffer_factor:
        config_dict.setdefault("packing", {})[
            "auto_buffer_factor"
        ] = args.packing_auto_buffer_factor
    if not args.packing_check_range_exceeded:
        config_dict.setdefault("packing", {})["check_range_exceeded"] = False
    if args.packing_range_exceeded_action:
        config_dict.setdefault("packing", {})[
            "range_exceeded_action"
        ] = args.packing_range_exceeded_action
    if args.time_dim:
        config_dict.setdefault("time", {})["dim"] = args.time_dim
    if args.target_chunk_size_mb:
        config_dict["target_chunk_size_mb"] = args.target_chunk_size_mb

    # Add datamesh config if provided
    if args.datamesh_datasource:
        config_dict.setdefault("datamesh", {})
        config_dict["datamesh"]["datasource"] = json.loads(args.datamesh_datasource)
        if args.datamesh_token:
            config_dict["datamesh"]["token"] = args.datamesh_token
        if args.datamesh_service:
            config_dict["datamesh"]["service"] = args.datamesh_service

    # Create converter with config
    converter_config = ZarrConverterConfig(**config_dict)
    converter = ZarrConverter(config=converter_config)

    # Open template dataset
    template_ds = xr.open_dataset(args.template)

    # Create template
    converter.create_template(
        template_dataset=template_ds,
        output_path=args.output,
        global_start=args.global_start,
        global_end=args.global_end,
        freq=args.freq,
        compute=not args.metadata_only,
    )

    logger.info("Template creation completed successfully")


def append_command(args: argparse.Namespace) -> None:
    """Handle append command."""
    # Parse chunking
    chunking = parse_chunking(args.chunking)

    # Load config if provided
    config = None
    if args.config:
        config = load_config_from_file(args.config)

    # Override config with command line arguments
    config_dict = {}
    if config:
        config_dict = config.model_dump()

    if chunking:
        config_dict.setdefault("chunking", {}).update(chunking)
    if args.append_dim:
        config_dict.setdefault("time", {})["append_dim"] = args.append_dim
    if args.time_dim:
        config_dict.setdefault("time", {})["dim"] = args.time_dim
    if args.target_chunk_size_mb:
        config_dict["target_chunk_size_mb"] = args.target_chunk_size_mb

    # Add datamesh config if provided
    if args.datamesh_datasource:
        config_dict.setdefault("datamesh", {})
        config_dict["datamesh"]["datasource"] = json.loads(args.datamesh_datasource)
        if args.datamesh_token:
            config_dict["datamesh"]["token"] = args.datamesh_token
        if args.datamesh_service:
            config_dict["datamesh"]["service"] = args.datamesh_service

    # Create converter with config
    converter_config = ZarrConverterConfig(**config_dict)
    converter = ZarrConverter(config=converter_config)

    # Parse variables
    variables = args.variables.split(",") if args.variables else None
    drop_variables = args.drop_variables.split(",") if args.drop_variables else None

    # Perform append
    converter.append(
        input_path=args.input,
        zarr_path=args.zarr,
        variables=variables,
        drop_variables=drop_variables,
    )

    logger.info("Append completed successfully")


def create_template_command(args: argparse.Namespace) -> None:
    """Handle create-template command."""
    # Parse chunking
    chunking = parse_chunking(args.chunking)

    # Load config if provided
    config = None
    if args.config:
        config = load_config_from_file(args.config)

    # Override config with command line arguments
    config_dict = {}
    if config:
        config_dict = config.model_dump()

    if chunking:
        config_dict.setdefault("chunking", {}).update(chunking)
    if args.compression:
        config_dict.setdefault("compression", {})["method"] = args.compression
    if args.packing:
        config_dict.setdefault("packing", {})["enabled"] = True
    if args.packing_bits:
        config_dict.setdefault("packing", {})["bits"] = args.packing_bits
    if args.packing_manual_ranges:
        import json

        config_dict.setdefault("packing", {})["manual_ranges"] = json.loads(
            args.packing_manual_ranges
        )
    if args.packing_auto_buffer_factor:
        config_dict.setdefault("packing", {})[
            "auto_buffer_factor"
        ] = args.packing_auto_buffer_factor
    if not args.packing_check_range_exceeded:
        config_dict.setdefault("packing", {})["check_range_exceeded"] = False
    if args.packing_range_exceeded_action:
        config_dict.setdefault("packing", {})[
            "range_exceeded_action"
        ] = args.packing_range_exceeded_action
    if args.time_dim:
        config_dict.setdefault("time", {})["dim"] = args.time_dim
    if args.target_chunk_size_mb:
        config_dict["target_chunk_size_mb"] = args.target_chunk_size_mb

    # Add datamesh config if provided
    if args.datamesh_datasource:
        config_dict.setdefault("datamesh", {})
        config_dict["datamesh"]["datasource"] = json.loads(args.datamesh_datasource)
        if args.datamesh_token:
            config_dict["datamesh"]["token"] = args.datamesh_token
        if args.datamesh_service:
            config_dict["datamesh"]["service"] = args.datamesh_service

    # Create converter with config
    converter_config = ZarrConverterConfig(**config_dict)
    converter = ZarrConverter(config=converter_config)

    # Open template dataset
    template_ds = xr.open_dataset(args.template)

    # Create template
    converter.create_template(
        template_dataset=template_ds,
        output_path=args.output,
        global_start=args.global_start,
        global_end=args.global_end,
        freq=args.freq,
        compute=not args.metadata_only,
    )

    logger.info("Template creation completed successfully")


def write_region_command(args: argparse.Namespace) -> None:
    """Handle write-region command."""
    # Parse chunking
    chunking = parse_chunking(args.chunking)

    # Load config if provided
    config = None
    if args.config:
        config = load_config_from_file(args.config)

    # Override config with command line arguments
    config_dict = {}
    if config:
        config_dict = config.model_dump()

    if chunking:
        config_dict.setdefault("chunking", {}).update(chunking)
    if args.time_dim:
        config_dict.setdefault("time", {})["dim"] = args.time_dim
    if args.target_chunk_size_mb:
        config_dict["target_chunk_size_mb"] = args.target_chunk_size_mb

    # Add datamesh config if provided
    if args.datamesh_datasource:
        config_dict.setdefault("datamesh", {})
        config_dict["datamesh"]["datasource"] = json.loads(args.datamesh_datasource)
        if args.datamesh_token:
            config_dict["datamesh"]["token"] = args.datamesh_token
        if args.datamesh_service:
            config_dict["datamesh"]["service"] = args.datamesh_service

    # Create converter with config
    converter_config = ZarrConverterConfig(**config_dict)
    converter = ZarrConverter(config=converter_config)

    # Parse region if provided
    region = None
    if args.region:
        region = {}
        for part in args.region.split(","):
            dim, slice_str = part.split("=")
            start, end = slice_str.split(":")
            region[dim.strip()] = slice(int(start), int(end))

    # Parse variables
    variables = args.variables.split(",") if args.variables else None
    drop_variables = args.drop_variables.split(",") if args.drop_variables else None

    # Write region
    converter.write_region(
        input_path=args.input,
        zarr_path=args.zarr,
        region=region,
        variables=variables,
        drop_variables=drop_variables,
    )

    logger.info("Region writing completed successfully")


def analyze_command(args: argparse.Namespace) -> None:
    """Handle analyze command."""
    try:
        # Import required modules
        import os
        import tempfile
        import time

        import numpy as np
        import xarray as xr

        from .chunking import get_chunk_recommendation
        from .core import ZarrConverter
        from .models import (CompressionConfig, PackingConfig,
                             ZarrConverterConfig)
        from .packing import Packer

        print("zarrify Analysis Tool")
        print("=" * 50)
        print(f"Analyzing file: {args.input}")
        print()

        # Open dataset
        print("Loading dataset...")
        ds = xr.open_dataset(args.input)
        print("Dataset loaded successfully!")
        print()

        # Display basic dataset information
        print("Dataset Information:")
        print("-" * 20)
        print(f"Dimensions: {dict(ds.sizes)}")
        print(f"Variables: {list(ds.data_vars.keys())}")
        print(f"Coordinates: {list(ds.coords.keys())}")
        print()

        # Get data type information
        dtype_info = {}
        total_size_estimate = 0
        for var_name, var in ds.data_vars.items():
            dtype_size = var.dtype.itemsize
            dtype_info[var_name] = {
                "dtype": var.dtype,
                "dtype_size": dtype_size,
                "shape": var.shape,
                "size_elements": np.prod(var.shape),
            }
            total_size_estimate += dtype_size * np.prod(var.shape)

        print("Data Type Information:")
        print("-" * 20)
        for var_name, info in dtype_info.items():
            print(f"{var_name}: {info['dtype']} ({info['dtype_size']} bytes/element)")
            print(f"  Shape: {info['shape']}")
            size_mb = info["size_elements"] * info["dtype_size"] / (1024**2)
            print(f"  Size estimate: {size_mb:.2f} MB")
        print(f"Total dataset size estimate: {total_size_estimate / (1024**2):.2f} MB")
        print()

        # Analyze chunking options
        print("Chunking Analysis:")
        print("-" * 16)

        # Get dimensions for chunking analysis
        dimensions = dict(ds.sizes)

        # Test different access patterns
        access_patterns = ["temporal", "spatial", "balanced"]
        chunk_recommendations = {}

        for pattern in access_patterns:
            try:
                recommendation = get_chunk_recommendation(
                    dimensions=dimensions,
                    dtype_size_bytes=4,  # Assume float32 for analysis
                    access_pattern=pattern,
                    target_chunk_size_mb=args.target_chunk_size_mb or 50,
                )
                chunk_recommendations[pattern] = recommendation
                print(f"{pattern.capitalize()} Access Pattern:")
                print(f"  Recommended chunks: {recommendation.chunks}")
                print(
                    f"  Estimated chunk size: {recommendation.estimated_chunk_size_mb:.2f} MB"
                )
                if recommendation.warnings:
                    print(f"  Warnings: {', '.join(recommendation.warnings)}")
                if recommendation.notes:
                    print(f"  Notes: {', '.join(recommendation.notes)}")
                print()
            except Exception as e:
                print(f"Error analyzing {pattern} pattern: {e}")
                print()

        # Analyze packing options
        print("Packing Analysis:")
        print("-" * 16)

        # Check for existing valid_min/valid_max attributes
        variables_with_ranges = []
        variables_without_ranges = []

        for var_name in ds.data_vars.keys():
            var = ds[var_name]
            has_min = "valid_min" in var.attrs
            has_max = "valid_max" in var.attrs
            if has_min and has_max:
                variables_with_ranges.append(var_name)
            else:
                variables_without_ranges.append(var_name)

        print(f"Variables with valid range attributes: {variables_with_ranges}")
        print(f"Variables without valid range attributes: {variables_without_ranges}")
        print()

        if variables_without_ranges:
            print(
                "Recommendation: Consider adding valid_min/valid_max attributes to variables"
            )
            print("for optimal packing. You can either:")
            print("1. Use automatic range calculation (with buffer)")
            print("2. Specify manual ranges based on your domain knowledge")
            print()

        # Analyze compression options
        print("Compression Analysis:")
        print("-" * 20)
        print("Common compression options:")
        print("1. blosc:zstd:1 - Fast compression, good balance")
        print("2. blosc:zstd:3 - Higher compression, slower")
        print("3. blosc:lz4:1 - Very fast compression")
        print("4. blosc:lz4:3 - Higher compression, slower")
        print()

        # Theoretical performance testing if requested
        if args.test_performance:
            print("Performance Analysis (Theoretical):")
            print("-" * 37)

            # Select a representative variable for testing
            test_var = list(ds.data_vars.keys())[0] if ds.data_vars.keys() else None
            if test_var:
                print(f"Analyzing compression and packing for variable: {test_var}")

                # Get variable data characteristics
                var_data = ds[test_var]
                original_dtype = var_data.dtype
                original_size = var_data.nbytes / (1024**2)  # Size in MB

                print(f"  Original data: {original_size:.2f} MB ({original_dtype})")

                # Theoretical packing benefits
                print("\nTheoretical Benefits:")
                print("-" * 19)

                # 16-bit packing
                if original_dtype == np.float64:
                    packed_16_size = original_size * 0.5  # 50% reduction
                    print(f"  16-bit packing: {packed_16_size:.2f} MB (2.0x smaller)")
                elif original_dtype == np.float32:
                    packed_16_size = original_size  # Same size
                    print(f"  16-bit packing: {packed_16_size:.2f} MB (1.0x smaller)")

                # 8-bit packing
                if original_dtype in [np.float64, np.float32]:
                    packed_8_size = original_size * 0.25  # 75% reduction
                    print(f"  8-bit packing: {packed_8_size:.2f} MB (4.0x smaller)")

                # Typical compression ratios (based on empirical data)
                print("\nTypical Compression Ratios:")
                print("-" * 26)
                print("  Blosc Zstd Level 1: 2-3x smaller (fast)")
                print("  Blosc Zstd Level 3: 3-5x smaller (slower)")
                print("  Blosc LZ4 Level 1: 2-2.5x smaller (very fast)")
                print("  Blosc LZ4 Level 3: 2.5-3.5x smaller (fast)")
                print("  Packing + Blosc Zstd: 5-10x smaller (combined benefits)")

                # Performance considerations
                print("\nPerformance Considerations:")
                print("-" * 27)
                print("  Packing adds CPU overhead during conversion")
                print("  Compression adds CPU overhead during read/write")
                print("  Higher compression levels = more CPU overhead")
                print("  Smaller chunks = more metadata overhead")
                print("  Larger chunks = more memory usage during processing")

                print(
                    "\nRecommendation: Use --run-tests to measure real-world performance"
                )
                print("for your specific data and use case.")
                print()
            else:
                print("No variables found for analysis.")
                print()

        # Actual performance testing if requested
        if args.run_tests:
            print("Performance Testing (Actual):")
            print("-" * 26)

            # Select a representative variable for testing
            test_var = list(ds.data_vars.keys())[0] if ds.data_vars.keys() else None
            if test_var:
                print(f"Testing compression and packing on variable: {test_var}")

                # Create a small subset for testing to save time
                test_ds = ds.copy()
                if "time" in test_ds.sizes and test_ds.sizes["time"] > 10:
                    test_ds = test_ds.isel(time=slice(0, 10))

                # Test configurations
                test_configs = [
                    ("No compression, no packing", {}, {}),
                    ("Packing 16-bit", {}, {"enabled": True, "bits": 16}),
                    ("Packing 8-bit", {}, {"enabled": True, "bits": 8}),
                    ("Blosc Zstd Level 1", {"method": "blosc:zstd:1"}, {}),
                    ("Blosc Zstd Level 3", {"method": "blosc:zstd:3"}, {}),
                    ("Blosc LZ4 Level 1", {"method": "blosc:lz4:1"}, {}),
                    (
                        "Packing + Blosc Zstd",
                        {"method": "blosc:zstd:1"},
                        {"enabled": True, "bits": 16},
                    ),
                ]

                results = []
                for name, compression_config, packing_config in test_configs:
                    try:
                        # Create temporary files
                        with tempfile.TemporaryDirectory() as tmp_dir:
                            output_path = os.path.join(tmp_dir, "test.zarr")

                            # Time the conversion
                            start_time = time.time()

                            # Create a copy of the test dataset for this test
                            ds_copy = test_ds.copy()

                            # Setup encoding
                            encoding = {}

                            # Apply packing if configured
                            if packing_config.get("enabled"):
                                from .packing import Packer

                                packer = Packer(nbits=packing_config.get("bits", 16))

                                # Add valid range attributes if not present
                                ds_copy = packer.add_valid_range_attributes(
                                    ds_copy,
                                    buffer_factor=packing_config.get(
                                        "auto_buffer_factor", 0.01
                                    ),
                                )

                                # Setup encoding
                                packing_encoding = packer.setup_encoding(ds_copy)
                                encoding.update(packing_encoding)

                            # Apply compression if configured
                            if compression_config:
                                try:
                                    import zarr
                                    from numcodecs import Blosc

                                    method = compression_config.get("method", "")
                                    if method.startswith("blosc:"):
                                        parts = method.split(":")
                                        cname = parts[1] if len(parts) > 1 else "zstd"
                                        clevel = int(parts[2]) if len(parts) > 2 else 1

                                        compressor = Blosc(
                                            cname=cname,
                                            clevel=clevel,
                                            shuffle=Blosc.SHUFFLE,
                                        )

                                        # Apply compressor to all variables
                                        for var in ds_copy.data_vars:
                                            encoding.setdefault(var, {})[
                                                "compressor"
                                            ] = compressor
                                except ImportError:
                                    print(
                                        f"  {name}: Warning - zarr/numcodecs not available, compression disabled"
                                    )
                                except Exception as e:
                                    print(f"  {name}: Warning - compression error: {e}")

                            # Debug: Print encoding info
                            # print(f"  Debug - Encoding keys: {list(encoding.keys())}")

                            # Apply chunking based on actual dimensions
                            chunking_dict = {}
                            for dim in ds_copy.sizes:
                                # Use a reasonable chunk size for each dimension
                                if dim == "time":
                                    chunking_dict[dim] = min(10, ds_copy.sizes[dim])
                                elif dim in ["lat", "lon", "latitude", "longitude"]:
                                    chunking_dict[dim] = min(50, ds_copy.sizes[dim])
                                elif dim in ["dir", "freq"]:
                                    chunking_dict[dim] = min(10, ds_copy.sizes[dim])
                                else:
                                    # For other dimensions, use a moderate chunk size
                                    chunking_dict[dim] = min(20, ds_copy.sizes[dim])

                            ds_copy = ds_copy.chunk(chunking_dict)

                            # Perform conversion using xarray directly
                            try:
                                ds_copy.to_zarr(
                                    output_path, mode="w", encoding=encoding
                                )

                                end_time = time.time()

                                # Check output size
                                if os.path.exists(output_path):
                                    # Use a simple approach to get directory size
                                    import subprocess

                                    try:
                                        result = subprocess.run(
                                            ["du", "-sb", output_path],
                                            capture_output=True,
                                            text=True,
                                            check=True,
                                        )
                                        size_bytes = int(result.stdout.split()[0])
                                        size_mb = size_bytes / (1024**2)
                                    except:
                                        # Fallback method
                                        size_mb = 0
                                        for dirpath, dirnames, filenames in os.walk(
                                            output_path
                                        ):
                                            for filename in filenames:
                                                filepath = os.path.join(
                                                    dirpath, filename
                                                )
                                                try:
                                                    size_mb += os.path.getsize(
                                                        filepath
                                                    ) / (1024**2)
                                                except:
                                                    pass

                                    results.append(
                                        {
                                            "name": name,
                                            "time": end_time - start_time,
                                            "size_mb": size_mb,
                                        }
                                    )
                                    print(
                                        f"  {name}: {size_mb:.2f} MB in {end_time - start_time:.2f}s"
                                    )
                                else:
                                    print(f"  {name}: Failed to create output")
                            except Exception as e:
                                print(f"  {name}: Error - {e}")
                    except Exception as e:
                        print(f"  {name}: Error - {e}")

                # Show comparison
                if results:
                    print("\nPerformance Comparison:")
                    print("-" * 22)
                    base_result = results[0]  # No compression, no packing
                    for result in results:
                        size_ratio = (
                            base_result["size_mb"] / result["size_mb"]
                            if result["size_mb"] > 0
                            else 0
                        )
                        time_ratio = (
                            result["time"] / base_result["time"]
                            if base_result["time"] > 0
                            else 0
                        )
                        print(f"  {result['name']}:")
                        print(
                            f"    Size: {result['size_mb']:.2f} MB ({size_ratio:.1f}x smaller)"
                        )
                        print(
                            f"    Time: {result['time']:.2f}s ({time_ratio:.1f}x slower)"
                        )
                    print()
            else:
                print("No variables found for testing.")
                print()

        # Interactive configuration setup
        if args.interactive:
            print("\nInteractive Configuration Setup:")
            print("=" * 32)

            config = {}

            # Chunking selection
            print("\nChunking Configuration:")
            print("Available access patterns:")
            for i, pattern in enumerate(access_patterns, 1):
                rec = chunk_recommendations.get(pattern)
                if rec:
                    print(
                        f"{i}. {pattern.capitalize()} - {rec.chunks} ({rec.estimated_chunk_size_mb:.2f} MB)"
                    )

            try:
                choice = int(
                    input("\nSelect access pattern (1-3) or 0 for custom: ") or "3"
                )
                if 1 <= choice <= 3:
                    selected_pattern = access_patterns[choice - 1]
                    config["chunking"] = chunk_recommendations[selected_pattern].chunks
                    print(f"Selected {selected_pattern} chunking: {config['chunking']}")
                elif choice == 0:
                    custom_chunks = {}
                    for dim in dimensions.keys():
                        size = input(
                            f"Enter chunk size for {dim} (default {dimensions[dim]}): "
                        )
                        if size:
                            custom_chunks[dim] = int(size)
                    if custom_chunks:
                        config["chunking"] = custom_chunks
                        print(f"Custom chunking: {custom_chunks}")
            except (ValueError, IndexError):
                print("Using balanced chunking as default")
                config["chunking"] = (
                    chunk_recommendations.get("balanced", {}).chunks or {}
                )

            # Packing selection
            print("\nPacking Configuration:")
            pack_choice = input("Enable data packing? (y/N): ").lower()
            if pack_choice == "y":
                config["packing"] = {"enabled": True}

                bits_choice = input("Bits for packing (8/16/32, default 16): ") or "16"
                try:
                    config["packing"]["bits"] = int(bits_choice)
                except ValueError:
                    config["packing"]["bits"] = 16
                    print("Using default 16 bits")

                if variables_without_ranges:
                    range_choice = (
                        input(
                            "\nHow to handle variables without valid ranges?\n1. Automatic calculation with buffer\n2. Specify manual ranges\n3. Skip packing for these variables\nChoose (1-3, default 1): "
                        )
                        or "1"
                    )
                    if range_choice == "2":
                        manual_ranges = {}
                        for var in variables_without_ranges:
                            min_val = input(f"Enter min value for {var} (or skip): ")
                            max_val = input(f"Enter max value for {var} (or skip): ")
                            if min_val and max_val:
                                try:
                                    manual_ranges[var] = {
                                        "min": float(min_val),
                                        "max": float(max_val),
                                    }
                                except ValueError:
                                    print(
                                        f"Skipping manual range for {var} due to invalid input"
                                    )
                        if manual_ranges:
                            config["packing"]["manual_ranges"] = manual_ranges
                    elif range_choice == "1":
                        buffer_choice = (
                            input(
                                "Buffer factor for automatic calculation (default 0.01): "
                            )
                            or "0.01"
                        )
                        try:
                            config["packing"]["auto_buffer_factor"] = float(
                                buffer_choice
                            )
                        except ValueError:
                            config["packing"]["auto_buffer_factor"] = 0.01
                            print("Using default buffer factor")

            # Compression selection
            print("\nCompression Configuration:")
            compress_choice = input("Enable compression? (y/N): ").lower()
            if compress_choice == "y":
                print("Compression options:")
                print("1. blosc:zstd:1 - Fast compression, good balance")
                print("2. blosc:zstd:3 - Higher compression, slower")
                print("3. blosc:lz4:1 - Very fast compression")
                print("4. blosc:lz4:3 - Higher compression, slower")
                comp_choice = input("Choose compression (1-4, default 1): ") or "1"
                compression_methods = {
                    "1": "blosc:zstd:1",
                    "2": "blosc:zstd:3",
                    "3": "blosc:lz4:1",
                    "4": "blosc:lz4:3",
                }
                config["compression"] = {
                    "method": compression_methods.get(comp_choice, "blosc:zstd:1")
                }
                print(f"Selected compression: {config['compression']['method']}")

            # Save configuration
            save_choice = input("\nSave configuration to file? (y/N): ").lower()
            if save_choice == "y":
                config_filename = (
                    input("Configuration filename (default config.yaml): ")
                    or "config.yaml"
                )
                try:
                    import yaml

                    with open(config_filename, "w") as f:
                        yaml.dump(config, f, default_flow_style=False)
                    print(f"Configuration saved to {config_filename}")
                except Exception as e:
                    print(f"Error saving configuration: {e}")

            print("\nConfiguration Summary:")
            print("-" * 20)
            for key, value in config.items():
                print(f"{key}: {value}")

        # Close dataset
        ds.close()
        print("\nAnalysis complete!")

    except Exception as e:
        logger.error(f"Analysis failed: {e}")
        raise


def main() -> None:
    """Main entry point for CLI."""
    parser = argparse.ArgumentParser(
        description="zarrify - Convert scientific data to Zarr format",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
examples:
  # Convert NetCDF to Zarr
  zarrify convert input.nc output.zarr

  # Convert with chunking
  zarrify convert input.nc output.zarr --chunking "time:100,lat:50,lon:100"

  # Convert with compression
  zarrify convert input.nc output.zarr --compression "blosc:zstd:3"

  # Convert with data packing
  zarrify convert input.nc output.zarr --packing --packing-bits 16

  # Convert with manual packing ranges
  zarrify convert input.nc output.zarr --packing
      --packing-manual-ranges '{"temperature": {"min": 0, "max": 100}}'

  # Analyze NetCDF file for optimization recommendations
  zarrify analyze input.nc

  # Analyze with theoretical performance benefits
  zarrify analyze input.nc --test-performance

  # Analyze with actual performance tests
  zarrify analyze input.nc --run-tests

  # Analyze with interactive configuration setup
  zarrify analyze input.nc --interactive

  # Create template for parallel writing
  zarrify create-template template.nc archive.zarr --global-start 2023-01-01 --global-end 2023-12-31

  # Write region to existing archive
  zarrify write-region data.nc archive.zarr

  # Append to existing Zarr store
  zarrify append new_data.nc existing.zarr

  # Convert to datamesh datasource
  zarrify convert input.nc --datamesh-datasource '{"id":"my_datasource","name":"My Data","coordinates":{"x":"longitude","y":"latitude","t":"time"}}' --datamesh-token $DATAMESH_TOKEN
        """,
    )

    parser.add_argument("--version", action="version", version=f"zarrify {__version__}")

    parser.add_argument(
        "-v",
        "--verbose",
        action="count",
        default=0,
        help="Increase verbosity (use -v, -vv, or -vvv)",
    )

    subparsers = parser.add_subparsers(dest="command", help="Available commands")

    # Convert command
    convert_parser = subparsers.add_parser(
        "convert", help="Convert data to Zarr format"
    )
    convert_parser.add_argument("input", help="Input file path")
    convert_parser.add_argument("output", help="Output Zarr store path")
    convert_parser.add_argument(
        "--chunking", help="Chunking specification (e.g., 'time:100,lat:50,lon:100')"
    )
    convert_parser.add_argument(
        "--compression", help="Compression specification (e.g., 'blosc:zstd:3')"
    )
    convert_parser.add_argument(
        "--packing", action="store_true", help="Enable data packing"
    )
    convert_parser.add_argument(
        "--packing-bits",
        type=int,
        default=16,
        choices=[8, 16, 32],
        help="Number of bits for packing (default: 16)",
    )
    convert_parser.add_argument(
        "--packing-manual-ranges",
        help='Manual min/max ranges as JSON string (e.g., \'{"temperature": {"min": 0, "max": 100}}\')',
    )
    convert_parser.add_argument(
        "--packing-auto-buffer-factor",
        type=float,
        default=0.01,
        help="Buffer factor for automatically calculated ranges (default: 0.01)",
    )
    convert_parser.add_argument(
        "--packing-check-range-exceeded",
        action="store_true",
        default=True,
        help="Check if data exceeds specified ranges (default: True)",
    )
    convert_parser.add_argument(
        "--packing-range-exceeded-action",
        choices=["warn", "error", "ignore"],
        default="warn",
        help="Action when data exceeds range (default: warn)",
    )
    convert_parser.add_argument(
        "--variables", help="Comma-separated list of variables to include"
    )
    convert_parser.add_argument(
        "--drop-variables", help="Comma-separated list of variables to exclude"
    )
    convert_parser.add_argument(
        "--attrs", help="Additional global attributes as JSON string"
    )
    convert_parser.add_argument(
        "--time-dim", default="time", help="Name of time dimension (default: time)"
    )
    convert_parser.add_argument(
        "--datamesh-datasource", help="Datamesh datasource configuration as JSON string"
    )
    convert_parser.add_argument(
        "--datamesh-token", help="Datamesh token for authentication"
    )
    convert_parser.add_argument(
        "--datamesh-service",
        default="https://datamesh-v1.oceanum.io",
        help="Datamesh service URL",
    )
    convert_parser.add_argument(
        "--target-chunk-size-mb",
        type=int,
        help="Target chunk size in MB for intelligent chunking (default: 50)",
    )
    convert_parser.set_defaults(func=convert_command)

    # Append command
    append_parser = subparsers.add_parser(
        "append", help="Append data to existing Zarr store"
    )
    append_parser.add_argument("input", help="Input file path")
    append_parser.add_argument("zarr", help="Existing Zarr store path")
    append_parser.add_argument(
        "--chunking", help="Chunking specification (e.g., 'time:100,lat:50,lon:100')"
    )
    append_parser.add_argument(
        "--variables", help="Comma-separated list of variables to include"
    )
    append_parser.add_argument(
        "--drop-variables", help="Comma-separated list of variables to exclude"
    )
    append_parser.add_argument(
        "--append-dim", default="time", help="Dimension to append along (default: time)"
    )
    append_parser.add_argument(
        "--time-dim", default="time", help="Name of time dimension (default: time)"
    )
    append_parser.add_argument(
        "--datamesh-datasource", help="Datamesh datasource configuration as JSON string"
    )
    append_parser.add_argument(
        "--datamesh-token", help="Datamesh token for authentication"
    )
    append_parser.add_argument(
        "--datamesh-service",
        default="https://datamesh-v1.oceanum.io",
        help="Datamesh service URL",
    )
    append_parser.add_argument(
        "--target-chunk-size-mb",
        type=int,
        help="Target chunk size in MB for intelligent chunking (default: 50)",
    )
    append_parser.add_argument("--config", help="Configuration file (YAML or JSON)")
    append_parser.set_defaults(func=append_command)

    # Create template command
    template_parser = subparsers.add_parser(
        "create-template", help="Create template Zarr archive for parallel writing"
    )
    template_parser.add_argument("template", help="Template NetCDF file")
    template_parser.add_argument("output", help="Output Zarr store path")
    template_parser.add_argument(
        "--chunking", help="Chunking specification (e.g., 'time:100,lat:50,lon:100')"
    )
    template_parser.add_argument(
        "--compression", help="Compression specification (e.g., 'blosc:zstd:3')"
    )
    template_parser.add_argument(
        "--packing", action="store_true", help="Enable data packing"
    )
    template_parser.add_argument(
        "--packing-bits",
        type=int,
        default=16,
        choices=[8, 16, 32],
        help="Number of bits for packing (default: 16)",
    )
    template_parser.add_argument(
        "--packing-manual-ranges",
        help='Manual min/max ranges as JSON string (e.g., \'{"temperature": {"min": 0, "max": 100}}\')',
    )
    template_parser.add_argument(
        "--packing-auto-buffer-factor",
        type=float,
        default=0.01,
        help="Buffer factor for automatically calculated ranges (default: 0.01)",
    )
    template_parser.add_argument(
        "--packing-check-range-exceeded",
        action="store_true",
        default=True,
        help="Check if data exceeds specified ranges (default: True)",
    )
    template_parser.add_argument(
        "--packing-range-exceeded-action",
        choices=["warn", "error", "ignore"],
        default="warn",
        help="Action when data exceeds range (default: warn)",
    )
    template_parser.add_argument(
        "--global-start", help="Start time for full archive (e.g., '2023-01-01')"
    )
    template_parser.add_argument(
        "--global-end", help="End time for full archive (e.g., '2023-12-31')"
    )
    template_parser.add_argument(
        "--freq", help="Time frequency (e.g., '1D', '1H', inferred if not provided)"
    )
    template_parser.add_argument(
        "--metadata-only",
        action="store_true",
        help="Create metadata only (compute=False)",
    )
    template_parser.add_argument(
        "--time-dim", default="time", help="Name of time dimension (default: time)"
    )
    template_parser.add_argument(
        "--datamesh-datasource", help="Datamesh datasource configuration as JSON string"
    )
    template_parser.add_argument(
        "--datamesh-token", help="Datamesh token for authentication"
    )
    template_parser.add_argument(
        "--datamesh-service",
        default="https://datamesh-v1.oceanum.io",
        help="Datamesh service URL",
    )
    template_parser.add_argument(
        "--target-chunk-size-mb",
        type=int,
        help="Target chunk size in MB for intelligent chunking (default: 50)",
    )
    template_parser.add_argument("--config", help="Configuration file (YAML or JSON)")
    template_parser.set_defaults(func=create_template_command)

    # Write region command
    region_parser = subparsers.add_parser(
        "write-region", help="Write data to specific region of Zarr archive"
    )
    region_parser.add_argument("input", help="Input file path")
    region_parser.add_argument("zarr", help="Existing Zarr store path")
    region_parser.add_argument(
        "--chunking", help="Chunking specification (e.g., 'time:100,lat:50,lon:100')"
    )
    region_parser.add_argument(
        "--region", help="Region specification (e.g., 'time=0:100,lat=0:50')"
    )
    region_parser.add_argument(
        "--variables", help="Comma-separated list of variables to include"
    )
    region_parser.add_argument(
        "--drop-variables", help="Comma-separated list of variables to exclude"
    )
    region_parser.add_argument(
        "--time-dim", default="time", help="Name of time dimension (default: time)"
    )
    region_parser.add_argument(
        "--datamesh-datasource", help="Datamesh datasource configuration as JSON string"
    )
    region_parser.add_argument(
        "--datamesh-token", help="Datamesh token for authentication"
    )
    region_parser.add_argument(
        "--datamesh-service",
        default="https://datamesh-v1.oceanum.io",
        help="Datamesh service URL",
    )
    region_parser.add_argument(
        "--target-chunk-size-mb",
        type=int,
        help="Target chunk size in MB for intelligent chunking (default: 50)",
    )
    region_parser.add_argument("--config", help="Configuration file (YAML or JSON)")
    region_parser.set_defaults(func=write_region_command)

    # Analyze command
    analyze_parser = subparsers.add_parser(
        "analyze", help="Analyze NetCDF file and recommend optimization options"
    )
    analyze_parser.add_argument("input", help="Input NetCDF file path")
    analyze_parser.add_argument(
        "--target-chunk-size-mb",
        type=int,
        help="Target chunk size in MB for analysis (default: 50)",
    )
    analyze_parser.add_argument(
        "--test-performance",
        action="store_true",
        help="Show theoretical performance benefits",
    )
    analyze_parser.add_argument(
        "--run-tests",
        action="store_true",
        help="Run actual performance tests to measure real-world benefits",
    )
    analyze_parser.add_argument(
        "-i",
        "--interactive",
        action="store_true",
        help="Interactive mode to guide configuration setup",
    )
    analyze_parser.set_defaults(func=analyze_command)

    # Parse arguments
    args = parser.parse_args()

    # Setup logging
    setup_logging(args.verbose)

    # Execute command
    if hasattr(args, "func"):
        try:
            args.func(args)
        except Exception as e:
            logger.error(f"Command failed: {e}")
            sys.exit(1)
    else:
        parser.print_help()
        sys.exit(1)


if __name__ == "__main__":
    main()
