"""
Command-line interface for zarrify with Pydantic configuration support.
"""

import argparse
import logging
import sys
import json
from pathlib import Path
from typing import Dict, Any

import yaml
import xarray as xr

from .core import ZarrConverter
from .packing import Packer
from .models import ZarrConverterConfig, load_config_from_file
from .__init__ import __version__

logger = logging.getLogger(__name__)


def setup_logging(verbosity: int = 0) -> None:
    """Setup logging based on verbosity level."""
    levels = [logging.WARNING, logging.INFO, logging.DEBUG]
    level = levels[min(verbosity, len(levels) - 1)]
    logging.basicConfig(
        level=level,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
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
        config_dict.setdefault('chunking', {}).update(chunking)
    if args.compression:
        config_dict.setdefault('compression', {})['method'] = args.compression
    if args.packing:
        config_dict.setdefault('packing', {})['enabled'] = True
    if args.packing_bits:
        config_dict.setdefault('packing', {})['bits'] = args.packing_bits
    if args.time_dim:
        config_dict.setdefault('time', {})['dim'] = args.time_dim
    if args.target_chunk_size_mb:
        config_dict['target_chunk_size_mb'] = args.target_chunk_size_mb
    if args.attrs:
        config_dict['attrs'] = json.loads(args.attrs)
    
    # Add datamesh config if provided
    if args.datamesh_datasource:
        config_dict.setdefault('datamesh', {})
        config_dict['datamesh']['datasource'] = json.loads(args.datamesh_datasource)
        if args.datamesh_token:
            config_dict['datamesh']['token'] = args.datamesh_token
        if args.datamesh_service:
            config_dict['datamesh']['service'] = args.datamesh_service
    
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
        drop_variables=drop_variables
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
        config_dict.setdefault('chunking', {}).update(chunking)
    if args.append_dim:
        config_dict.setdefault('time', {})['append_dim'] = args.append_dim
    if args.time_dim:
        config_dict.setdefault('time', {})['dim'] = args.time_dim
    if args.target_chunk_size_mb:
        config_dict['target_chunk_size_mb'] = args.target_chunk_size_mb
    
    # Add datamesh config if provided
    if args.datamesh_datasource:
        config_dict.setdefault('datamesh', {})
        config_dict['datamesh']['datasource'] = json.loads(args.datamesh_datasource)
        if args.datamesh_token:
            config_dict['datamesh']['token'] = args.datamesh_token
        if args.datamesh_service:
            config_dict['datamesh']['service'] = args.datamesh_service
    
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
        drop_variables=drop_variables
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
        config_dict.setdefault('chunking', {}).update(chunking)
    if args.compression:
        config_dict.setdefault('compression', {})['method'] = args.compression
    if args.packing:
        config_dict.setdefault('packing', {})['enabled'] = True
    if args.packing_bits:
        config_dict.setdefault('packing', {})['bits'] = args.packing_bits
    if args.time_dim:
        config_dict.setdefault('time', {})['dim'] = args.time_dim
    if args.target_chunk_size_mb:
        config_dict['target_chunk_size_mb'] = args.target_chunk_size_mb
    
    # Add datamesh config if provided
    if args.datamesh_datasource:
        config_dict.setdefault('datamesh', {})
        config_dict['datamesh']['datasource'] = json.loads(args.datamesh_datasource)
        if args.datamesh_token:
            config_dict['datamesh']['token'] = args.datamesh_token
        if args.datamesh_service:
            config_dict['datamesh']['service'] = args.datamesh_service
    
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
        compute=not args.metadata_only
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
        config_dict.setdefault('chunking', {}).update(chunking)
    if args.time_dim:
        config_dict.setdefault('time', {})['dim'] = args.time_dim
    if args.target_chunk_size_mb:
        config_dict['target_chunk_size_mb'] = args.target_chunk_size_mb
    
    # Add datamesh config if provided
    if args.datamesh_datasource:
        config_dict.setdefault('datamesh', {})
        config_dict['datamesh']['datasource'] = json.loads(args.datamesh_datasource)
        if args.datamesh_token:
            config_dict['datamesh']['token'] = args.datamesh_token
        if args.datamesh_service:
            config_dict['datamesh']['service'] = args.datamesh_service
    
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
        drop_variables=drop_variables
    )
    
    logger.info("Region writing completed successfully")


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
  
  # Create template for parallel writing
  zarrify create-template template.nc archive.zarr --global-start 2023-01-01 --global-end 2023-12-31
  
  # Write region to existing archive
  zarrify write-region data.nc archive.zarr
  
  # Append to existing Zarr store
  zarrify append new_data.nc existing.zarr
  
  # Convert to datamesh datasource
  zarrify convert input.nc --datamesh-datasource '{"id":"my_datasource","name":"My Data","coordinates":{"x":"longitude","y":"latitude","t":"time"}}' --datamesh-token $DATAMESH_TOKEN
        """
    )
    
    parser.add_argument(
        "--version",
        action="version",
        version=f"zarrify {__version__}"
    )
    
    parser.add_argument(
        "-v", "--verbose",
        action="count",
        default=0,
        help="Increase verbosity (use -v, -vv, or -vvv)"
    )
    
    subparsers = parser.add_subparsers(dest="command", help="Available commands")
    
    # Convert command
    convert_parser = subparsers.add_parser("convert", help="Convert data to Zarr format")
    convert_parser.add_argument("input", help="Input file path")
    convert_parser.add_argument("output", help="Output Zarr store path")
    convert_parser.add_argument(
        "--chunking",
        help="Chunking specification (e.g., 'time:100,lat:50,lon:100')"
    )
    convert_parser.add_argument(
        "--compression",
        help="Compression specification (e.g., 'blosc:zstd:3')"
    )
    convert_parser.add_argument(
        "--packing",
        action="store_true",
        help="Enable data packing"
    )
    convert_parser.add_argument(
        "--packing-bits",
        type=int,
        default=16,
        choices=[8, 16, 32],
        help="Number of bits for packing (default: 16)"
    )
    convert_parser.add_argument(
        "--variables",
        help="Comma-separated list of variables to include"
    )
    convert_parser.add_argument(
        "--drop-variables",
        help="Comma-separated list of variables to exclude"
    )
    convert_parser.add_argument(
        "--attrs",
        help="Additional global attributes as JSON string"
    )
    convert_parser.add_argument(
        "--time-dim",
        default="time",
        help="Name of time dimension (default: time)"
    )
    convert_parser.add_argument(
        "--datamesh-datasource",
        help="Datamesh datasource configuration as JSON string"
    )
    convert_parser.add_argument(
        "--datamesh-token",
        help="Datamesh token for authentication"
    )
    convert_parser.add_argument(
        "--datamesh-service",
        default="https://datamesh-v1.oceanum.io",
        help="Datamesh service URL"
    )
    convert_parser.add_argument(
        "--target-chunk-size-mb",
        type=int,
        help="Target chunk size in MB for intelligent chunking (default: 50)"
    )
    convert_parser.set_defaults(func=convert_command)
    
    # Append command
    append_parser = subparsers.add_parser("append", help="Append data to existing Zarr store")
    append_parser.add_argument("input", help="Input file path")
    append_parser.add_argument("zarr", help="Existing Zarr store path")
    append_parser.add_argument(
        "--chunking",
        help="Chunking specification (e.g., 'time:100,lat:50,lon:100')"
    )
    append_parser.add_argument(
        "--variables",
        help="Comma-separated list of variables to include"
    )
    append_parser.add_argument(
        "--drop-variables",
        help="Comma-separated list of variables to exclude"
    )
    append_parser.add_argument(
        "--append-dim",
        default="time",
        help="Dimension to append along (default: time)"
    )
    append_parser.add_argument(
        "--time-dim",
        default="time",
        help="Name of time dimension (default: time)"
    )
    append_parser.add_argument(
        "--datamesh-datasource",
        help="Datamesh datasource configuration as JSON string"
    )
    append_parser.add_argument(
        "--datamesh-token",
        help="Datamesh token for authentication"
    )
    append_parser.add_argument(
        "--datamesh-service",
        default="https://datamesh-v1.oceanum.io",
        help="Datamesh service URL"
    )
    append_parser.add_argument(
        "--target-chunk-size-mb",
        type=int,
        help="Target chunk size in MB for intelligent chunking (default: 50)"
    )
    append_parser.add_argument(
        "--config",
        help="Configuration file (YAML or JSON)"
    )
    append_parser.set_defaults(func=append_command)
    
    # Create template command
    template_parser = subparsers.add_parser("create-template", help="Create template Zarr archive for parallel writing")
    template_parser.add_argument("template", help="Template NetCDF file")
    template_parser.add_argument("output", help="Output Zarr store path")
    template_parser.add_argument(
        "--chunking",
        help="Chunking specification (e.g., 'time:100,lat:50,lon:100')"
    )
    template_parser.add_argument(
        "--compression",
        help="Compression specification (e.g., 'blosc:zstd:3')"
    )
    template_parser.add_argument(
        "--packing",
        action="store_true",
        help="Enable data packing"
    )
    template_parser.add_argument(
        "--packing-bits",
        type=int,
        default=16,
        choices=[8, 16, 32],
        help="Number of bits for packing (default: 16)"
    )
    template_parser.add_argument(
        "--global-start",
        help="Start time for full archive (e.g., '2023-01-01')"
    )
    template_parser.add_argument(
        "--global-end",
        help="End time for full archive (e.g., '2023-12-31')"
    )
    template_parser.add_argument(
        "--freq",
        help="Time frequency (e.g., '1D', '1H', inferred if not provided)"
    )
    template_parser.add_argument(
        "--metadata-only",
        action="store_true",
        help="Create metadata only (compute=False)"
    )
    template_parser.add_argument(
        "--time-dim",
        default="time",
        help="Name of time dimension (default: time)"
    )
    template_parser.add_argument(
        "--datamesh-datasource",
        help="Datamesh datasource configuration as JSON string"
    )
    template_parser.add_argument(
        "--datamesh-token",
        help="Datamesh token for authentication"
    )
    template_parser.add_argument(
        "--datamesh-service",
        default="https://datamesh-v1.oceanum.io",
        help="Datamesh service URL"
    )
    template_parser.add_argument(
        "--target-chunk-size-mb",
        type=int,
        help="Target chunk size in MB for intelligent chunking (default: 50)"
    )
    template_parser.add_argument(
        "--config",
        help="Configuration file (YAML or JSON)"
    )
    template_parser.set_defaults(func=create_template_command)
    
    # Write region command
    region_parser = subparsers.add_parser("write-region", help="Write data to specific region of Zarr archive")
    region_parser.add_argument("input", help="Input file path")
    region_parser.add_argument("zarr", help="Existing Zarr store path")
    region_parser.add_argument(
        "--chunking",
        help="Chunking specification (e.g., 'time:100,lat:50,lon:100')"
    )
    region_parser.add_argument(
        "--region",
        help="Region specification (e.g., 'time=0:100,lat=0:50')"
    )
    region_parser.add_argument(
        "--variables",
        help="Comma-separated list of variables to include"
    )
    region_parser.add_argument(
        "--drop-variables",
        help="Comma-separated list of variables to exclude"
    )
    region_parser.add_argument(
        "--time-dim",
        default="time",
        help="Name of time dimension (default: time)"
    )
    region_parser.add_argument(
        "--datamesh-datasource",
        help="Datamesh datasource configuration as JSON string"
    )
    region_parser.add_argument(
        "--datamesh-token",
        help="Datamesh token for authentication"
    )
    region_parser.add_argument(
        "--datamesh-service",
        default="https://datamesh-v1.oceanum.io",
        help="Datamesh service URL"
    )
    region_parser.add_argument(
        "--target-chunk-size-mb",
        type=int,
        help="Target chunk size in MB for intelligent chunking (default: 50)"
    )
    region_parser.add_argument(
        "--config",
        help="Configuration file (YAML or JSON)"
    )
    region_parser.set_defaults(func=write_region_command)
    
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