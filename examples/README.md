# Zarrify Examples

This directory contains examples demonstrating various features of the zarrify library.

## Available Examples

### 1. Basic Demo (`demo.py`)
A complete example showing how to:
- Create sample NetCDF data
- Convert data to Zarr format
- Use different chunking strategies
- Apply data packing and compression
- Append data to existing Zarr stores

### 2. Datamesh Integration Demo (`datamesh_demo.py`)
A complete example showing how to:
- Create realistic sample climate data
- Configure zarrify for datamesh integration
- Write data directly to a datamesh datasource
- Handle configuration validation
- Use both API and CLI interfaces

### 3. Intelligent Chunking Demo (`consolidated_chunking_demo.py`)
A comprehensive example showing:
- How zarrify's intelligent chunking works for different dataset sizes
- Performance differences between chunking strategies
- How to achieve optimal chunk sizes for your data
- Manual optimization techniques for small datasets

### 4. Configurable Chunking Demo (`configurable_chunking_demo.py`)
Demonstrates how to:
- Configure target chunk sizes for different environments
- Use function arguments for programmatic control
- Use environment variables for deployment-specific settings
- Configure chunking in ZarrConverterConfig

### 5. Parallel Writing Demo (`parallel_demo.py`)
Demonstrates how to:
- Create Zarr templates for parallel writing
- Write data regions in parallel processes
- Handle large datasets efficiently

### 6. Retry Logic Demo (`retry_logic_demo.py`)
Shows how to:
- Handle missing data with automatic retries
- Configure retry limits and strategies
- Validate data quality after writing

## Notes

1. **Token Management**: Datamesh examples use `DATAMESH_TOKEN` environment variable for security
2. **Realistic Data**: The demos create scientifically plausible climate data
3. **Error Handling**: Proper error handling for various scenarios
4. **Validation**: Configuration validation with helpful error messages

## Requirements

To run these examples, you need:
1. Python 3.8+
2. Required dependencies (see pyproject.toml)
3. For datamesh integration: Valid datamesh token and network access

Without a datamesh token, the datamesh demos will still run and show configuration, but won't actually write to datamesh.