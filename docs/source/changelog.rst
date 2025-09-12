Changelog
=========

0.1.0 (2025-01-01)
------------------

Features
^^^^^^^^

* **Complete Rewrite**: Fresh, clean codebase without legacy constraints
* **Modern Architecture**: Modular design with clear separation of concerns
* **Full Type Safety**: Comprehensive type hints throughout the codebase
* **Pydantic Configuration**: Type-safe configuration management with validation
* **Intelligent Chunking**: Automatic chunking analysis and recommendations
* **Parallel Writing**: Template creation and region writing for parallel processing
* **Command-Line Interface**: Comprehensive CLI with intuitive options
* **Configuration Files**: YAML and JSON configuration file support
* **Data Packing**: Fixed-scale offset encoding for compression
* **Compression Support**: Blosc and other compression algorithms
* **Time Series Handling**: Efficient handling of time-series data
* **Data Appending**: Append new data to existing Zarr archives
* **Retry Logic**: Automatic retries for handling missing data
* **Error Handling**: Custom exceptions with clear error messages
* **Logging**: Comprehensive logging for debugging and monitoring

API Changes
^^^^^^^^^^^

* **Simplified Interface**: Cleaner, more intuitive APIs
* **Functional API**: ``convert_to_zarr``, ``append_to_zarr`` functions
* **Class-Based API**: ``ZarrConverter`` class for advanced usage
* **CLI Interface**: ``zarrify`` command with comprehensive options
* **Configuration API**: Pydantic models for type-safe configuration

Breaking Changes
^^^^^^^^^^^^^^^^

* **Complete Rewrite**: No backward compatibility with onzarr 1.x
* **New Package Name**: ``zarrify`` instead of ``onzarr``
* **New Module Structure**: Clean, modular organization
* **New API**: Simplified, more intuitive interfaces
* **New Configuration**: Pydantic-based configuration management

Improvements
^^^^^^^^^^^^

* **Maintainability**: Clean, focused modules that are easy to understand and modify
* **Reliability**: Proper error handling with custom exceptions
* **Performance**: Optimized for modern xarray and zarr capabilities
* **Documentation**: Comprehensive documentation with examples
* **Testing**: Comprehensive test suite with good coverage
* **Developer Experience**: Better IDE support with type hints
* **User Experience**: Intuitive APIs and CLI with helpful documentation

Bug Fixes
^^^^^^^^^

* **Circular Imports**: Fixed all import issues from the original design
* **Complex Inheritance**: Eliminated complex inheritance hierarchies
* **Redundant Code**: Removed redundant custom implementations now available in xarray
* **Parameter Handling**: Simplified parameter handling with clear validation

Dependencies
^^^^^^^^^^^^

* **Python**: 3.8+
* **xarray**: 0.18.0+
* **zarr**: 2.10.0+
* **numpy**: 1.20.0+
* **pandas**: 1.3.0+
* **click**: 8.0.0+
* **pyyaml**: 5.4.0+
* **dask**: 2021.0.0+
* **netCDF4**: 1.5.0+

Development
^^^^^^^^^^^

* **Continuous Integration**: GitHub Actions for testing and linting
* **Pre-commit Hooks**: Automated code formatting and linting
* **Documentation**: Sphinx documentation with ReadTheDocs theme
* **Testing**: Pytest with comprehensive coverage
* **Type Checking**: MyPy for static type checking
* **Code Formatting**: Black for consistent code style
* **Linting**: Flake8 for code quality

Examples
^^^^^^^^

* **Basic Usage**: Simple conversion examples
* **Advanced Usage**: Complex configuration examples
* **CLI Usage**: Command-line interface examples
* **Configuration Files**: YAML and JSON configuration examples
* **Parallel Processing**: Template creation and region writing examples
* **Intelligent Chunking**: Automatic chunking analysis examples
* **Data Packing**: Compression examples with packing
* **Error Handling**: Exception handling examples

Documentation
^^^^^^^^^^^^^

* **Installation Guide**: Complete installation instructions
* **Quickstart Guide**: Getting started quickly
* **API Reference**: Comprehensive API documentation
* **CLI Documentation**: Command-line interface reference
* **Configuration Guide**: Pydantic configuration management
* **Parallel Processing**: Template creation and region writing guide
* **Chunking Guide**: Intelligent chunking analysis and recommendations
* **Examples**: Practical usage examples
* **Contributing Guide**: Developer contribution guidelines
* **Changelog**: Version history and changes

Initial Release Notes
---------------------

zarrify represents a complete rewrite of the original onzarr library with a focus on:

1. **Clean Architecture**: Fresh start without legacy constraints
2. **Modern Design**: Full type hints, proper error handling, clean APIs
3. **Enhanced Functionality**: Parallel writing, Pydantic configuration, intelligent chunking
4. **Better Maintainability**: Easy to understand and modify
5. **Improved Usability**: Intuitive APIs and comprehensive documentation
6. **Robust Implementation**: Proper error handling and testing
7. **Performance Optimization**: Leveraging modern xarray and zarr capabilities

Key Features in Initial Release:

* NetCDF to Zarr conversion with data packing
* Flexible chunking strategies with intelligent analysis
* Compression support with Blosc and other algorithms
* Time series handling with duplicate removal
* Data appending to existing archives
* **Parallel writing with template creation and region writing**
* **Pydantic configuration management and validation**
* **Intelligent chunking analysis and recommendations**
* **Retry logic for handling missing data**
* Comprehensive CLI with configuration file support
* Full test suite with excellent coverage
* Detailed documentation with practical examples

Migration from onzarr 1.x:

* **Breaking Changes**: Complete rewrite with no backward compatibility
* **New Package**: ``zarrify`` instead of ``onzarr``
* **New API**: Simplified, more intuitive interfaces
* **New Configuration**: Pydantic-based configuration management
* **Enhanced Features**: Parallel writing, intelligent chunking, retry logic

Future Roadmap:

* Performance optimization for large datasets
* Additional compression algorithms
* Cloud storage integration
* Advanced parallel processing features
* Machine learning-based chunking recommendations
* Benchmarking and optimization tools
* Extended documentation and tutorials
* Community feedback and feature requests