Data Packing
============

zarrio provides advanced data packing functionality to reduce storage requirements and improve I/O performance by compressing floating-point data using fixed-scale offset encoding.

Overview
--------

Data packing converts floating-point data to integer representations with a fixed scale and offset, significantly reducing storage size while maintaining reasonable precision. This is particularly useful for climate and weather data where full 64-bit precision is often unnecessary.

The packing process uses the formula::

    packed_value = (original_value - offset) / scale

Where ``scale`` and ``offset`` are computed based on the data range to optimally fit within the specified bit width.

Configuration
-------------

Packing can be configured through the ``PackingConfig`` model:

.. code-block:: python

    from zarrio.models import PackingConfig

    packing = PackingConfig(
        enabled=True,
        bits=16
    )

The ``PackingConfig`` supports the following fields:

- **enabled**: Whether to enable data packing (default: False)
- **bits**: Number of bits for packing (8, 16, or 32) (default: 16)
- **manual_ranges**: Manual min/max ranges for variables (default: None)
- **auto_buffer_factor**: Buffer factor for automatically calculated ranges (default: 0.01)
- **check_range_exceeded**: Whether to check if data exceeds specified ranges (default: True)
- **range_exceeded_action**: Action when data exceeds range ('warn', 'error', 'ignore') (default: 'warn')

Enhanced Packing Features
-------------------------

zarrio's enhanced packing functionality provides several improvements over basic packing:

Priority-Based Range Determination
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The enhanced packing system uses a clear priority order for determining the min/max values used for packing:

1. **Manual ranges** (if provided)
2. **Variable attributes** (valid_min/valid_max)
3. **Automatic calculation** from data (with warnings)

Manual Range Specification
~~~~~~~~~~~~~~~~~~~~~~~~~~

Users can explicitly specify min/max ranges for variables:

.. code-block:: python

    from zarrio.models import PackingConfig

    packing = PackingConfig(
        enabled=True,
        bits=16,
        manual_ranges={
            "temperature": {"min": -50, "max": 50},
            "pressure": {"min": 90000, "max": 110000}
        }
    )

Automatic Range Calculation with Buffer
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

When no ranges are provided, the system automatically calculates them from the data:

.. code-block:: python

    from zarrio.models import PackingConfig

    packing = PackingConfig(
        enabled=True,
        bits=16,
        auto_buffer_factor=0.05  # 5% buffer
    )

Range Exceeded Validation
~~~~~~~~~~~~~~~~~~~~~~~~~

Optional checking to ensure data doesn't exceed specified ranges:

.. code-block:: python

    from zarrio.models import PackingConfig

    packing = PackingConfig(
        enabled=True,
        bits=16,
        manual_ranges={"temperature": {"min": -50, "max": 50}},
        check_range_exceeded=True,
        range_exceeded_action="error"  # or "warn" or "ignore"
    )

Usage Examples
--------------

Functional API
~~~~~~~~~~~~~~

.. code-block:: python

    from zarrio import convert_to_zarr

    # Basic packing
    convert_to_zarr(
        "input.nc", 
        "output.zarr",
        packing=True,
        packing_bits=16
    )

    # Packing with manual ranges
    convert_to_zarr(
        "input.nc", 
        "output.zarr",
        packing=True,
        packing_bits=16,
        packing_manual_ranges={
            "temperature": {"min": -50, "max": 50},
            "pressure": {"min": 90000, "max": 110000}
        }
    )

    # Packing with automatic range calculation
    convert_to_zarr(
        "input.nc", 
        "output.zarr",
        packing=True,
        packing_bits=16,
        packing_auto_buffer_factor=0.05
    )

Class-Based API
~~~~~~~~~~~~~~~

.. code-block:: python

    from zarrio import ZarrConverter
    from zarrio.models import PackingConfig

    # Programmatic configuration
    packing_config = PackingConfig(
        enabled=True,
        bits=16,
        manual_ranges={
            "temperature": {"min": -50, "max": 50}
        }
    )

    converter = ZarrConverter(packing=packing_config)
    converter.convert("input.nc", "output.zarr")

Command Line Interface
~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: bash

    # Basic packing
    zarrio convert input.nc output.zarr --packing --packing-bits 16

    # Packing with manual ranges
    zarrio convert input.nc output.zarr --packing \\
        --packing-manual-ranges '{"temperature": {"min": -50, "max": 50}}'

    # Packing with automatic range calculation
    zarrio convert input.nc output.zarr --packing \\
        --packing-auto-buffer-factor 0.05

Configuration Files
~~~~~~~~~~~~~~~~~~~

YAML:

.. code-block:: yaml

    # config.yaml
    packing:
      enabled: true
      bits: 16
      manual_ranges:
        temperature:
          min: -50
          max: 50
        pressure:
          min: 90000
          max: 110000
      auto_buffer_factor: 0.05
      check_range_exceeded: true
      range_exceeded_action: warn

JSON:

.. code-block:: json

    {
      "packing": {
        "enabled": true,
        "bits": 16,
        "manual_ranges": {
          "temperature": {
            "min": -50,
            "max": 50
          }
        },
        "auto_buffer_factor": 0.05,
        "check_range_exceeded": true,
        "range_exceeded_action": "warn"
      }
    }

Best Practices
--------------

1. **Use Manual Ranges When Possible**: If you know the valid range of your data, specify it manually for optimal packing.

2. **Consider Data Distribution**: For data with non-uniform distributions, manual ranges may provide better precision.

3. **Monitor Range Exceeded Warnings**: Pay attention to warnings about data exceeding specified ranges.

4. **Choose Appropriate Bit Width**: 
   - 8 bits: High compression, lower precision
   - 16 bits: Good balance of compression and precision
   - 32 bits: Higher precision, lower compression

5. **Use Buffer for Automatic Ranges**: When using automatic range calculation, add a buffer to account for future data.

6. **Validate Your Data**: Use the range exceeded checking feature to catch data anomalies.

Warning System
--------------

The enhanced packing system provides informative warnings in various scenarios:

- When manual ranges override existing attributes
- When automatically calculating ranges (with note about potential inaccuracy for region-based archives)
- When data exceeds specified ranges

These warnings help ensure data integrity and inform users about potential issues.

Technical Details
-----------------

The packing implementation uses zarr's ``FixedScaleOffset`` codec to perform the actual compression. The ``Packer`` class handles the computation of scale and offset parameters based on the configured ranges.

For region-based archives written over time, automatically calculated ranges may be inaccurate since they're based only on the current region's data. Manual ranges or attribute-based ranges are recommended for these scenarios.