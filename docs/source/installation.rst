Installation
============

zarrify can be installed in several ways depending on your needs.

Using pip
---------

The easiest way to install zarrify is using pip:

.. code-block:: bash

    pip install zarrify

This will install the latest stable version from PyPI.

Using conda
-----------

If you're using conda, you can install zarrify from conda-forge:

.. code-block:: bash

    conda install -c conda-forge zarrify

Installing from source
----------------------

To install the latest development version from source:

.. code-block:: bash

    git clone https://github.com/oceanum/zarrify.git
    cd zarrify
    pip install -e .

Dependencies
------------

zarrify requires the following dependencies:

Core dependencies:
~~~~~~~~~~~~~~~~~~

- Python >= 3.8
- xarray >= 0.18.0
- zarr >= 2.10.0
- numpy >= 1.20.0
- pandas >= 1.3.0
- click >= 8.0.0
- pyyaml >= 5.4.0
- dask >= 2021.0.0
- netCDF4 >= 1.5.0

Optional dependencies:
~~~~~~~~~~~~~~~~~~~~~~

- blosc: For Blosc compression support
- numcodecs: For additional compression codecs
- intake: For intake catalog support
- fsspec: For cloud storage support
- gcsfs: For Google Cloud Storage support

Development dependencies:
~~~~~~~~~~~~~~~~~~~~~~~~~

- pytest >= 6.2.0
- pytest-cov >= 2.12.0
- black >= 21.0.0
- flake8 >= 3.9.0
- mypy >= 0.910
- sphinx >= 4.0.0
- sphinx-rtd-theme >= 1.0.0
- pre-commit >= 2.13.0

To install development dependencies:

.. code-block:: bash

    pip install -e ".[dev]"

Verification
------------

To verify that zarrify is installed correctly, you can run:

.. code-block:: bash

    python -c "import zarrify; print(f'zarrify version: {zarrify.__version__}')"

Or using the CLI:

.. code-block:: bash

    zarrify --version

Docker Installation
-------------------

zarrify can also be used via Docker containers. See the :doc:`docker` documentation for details on building and running Docker images.

System requirements
-------------------

zarrify is designed to work on Linux, macOS, and Windows systems with Python 3.8 or higher.

For large datasets, ensure you have sufficient disk space and memory. The library can handle datasets larger than available RAM through dask's chunked array operations.