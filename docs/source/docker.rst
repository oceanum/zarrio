Docker Support
==============

zarrify can be easily deployed and run using Docker containers. This is particularly useful for:

- Ensuring consistent environments across different systems
- Simplifying deployment
- Running zarrify in cloud environments
- Isolating dependencies
- Parallel processing workflows

Docker Images
-------------

Two Docker images are provided:

1. **Development Image**: Includes all development tools and dependencies
2. **Production Image**: Minimal image optimized for production use

Building Images
---------------

To build the Docker images:

Development Image
~~~~~~~~~~~~~~~~~

.. code-block:: bash

    # Build the development image
    docker build -t zarrify:dev .

Production Image
~~~~~~~~~~~~~~~~

.. code-block:: bash

    # Build the production image
    docker build -f Dockerfile.prod -t zarrify:latest .

Using Docker Compose
~~~~~~~~~~~~~~~~~~~~

.. code-block:: bash

    # Build all services
    docker-compose build

Running Containers
------------------

Basic Usage
~~~~~~~~~~~

.. code-block:: bash

    # Run the container with default help command
    docker run --rm zarrify:latest

    # Convert a NetCDF file to Zarr (assuming files are in the current directory)
    docker run --rm -v $(pwd):/data zarrify:latest convert /data/input.nc /data/output.zarr

Development Container
~~~~~~~~~~~~~~~~~~~~~

.. code-block:: bash

    # Run a bash shell in the development container
    docker run --rm -it -v $(pwd):/app zarrify:dev bash

    # Or using docker-compose
    docker-compose run --rm zarrify-dev bash

Production Container
~~~~~~~~~~~~~~~~~~~~

.. code-block:: bash

    # Run the production container
    docker run --rm -v $(pwd):/data zarrify:latest --help

    # Convert files using the production container
    docker run --rm -v $(pwd):/data zarrify:latest convert /data/input.nc /data/output.zarr

Volume Mounting
---------------

To work with files on your host system, you need to mount volumes when running the container:

.. code-block:: bash

    # Mount current directory as /data in the container
    docker run --rm -v $(pwd):/data zarrify:latest convert /data/input.nc /data/output.zarr

    # For Windows (PowerShell)
    docker run --rm -v ${PWD}:/data zarrify:latest convert /data/input.nc /data/output.zarr

    # For Windows (Command Prompt)
    docker run --rm -v %cd%:/data zarrify:latest convert /data/input.nc /data/output.zarr

Create a ``data`` directory in your project root to store input/output files that will be accessible from the container.

Parallel Processing with Docker
-------------------------------

Docker is particularly useful for parallel processing workflows:

.. code-block:: bash

    # Create template
    docker run --rm -v $(pwd):/data zarrify:latest create-template /data/template.nc /data/archive.zarr \
        --global-start 2020-01-01 \
        --global-end 2023-12-31

    # Process multiple files in parallel containers
    docker run --rm -v $(pwd):/data zarrify:latest write-region /data/data_2020.nc /data/archive.zarr &
    docker run --rm -v $(pwd):/data zarrify:latest write-region /data/data_2021.nc /data/archive.zarr &
    docker run --rm -v $(pwd):/data zarrify:latest write-region /data/data_2022.nc /data/archive.zarr &
    docker run --rm -v $(pwd):/data zarrify:latest write-region /data/data_2023.nc /data/archive.zarr &

    # Wait for all processes to complete
    wait

Docker Compose Usage
--------------------

.. code-block:: bash

    # Start a development shell
    docker-compose run --rm zarrify-dev bash

    # Run the application with specific arguments
    docker-compose run --rm zarrify convert /data/input.nc /data/output.zarr

    # Build all services
    docker-compose build

Security Features
-----------------

The production Docker image includes several security features:

1. **Non-root User**: Runs as a dedicated ``onzarr`` user instead of root
2. **Minimal Dependencies**: Only includes necessary runtime dependencies
3. **File Ownership**: Proper file ownership management
4. **Slim Base Image**: Uses ``python:3.10-slim`` for reduced attack surface

Customization
-------------

You can customize the Docker images by modifying the Dockerfiles:

1. For development: ``Dockerfile``
2. For production: ``Dockerfile.prod``

Common customizations might include:

- Adding additional system dependencies
- Changing the base image
- Adding specific environment variables
- Modifying the entrypoint or default command

Troubleshooting
---------------

Common issues and solutions:

1. **Permission Errors**: Ensure the ``data`` directory has appropriate permissions
2. **File Not Found**: Verify file paths and volume mounting
3. **Memory Issues**: Monitor container memory usage for large datasets
4. **Network Issues**: For cloud storage, ensure network access from containers

Example with Debugging:

.. code-block:: bash

    docker run --rm -v $(pwd):/data zarrify:latest convert /data/input.nc /data/output.zarr --log-level DEBUG