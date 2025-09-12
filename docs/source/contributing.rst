Contributing to zarrify
========================

Thank you for considering contributing to zarrify! We welcome contributions from everyone.

Types of Contributions
----------------------

Bug Reports
^^^^^^^^^^^

If you find a bug, please report it by creating an issue on GitHub with:

* A clear, descriptive title
* Steps to reproduce the bug
* Expected vs actual behavior
* Environment information (Python version, OS, etc.)

Feature Requests
^^^^^^^^^^^^^^^^

We welcome suggestions for new features. Please create an issue with:

* A clear description of the feature
* Use cases and benefits
* Any implementation ideas

Code Contributions
^^^^^^^^^^^^^^^^^^

We welcome code contributions including:

* Bug fixes
* New features
* Performance improvements
* Documentation improvements
* Test enhancements

Getting Started
---------------

Setting Up Development Environment
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

1. Fork the repository on GitHub
2. Clone your fork locally:

   .. code-block:: bash

       git clone https://github.com/your-username/zarrify.git
       cd zarrify

3. Create a virtual environment:

   .. code-block:: bash

       python -m venv venv
       source venv/bin/activate  # On Windows: venv\Scripts\activate

4. Install development dependencies:

   .. code-block:: bash

       pip install -e ".[dev]"

5. Install pre-commit hooks:

   .. code-block:: bash

       pre-commit install

Running Tests
^^^^^^^^^^^^^

Run the test suite:

.. code-block:: bash

    pytest

Run tests with coverage:

.. code-block:: bash

    pytest --cov=zarrify

Code Style
^^^^^^^^^^

We follow these style guidelines:

* **Black**: Code formatting
* **Flake8**: Code style checking
* **Mypy**: Type checking
* **PEP 8**: Python style guide

Run code formatting:

.. code-block:: bash

    black .

Run style checking:

.. code-block:: bash

    flake8

Run type checking:

.. code-block:: bash

    mypy zarrify

Pull Request Process
--------------------

1. Create a new branch for your feature or bugfix:

   .. code-block:: bash

       git checkout -b feature/my-new-feature

2. Make your changes
3. Add tests for your changes
4. Ensure all tests pass:

   .. code-block:: bash

       pytest

5. Format your code:

   .. code-block:: bash

       black .

6. Check code style:

   .. code-block:: bash

       flake8

7. Check types:

   .. code-block:: bash

       mypy zarrify

8. Commit your changes:

   .. code-block:: bash

       git commit -am "Add my new feature"

9. Push to your fork:

   .. code-block:: bash

       git push origin feature/my-new-feature

10. Create a Pull Request on GitHub

Pull Request Guidelines
-----------------------

* **Clear Description**: Explain what your changes do and why
* **Related Issues**: Reference any related issues
* **Tests**: Include tests for your changes
* **Documentation**: Update documentation as needed
* **Small Changes**: Keep PRs focused on a single feature or bugfix

Code Review Process
-------------------

All pull requests are reviewed by maintainers. Reviews focus on:

* Code quality and maintainability
* Correctness and performance
* Test coverage
* Documentation
* Following project conventions

We aim to review PRs within 48 hours.

Development Workflow
--------------------

Branch Naming
^^^^^^^^^^^^^

* ``feature/feature-name`` for new features
* ``bugfix/bug-description`` for bug fixes
* ``docs/documentation-topic`` for documentation changes
* ``perf/performance-improvement`` for performance improvements

Commit Messages
^^^^^^^^^^^^^^^

Follow conventional commit format:

* ``feat: Add new feature``
* ``fix: Resolve bug issue``
* ``docs: Update documentation``
* ``test: Add test coverage``
* ``refactor: Improve code structure``
* ``perf: Optimize performance``
* ``chore: Maintenance tasks``

Versioning
^^^^^^^^^^

We follow semantic versioning (SemVer):

* MAJOR version for incompatible API changes
* MINOR version for new functionality
* PATCH version for bug fixes

Documentation
-------------

Writing Documentation
^^^^^^^^^^^^^^^^^^^^^

* Use clear, concise language
* Provide examples for complex concepts
* Keep documentation up to date with code changes
* Use proper formatting and structure

Building Documentation
^^^^^^^^^^^^^^^^^^^^^^

Build the documentation locally:

.. code-block:: bash

    cd docs
    make html

The built documentation will be in ``docs/_build/html/``.

Testing
-------

Test Structure
^^^^^^^^^^^^^^

* Unit tests for individual functions and classes
* Integration tests for combined functionality
* CLI tests for command-line interface
* Performance tests for critical paths

Test Guidelines
^^^^^^^^^^^^^^^

* Use descriptive test names
* Test edge cases and error conditions
* Mock external dependencies when appropriate
* Keep tests fast and isolated
* Use fixtures for common setup

Running Specific Tests
^^^^^^^^^^^^^^^^^^^^^^

Run a specific test file:

.. code-block:: bash

    pytest tests/test_core.py

Run a specific test:

.. code-block:: bash

    pytest tests/test_core.py::test_zarr_converter_basic

Run tests with verbose output:

.. code-block:: bash

    pytest -v

Code Quality
------------

Type Hints
^^^^^^^^^^

All new code should include type hints. Use:

* ``typing`` module for complex types
* Generic types when appropriate
* Type aliases for complex type expressions

Error Handling
^^^^^^^^^^^^^^

* Use custom exceptions for domain-specific errors
* Provide clear error messages
* Handle edge cases gracefully
* Log errors appropriately

Performance
^^^^^^^^^^^

* Profile performance-critical code
* Avoid unnecessary computations
* Use efficient algorithms and data structures
* Consider memory usage

Security
^^^^^^^^

* Validate all inputs
* Handle file paths securely
* Avoid injection vulnerabilities
* Follow security best practices

Community
---------

Communication
^^^^^^^^^^^^^

* Be respectful and inclusive
* Provide constructive feedback
* Help others learn and grow
* Share knowledge and experience

Code of Conduct
^^^^^^^^^^^^^^^

We follow a code of conduct that promotes:

* Respect and inclusivity
* Professionalism
* Constructive feedback
* Collaborative spirit

Reporting Issues
----------------

If you encounter issues with the contribution process:

1. Check existing issues and documentation
2. Ask questions in issues or discussions
3. Contact maintainers directly if needed

Thank You!
----------

Thank you for contributing to zarrify! Your contributions help make this project better for everyone.