.PHONY: clean clean-test clean-pyc clean-build docs help
.DEFAULT_GOAL := help

define BROWSER_PYSCRIPT
import os, webbrowser, sys

try:
	from urllib import pathname2url
except:
	from urllib.request import pathname2url

webbrowser.open("file://" + pathname2url(os.path.abspath(sys.argv[1])))
endef
export BROWSER_PYSCRIPT

define PRINT_HELP_PYSCRIPT
import re, sys

for line in sys.stdin:
	match = re.match(r'^([a-zA-Z_-]+):.*?## (.*)$$', line)
	if match:
		target, help = match.groups()
		print("%-20s %s" % (target, help))
endef
export PRINT_HELP_PYSCRIPT

BROWSER := python -c "$$BROWSER_PYSCRIPT"

help:
	@python -c "$$PRINT_HELP_PYSCRIPT" < $(MAKEFILE_LIST)

clean: clean-build clean-pyc clean-test ## remove all build, test, coverage and Python artifacts

clean-build: ## remove build artifacts
	rm -fr build/
	rm -fr dist/
	rm -fr .eggs/
	find . -name '*.egg-info' -exec rm -fr {} +
	find . -name '*.egg' -exec rm -f {} +

clean-pyc: ## remove Python file artifacts
	find . -name '*.pyc' -exec rm -f {} +
	find . -name '*.pyo' -exec rm -f {} +
	find . -name '*~' -exec rm -f {} +
	find . -name '__pycache__' -exec rm -fr {} +

clean-test: ## remove test and coverage artifacts
	rm -fr .tox/
	rm -f .coverage
	rm -fr htmlcov/
	rm -fr .pytest_cache

lint: ## check style with flake8
	flake8 zarrio tests

test: ## run tests quickly with the default Python
	pytest

test-quick: ## run tests quickly with the default Python
	pytest -v -m "not slow"

test-all: ## run tests on every Python version with tox
	tox

test-cov: ## run tests with coverage
	pytest --cov=zarrio --cov-report=html

coverage: ## check code coverage quickly with the default Python
	coverage run --source zarrio -m pytest
	coverage report -m
	coverage html
	$(BROWSER) htmlcov/index.html

docs: ## generate Sphinx HTML documentation, including API docs
	rm -f docs/source/zarrio.rst
	rm -f docs/source/modules.rst
	sphinx-apidoc -o docs/source/ zarrio
	$(MAKE) -C docs clean
	$(MAKE) -C docs html
	$(BROWSER) docs/_build/html/index.html

servedocs: docs ## compile the docs watching for changes
	watchmedo shell-command -p '*.rst' -c '$(MAKE) -C docs html' -R -D .

release: dist ## package and upload a release
	twine upload dist/*

dist: clean ## builds source and wheel package
	python -m build
	ls -l dist

install: clean ## install the package to the active Python's site-packages
	pip install .

install-dev: clean ## install the package in development mode
	pip install -e ".[dev]"

activate-hooks: ## install hooks for pre-commit and pre-merge hooks
	pre-commit install

format: ## format code with black
	black zarrio tests examples

check: ## run all checks
	black --check zarrio tests examples
	flake8 zarrio tests examples
	mypy zarrio

publish-test: dist ## publish to test PyPI
	twine upload --repository testpypi dist/*

publish: dist ## publish to PyPI
	twine upload dist/*

.PHONY: all
all: clean install-dev activate-hooks test docs