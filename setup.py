"""Shim for Databricks Asset Bundles wheel builder.

DAB's default artifact builder runs `python setup.py bdist_wheel`.
This shim delegates to the real build system defined in pyproject.toml.
All package metadata lives in pyproject.toml -- this file has no config.
"""

from setuptools import setup

setup()
