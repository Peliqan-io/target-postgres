"""
Shared configuration for target-postgres regression tests.
"""
import pytest


def pytest_configure(config):
    config.addinivalue_line("markers", "regression: target-postgres 3.9→3.11 migration regression tests")
