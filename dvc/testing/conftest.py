from .fixtures import *  # noqa, pylint: disable=wildcard-import


def pytest_addoption(parser):
    from .benchmarks.conftest import pytest_addoption as bench_addoption

    bench_addoption(parser)
