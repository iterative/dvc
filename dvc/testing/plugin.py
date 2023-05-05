from .benchmarks.fixtures import *  # noqa, pylint: disable=wildcard-import
from .fixtures import *  # noqa, pylint: disable=wildcard-import


def pytest_generate_tests(metafunc):
    from .benchmarks.plugin import pytest_generate_tests as bench_generate_tests

    bench_generate_tests(metafunc)


def pytest_addoption(parser):
    from .benchmarks.plugin import pytest_addoption as bench_addoption

    bench_addoption(parser)


def pytest_configure(config):
    from .benchmarks.plugin import pytest_configure as bench_configure

    bench_configure(config)
