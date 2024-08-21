from .benchmarks.fixtures import *  # noqa: F403
from .fixtures import *  # noqa: F403


def pytest_generate_tests(metafunc):
    from .benchmarks.plugin import pytest_generate_tests as bench_generate_tests

    bench_generate_tests(metafunc)


def pytest_addoption(parser):
    from .benchmarks.plugin import pytest_addoption as bench_addoption

    bench_addoption(parser)


def pytest_configure(config):
    from .benchmarks.plugin import pytest_configure as bench_configure

    bench_configure(config)


def pytest_report_header(config):
    from .benchmarks.plugin import pytest_report_header as bench_report_header

    return bench_report_header(config)
