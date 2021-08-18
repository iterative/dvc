import pytest

from tests.shell import Command


@pytest.fixture
def dvc_cmd(pytestconfig) -> "Command":
    return Command("dvc")
