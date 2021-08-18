from tests.shell import Command


def test_startup(dvc_cmd: Command, benchmark):
    result = benchmark(dvc_cmd.args("--help"))
    assert result.returncode == 0
