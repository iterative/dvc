import re

from dvc.command.version import CmdVersion
from dvc.cli import parse_args


def test_run_in_repo(repo_dir, dvc):
    cmd = CmdVersion(parse_args(["version"]))
    ret = cmd.run_cmd()
    assert ret == 0


def test_run_outside_of_repo(repo_dir):
    cmd = CmdVersion(parse_args(["version"]))
    ret = cmd.run_cmd()
    assert ret == 0


def test_info(caplog):
    cmd = CmdVersion(parse_args(["version"]))
    cmd.run()

    assert re.search(re.compile(r"DVC version: \d+\.\d+\.\d+"), caplog.text)
    assert re.search(re.compile(r"Python version: \d\.\d\.\d"), caplog.text)
    assert re.search(re.compile(r"Platform: .*"), caplog.text)
