import re

from dvc.command.version import CmdVersion
from dvc.cli import parse_args


def test_run():
    cmd = CmdVersion(parse_args(["version"]))
    ret = cmd.run_cmd()
    assert ret == 0


def test_info(caplog):
    check_data = ["DVC version", "Python version", "Platform"]
    cmd = CmdVersion(parse_args(["version"]))
    cmd.run()
    info_dict = {}

    for record in caplog.records:
        msg = re.sub(r"\n", ": ", record.msg)
        info_list = msg.split(": ")
    for i in range(0, len(info_list), 2):
        info_dict[info_list[i]] = info_list[i + 1]

    for info_type in check_data:
        assert re.match(r".*", info_dict.get(info_type))
