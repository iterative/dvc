from dvc.main import main
from dvc.command.version import CmdVersion
from dvc.cli import parse_args

from tests.basic_env import TestDvc


class TestVersion(TestDvc):
    def test_run(self):
        ret = main(["version"])
        self.assertEqual(ret, 0)


def test_info(dvc, mocker, caplog):
    cmd = CmdVersion(parse_args(["version"]))
    cmd.run()
    for record in caplog.records:
        assert "DVC version" in record.msg
        assert "Python version" in record.msg
        assert "Platform" in record.msg
