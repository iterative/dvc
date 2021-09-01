import configobj

from dvc.cli import parse_args
from dvc.command.machine import (
    CmdMachineAdd,
    CmdMachineCreate,
    CmdMachineDestroy,
    CmdMachineRemove,
    CmdMachineSsh,
)


def test_add(tmp_dir):
    tmp_dir.gen({".dvc": {"config": "[feature]\n  machine = true"}})
    cli_args = parse_args(["machine", "add", "foo", "aws"])
    assert cli_args.func == CmdMachineAdd
    cmd = cli_args.func(cli_args)
    assert cmd.run() == 0
    config = configobj.ConfigObj(str(tmp_dir / ".dvc" / "config"))
    assert config['machine "foo"']["cloud"] == "aws"


def test_remove(tmp_dir):
    tmp_dir.gen(
        {
            ".dvc": {
                "config": (
                    "[feature]\n"
                    "  machine = true\n"
                    "['machine \"foo\"']\n"
                    "  cloud = aws"
                )
            }
        }
    )
    cli_args = parse_args(["machine", "remove", "foo"])
    assert cli_args.func == CmdMachineRemove
    cmd = cli_args.func(cli_args)
    assert cmd.run() == 0
    config = configobj.ConfigObj(str(tmp_dir / ".dvc" / "config"))
    assert list(config.keys()) == ["feature"]


def test_create(tmp_dir, dvc, mocker):
    cli_args = parse_args(["machine", "create", "foo"])
    assert cli_args.func == CmdMachineCreate

    cmd = cli_args.func(cli_args)
    m = mocker.patch.object(
        cmd.repo.machine, "create", autospec=True, return_value=0
    )

    assert cmd.run() == 0
    m.assert_called_once_with("foo")


def test_destroy(tmp_dir, dvc, mocker):
    cli_args = parse_args(["machine", "destroy", "foo"])
    assert cli_args.func == CmdMachineDestroy

    cmd = cli_args.func(cli_args)
    m = mocker.patch.object(
        cmd.repo.machine, "destroy", autospec=True, return_value=0
    )

    assert cmd.run() == 0
    m.assert_called_once_with("foo")


def test_ssh(tmp_dir, dvc, mocker):
    cli_args = parse_args(["machine", "ssh", "foo"])
    assert cli_args.func == CmdMachineSsh

    cmd = cli_args.func(cli_args)
    m = mocker.patch.object(
        cmd.repo.machine, "run_shell", autospec=True, return_value=0
    )

    assert cmd.run() == 0
    m.assert_called_once_with("foo")
