import configobj

from dvc.cli import parse_args
from dvc.command.machine import (
    CmdMachineAdd,
    CmdMachineCreate,
    CmdMachineDestroy,
    CmdMachineList,
    CmdMachineModify,
    CmdMachineRemove,
    CmdMachineRename,
    CmdMachineSsh,
    CmdMachineStatus,
)

DATA = {
    ".dvc": {
        "config": (
            "[feature]\n"
            "  machine = true\n"
            "['machine \"foo\"']\n"
            "  cloud = aws"
        )
    }
}


def test_add(tmp_dir):
    tmp_dir.gen({".dvc": {"config": "[feature]\n  machine = true"}})
    cli_args = parse_args(["machine", "add", "foo", "aws"])
    assert cli_args.func == CmdMachineAdd
    cmd = cli_args.func(cli_args)
    assert cmd.run() == 0
    config = configobj.ConfigObj(str(tmp_dir / ".dvc" / "config"))
    assert config['machine "foo"']["cloud"] == "aws"


def test_remove(tmp_dir):
    tmp_dir.gen(DATA)
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


def test_status(tmp_dir, dvc, mocker):
    cli_args = parse_args(["machine", "status", "foo"])
    assert cli_args.func == CmdMachineStatus

    cmd = cli_args.func(cli_args)
    m = mocker.patch.object(
        cmd.repo.machine, "status", autospec=True, return_value=[]
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


def test_list(tmp_dir, mocker):
    from dvc.ui import ui

    tmp_dir.gen(DATA)
    cli_args = parse_args(["machine", "list", "foo"])
    assert cli_args.func == CmdMachineList
    cmd = cli_args.func(cli_args)
    m = mocker.patch.object(ui, "write", autospec=True)
    assert cmd.run() == 0
    m.assert_called_once_with("cloud=aws")


def test_modified(tmp_dir):
    tmp_dir.gen(DATA)
    cli_args = parse_args(["machine", "modify", "foo", "cloud", "azure"])
    assert cli_args.func == CmdMachineModify
    cmd = cli_args.func(cli_args)
    assert cmd.run() == 0
    config = configobj.ConfigObj(str(tmp_dir / ".dvc" / "config"))
    assert config['machine "foo"']["cloud"] == "azure"


def test_rename(tmp_dir, scm, dvc):
    tmp_dir.gen(DATA)
    cli_args = parse_args(["machine", "rename", "foo", "bar"])
    assert cli_args.func == CmdMachineRename
    cmd = cli_args.func(cli_args)
    assert cmd.run() == 0
    config = configobj.ConfigObj(str(tmp_dir / ".dvc" / "config"))
    assert config['machine "bar"']["cloud"] == "aws"
