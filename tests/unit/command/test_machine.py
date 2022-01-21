import os

import configobj
import pytest
from mock import call

from dvc.cli import parse_args
from dvc.commands.machine import (
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
            "  cloud = aws\n"
            "['machine \"myaws\"']\n"
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
    assert list(config.keys()) == ["feature", 'machine "myaws"']


def test_create(tmp_dir, dvc, mocker):
    cli_args = parse_args(["machine", "create", "foo"])
    assert cli_args.func == CmdMachineCreate

    cmd = cli_args.func(cli_args)
    m = mocker.patch.object(
        cmd.repo.machine, "create", autospec=True, return_value=0
    )

    assert cmd.run() == 0
    m.assert_called_once_with("foo")


def test_status(tmp_dir, scm, dvc, mocker):
    tmp_dir.gen(DATA)
    cli_args = parse_args(["machine", "status", "foo"])
    assert cli_args.func == CmdMachineStatus

    cmd = cli_args.func(cli_args)
    m = mocker.patch.object(
        cmd.repo.machine, "status", autospec=True, return_value=[]
    )
    assert cmd.run() == 0
    m.assert_called_once_with("foo")

    cli_args = parse_args(["machine", "status"])
    cmd = cli_args.func(cli_args)
    m = mocker.patch.object(
        cmd.repo.machine, "status", autospec=True, return_value=[]
    )
    assert cmd.run() == 0
    assert m.call_count == 2
    m.assert_has_calls([call("foo"), call("myaws")])


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


@pytest.mark.parametrize("show_origin", [["--show-origin"], []])
def test_list(tmp_dir, mocker, show_origin):
    from dvc.compare import TabularData
    from dvc.ui import ui

    tmp_dir.gen(DATA)
    cli_args = parse_args(["machine", "list"] + show_origin + ["foo"])
    assert cli_args.func == CmdMachineList
    cmd = cli_args.func(cli_args)
    if show_origin:
        m = mocker.patch.object(ui, "write", autospec=True)
    else:
        m = mocker.patch.object(TabularData, "render", autospec=True)
    assert cmd.run() == 0
    if show_origin:
        m.assert_called_once_with(f".dvc{os.sep}config	cloud=aws")
    else:
        m.assert_called_once()


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


@pytest.mark.parametrize(
    "cmd,use_config",
    [
        ("add", True),
        ("default", True),
        ("remove", True),
        ("list", True),
        ("modify", True),
        ("create", False),
        ("destroy", False),
        ("rename", True),
        ("status", False),
        ("ssh", False),
    ],
)
def test_help_message(tmp_dir, scm, dvc, cmd, use_config, capsys):

    try:
        parse_args(["machine", cmd, "--help"])
    except SystemExit:
        pass
    cap = capsys.readouterr()
    print(cap.out)
    assert ("--global" in cap.out) is use_config
