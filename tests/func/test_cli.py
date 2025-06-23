import os

import pytest

from dvc.cli import DvcParserError, parse_args
from dvc.cli.command import CmdBase
from dvc.commands.add import CmdAdd
from dvc.commands.checkout import CmdCheckout
from dvc.commands.config import CmdConfig
from dvc.commands.data_sync import CmdDataPull, CmdDataPush
from dvc.commands.init import CmdInit
from dvc.commands.remove import CmdRemove
from dvc.commands.repro import CmdRepro
from dvc.commands.status import CmdDataStatus
from dvc.exceptions import NotDvcRepoError


def test_argparse(dvc):
    args = parse_args(["init"])
    assert isinstance(args.func(args), CmdInit)


def test_pull(dvc):
    args = parse_args(["pull"])
    cmd = args.func(args)
    assert isinstance(cmd, CmdDataPull)

    cmd.repo.close()


def test_push(dvc):
    args = parse_args(["push"])
    cmd = args.func(args)
    assert isinstance(cmd, CmdDataPush)

    cmd.repo.close()


def test_status(dvc):
    args = parse_args(["status"])
    cmd = args.func(args)
    assert isinstance(cmd, CmdDataStatus)

    cmd.repo.close()


def test_repro(dvc):
    target1 = "1"
    target2 = "2"

    args = parse_args(
        ["repro", target1, target2, "-f", "--force", "-s", "--single-item"]
    )

    cmd = args.func(args)
    assert isinstance(cmd, CmdRepro)
    assert args.targets == [target1, target2]
    assert args.force
    assert args.single_item

    cmd.repo.close()


def test_remove(dvc):
    target1 = "1"
    target2 = "2"

    args = parse_args(["remove", target1, target2])

    cmd = args.func(args)
    assert isinstance(cmd, CmdRemove)
    assert args.targets == [target1, target2]

    cmd.repo.close()


def test_add(dvc):
    target1 = "1"
    target2 = "2"

    args = parse_args(["add", target1, target2])

    cmd = args.func(args)
    assert isinstance(cmd, CmdAdd)
    assert args.targets == [target1, target2]

    cmd.repo.close()


def test_config_unset(dvc):
    name = "section.option"
    value = "1"

    args = parse_args(["config", "-u", "--unset", name, value])

    cmd = args.func(args)
    assert isinstance(cmd, CmdConfig)
    assert args.unset
    assert args.name == (None, "section", "option")
    assert args.value == value


def test_config_list():
    args = parse_args(["config", "--list"])

    assert args.list
    assert args.name is None
    assert args.value is None


def test_checkout(dvc):
    args = parse_args(["checkout"])
    cmd = args.func(args)
    assert isinstance(cmd, CmdCheckout)

    cmd.repo.close()


def test_find_root(dvc):
    class Cmd(CmdBase):
        def run(self):
            pass

    class A:
        quiet = False
        verbose = True
        wait_for_lock = False
        cd = os.path.pardir

    args = A()
    with pytest.raises(NotDvcRepoError):
        Cmd(args)


def test_cd(dvc):
    class Cmd(CmdBase):
        def run(self):
            pass

    class A:
        quiet = False
        verbose = True
        wait_for_lock = False
        cd = os.path.pardir

    parent_dir = os.path.realpath(os.path.pardir)
    args = A()
    with pytest.raises(NotDvcRepoError):
        Cmd(args)
    current_dir = os.path.realpath(os.path.curdir)
    assert parent_dir == current_dir


def test_unknown_command_help(capsys):
    try:
        _ = parse_args(["unknown"])
    except DvcParserError:
        pass
    captured = capsys.readouterr()
    output = captured.out
    try:
        _ = parse_args(["--help"])
    except SystemExit:
        pass
    captured = capsys.readouterr()
    help_output = captured.out
    assert output == help_output


def test_unknown_subcommand_help(capsys):
    sample_subcommand = "push"
    try:
        _ = parse_args([sample_subcommand, "--unknown"])
    except DvcParserError:
        pass
    captured = capsys.readouterr()
    output = captured.out
    try:
        _ = parse_args([sample_subcommand, "--help"])
    except SystemExit:
        pass
    captured = capsys.readouterr()
    help_output = captured.out
    assert output == help_output
