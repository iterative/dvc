from dvc.cli import parse_args
from dvc.command.machine import CmdMachineCreate, CmdMachineDestroy


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
