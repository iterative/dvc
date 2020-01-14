from dvc.cli import parse_args
from dvc.command.update import CmdUpdate


def test_update(dvc, mocker):
    targets = ["target1", "target2", "target3"]
    cli_args = parse_args(["update"] + targets)
    assert cli_args.func == CmdUpdate
    cmd = cli_args.func(cli_args)
    m = mocker.patch("dvc.repo.Repo.update")

    assert cmd.run() == 0

    calls = [mocker.call(target) for target in targets]
    m.assert_has_calls(calls)
