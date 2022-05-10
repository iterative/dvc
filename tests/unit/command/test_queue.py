from dvc.cli import parse_args
from dvc.commands.queue.kill import CmdQueueKill
from dvc.commands.queue.remove import CmdQueueRemove


def test_experiments_remove(dvc, scm, mocker):
    cli_args = parse_args(
        [
            "queue",
            "remove",
            "--all",
        ]
    )
    assert cli_args.func == CmdQueueRemove

    cmd = cli_args.func(cli_args)
    m = mocker.patch(
        "dvc.repo.experiments.queue.local.LocalCeleryQueue.clear",
        return_value={},
    )

    assert cmd.run() == 0
    m.assert_called_once_with()

    cli_args = parse_args(
        [
            "queue",
            "remove",
            "exp1",
            "exp2",
        ]
    )
    assert cli_args.func == CmdQueueRemove

    cmd = cli_args.func(cli_args)
    m = mocker.patch(
        "dvc.repo.experiments.queue.local.LocalCeleryQueue.remove",
        return_value={},
    )

    assert cmd.run() == 0
    m.assert_called_once_with(revs=["exp1", "exp2"])


def test_experiments_kill(dvc, scm, mocker):
    cli_args = parse_args(
        [
            "queue",
            "kill",
            "exp1",
            "exp2",
        ]
    )
    assert cli_args.func == CmdQueueKill

    cmd = cli_args.func(cli_args)
    m = mocker.patch(
        "dvc.repo.experiments.queue.local.LocalCeleryQueue.kill",
        return_value={},
    )

    assert cmd.run() == 0
    m.assert_called_once_with(revs=["exp1", "exp2"])
