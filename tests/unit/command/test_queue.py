from dvc.cli import parse_args
from dvc.commands.queue.kill import CmdQueueKill
from dvc.commands.queue.remove import CmdQueueRemove
from dvc.commands.queue.start import CmdQueueStart
from dvc.commands.queue.stop import CmdQueueStop


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


def test_experiments_start(dvc, scm, mocker):
    cli_args = parse_args(["queue", "start", "-j", "3"])
    assert cli_args.func == CmdQueueStart

    cmd = cli_args.func(cli_args)
    m = mocker.patch(
        "dvc.repo.experiments.queue.local.LocalCeleryQueue.spawn_worker",
    )

    assert cmd.run() == 0
    assert m.call_count == 3


def test_experiments_stop(dvc, scm, mocker):
    cli_args = parse_args(
        [
            "queue",
            "stop",
            "--kill",
        ]
    )
    assert cli_args.func == CmdQueueStop

    cmd = cli_args.func(cli_args)
    m = mocker.patch(
        "dvc.repo.experiments.queue.local.LocalCeleryQueue.shutdown",
    )

    assert cmd.run() == 0
    m.assert_called_once_with(kill=True)
