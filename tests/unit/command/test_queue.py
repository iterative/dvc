from dvc.cli import parse_args
from dvc.commands.queue.kill import CmdQueueKill
from dvc.commands.queue.logs import CmdQueueLogs
from dvc.commands.queue.remove import CmdQueueRemove
from dvc.commands.queue.start import CmdQueueStart
from dvc.commands.queue.status import CmdQueueStatus
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


def test_experiments_status(dvc, scm, mocker, capsys, caplog):
    from datetime import datetime

    cli_args = parse_args(
        [
            "queue",
            "status",
        ]
    )
    assert cli_args.func == CmdQueueStatus

    cmd = cli_args.func(cli_args)
    status_result = [
        {
            "rev": "c61a525a4ff39007301b4516fb6e54b323a0587b",
            "name": "I40",
            "timestamp": datetime(2022, 6, 9, 20, 49, 48),
            "status": "Queued",
        },
        {
            "rev": "8da9c339da30636261a3491a90aafdb760a4168f",
            "name": "I60",
            "timestamp": datetime(2022, 6, 9, 20, 49, 43),
            "status": "Running",
        },
    ]
    m = mocker.patch(
        "dvc.repo.experiments.queue.local.LocalCeleryQueue.status",
        return_value=status_result,
    )

    assert cmd.run() == 0
    m.assert_called_once_with()
    log, _ = capsys.readouterr()
    assert "Task     Name    Created       Status" in log
    assert "c61a525  I40     Jun 09, 2022  Queued" in log
    assert "8da9c33  I60     Jun 09, 2022  Running" in log


def test_queue_logs(dvc, scm, mocker):
    cli_args = parse_args(["queue", "logs", "exp1", "-e", "utf8", "-f"])
    assert cli_args.func == CmdQueueLogs

    cmd = cli_args.func(cli_args)
    m = mocker.patch(
        "dvc.repo.experiments.queue.local.LocalCeleryQueue.logs",
        return_value={},
    )

    assert cmd.run() == 0
    m.assert_called_once_with(rev="exp1", encoding="utf8", follow=True)
