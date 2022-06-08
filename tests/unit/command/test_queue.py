from dvc.cli import parse_args
from dvc.commands.queue.attach import CmdQueueAttach
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


def test_experiments_status(dvc, scm, mocker):
    cli_args = parse_args(
        [
            "queue",
            "status",
        ]
    )
    assert cli_args.func == CmdQueueStatus

    cmd = cli_args.func(cli_args)
    m = mocker.patch(
        "dvc.repo.experiments.queue.local.LocalCeleryQueue.status",
    )

    assert cmd.run() == 0
    m.assert_called_once_with()


def test_experiments_attach(dvc, scm, mocker):
    cli_args = parse_args(
        [
            "queue",
            "attach",
            "exp1",
            "-e",
            "utf8",
        ]
    )
    assert cli_args.func == CmdQueueAttach

    cmd = cli_args.func(cli_args)
    m = mocker.patch(
        "dvc.repo.experiments.queue.local.LocalCeleryQueue.attach",
        return_value={},
    )

    assert cmd.run() == 0
    m.assert_called_once_with(rev="exp1", encoding="utf8")


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
