import json

import pytest

from dvc.cli import parse_args
from dvc.commands.status import CmdDataStatus


def test_cloud_status(tmp_dir, dvc, mocker):
    cli_args = parse_args(
        [
            "status",
            "--cloud",
            "target1",
            "target2",
            "--jobs",
            "2",
            "--remote",
            "remote",
            "--all-branches",
            "--all-tags",
            "--all-commits",
            "--with-deps",
            "--recursive",
        ]
    )
    assert cli_args.func == CmdDataStatus

    cmd = cli_args.func(cli_args)
    m = mocker.patch.object(cmd.repo, "status", autospec=True, return_value={})

    assert cmd.run() == 0

    m.assert_called_once_with(
        cloud=True,
        targets=["target1", "target2"],
        jobs=2,
        remote="remote",
        all_branches=True,
        all_tags=True,
        all_commits=True,
        with_deps=True,
        recursive=True,
        check_updates=True,
    )


@pytest.mark.parametrize("status", [{}, {"a": "b", "c": [1, 2, 3]}, [1, 2, 3]])
def test_status_show_json(dvc, mocker, capsys, status):
    cli_args = parse_args(["status", "--json"])
    assert cli_args.func == CmdDataStatus

    cmd = cli_args.func(cli_args)

    mocker.patch.object(cmd.repo, "status", autospec=True, return_value=status)

    assert cmd.run() == 0
    out, _ = capsys.readouterr()
    assert json.dumps(status) in out


@pytest.mark.parametrize(
    "status, ret", [({}, 0), ({"a": "b", "c": [1, 2, 3]}, 1), ([1, 2, 3], 1)]
)
def test_status_quiet(dvc, mocker, caplog, capsys, status, ret):
    cli_args = parse_args(["status", "-q"])
    assert cli_args.func == CmdDataStatus

    cmd = cli_args.func(cli_args)

    mocker.patch.object(cmd.repo, "status", autospec=True, return_value=status)
    caplog.clear()
    assert cmd.run() == ret
    assert not caplog.messages
    captured = capsys.readouterr()
    assert not captured.out


def test_status_empty(dvc, mocker, capsys):
    from dvc.repo.index import Index

    cli_args = parse_args(["status"])
    assert cli_args.func == CmdDataStatus

    cmd = cli_args.func(cli_args)

    spy = mocker.spy(Index, "from_repo")

    assert cmd.run() == 0

    captured = capsys.readouterr()
    assert "no data or pipelines tracked" in captured.out
    # stages should only be collected once
    assert spy.call_count == 1


@pytest.mark.parametrize(
    "cloud_opts, expected_message",
    [
        (["--cloud"], "Cache and remote 'default' are in sync"),
        (["--remote", "remote1"], "Cache and remote 'remote1' are in sync"),
        ([], "Data and pipelines are up to date"),
    ],
)
def test_status_up_to_date(dvc, mocker, capsys, cloud_opts, expected_message):
    from dvc.repo.index import Index

    cli_args = parse_args(["status", *cloud_opts])
    assert cli_args.func == CmdDataStatus

    cmd = cli_args.func(cli_args)

    mocker.patch.dict(cmd.repo.config, {"core": {"remote": "default"}})
    mocker.patch.object(cmd.repo, "status", autospec=True, return_value={})
    mocker.patch("dvc.repo.Repo.index", return_value=Index(dvc, [object()]))
    cmd.repo._reset = mocker.Mock()

    assert cmd.run() == 0
    captured = capsys.readouterr()
    assert expected_message in captured.out


def test_status_check_updates(dvc, mocker, capsys):
    cli_args = parse_args(["status", "--no-updates"])
    assert cli_args.func == CmdDataStatus

    cmd = cli_args.func(cli_args)
    m = mocker.patch.object(cmd.repo, "status", autospec=True, return_value={})

    assert cmd.run() == 0

    m.assert_called_once_with(
        cloud=False,
        targets=[],
        jobs=None,
        remote=None,
        all_branches=False,
        all_tags=False,
        all_commits=False,
        with_deps=False,
        recursive=False,
        check_updates=False,
    )
