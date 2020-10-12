import json

import pytest

from dvc.cli import parse_args
from dvc.command.status import CmdDataStatus


def test_cloud_status(mocker):
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
    )


@pytest.mark.parametrize("status", [{}, {"a": "b", "c": [1, 2, 3]}, [1, 2, 3]])
def test_status_show_json(mocker, caplog, status):
    cli_args = parse_args(["status", "--show-json"])
    assert cli_args.func == CmdDataStatus

    cmd = cli_args.func(cli_args)

    mocker.patch.object(cmd.repo, "status", autospec=True, return_value=status)
    caplog.clear()
    assert cmd.run() == 0
    assert caplog.messages == [json.dumps(status)]


@pytest.mark.parametrize(
    "status, ret", [({}, 0), ({"a": "b", "c": [1, 2, 3]}, 1), ([1, 2, 3], 1)]
)
def test_status_quiet(mocker, caplog, capsys, status, ret):
    cli_args = parse_args(["status", "-q"])
    assert cli_args.func == CmdDataStatus

    cmd = cli_args.func(cli_args)

    mocker.patch.object(cmd.repo, "status", autospec=True, return_value=status)
    caplog.clear()
    assert cmd.run() == ret
    assert not caplog.messages
    captured = capsys.readouterr()
    assert not captured.err
    assert not captured.out
