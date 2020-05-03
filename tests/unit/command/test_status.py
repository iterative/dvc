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
