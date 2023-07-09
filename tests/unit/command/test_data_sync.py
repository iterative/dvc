from dvc.cli import parse_args
from dvc.commands.data_sync import CmdDataFetch, CmdDataPull, CmdDataPush


def test_fetch(mocker, dvc):
    cli_args = parse_args(
        [
            "fetch",
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
            "--run-cache",
            "--max-size",
            "10",
            "--type",
            "plots",
            "--type",
            "metrics",
        ]
    )
    assert cli_args.func == CmdDataFetch

    cmd = cli_args.func(cli_args)
    m = mocker.patch.object(cmd.repo, "fetch", autospec=True, return_value=0)

    assert cmd.run() == 0

    m.assert_called_once_with(
        targets=["target1", "target2"],
        jobs=2,
        remote="remote",
        all_branches=True,
        all_tags=True,
        all_commits=True,
        with_deps=True,
        recursive=True,
        run_cache=True,
        max_size=10,
        types=["plots", "metrics"],
    )


def test_pull(mocker, dvc):
    cli_args = parse_args(
        [
            "pull",
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
            "--force",
            "--recursive",
            "--run-cache",
            "--glob",
            "--allow-missing",
        ]
    )
    assert cli_args.func == CmdDataPull

    cmd = cli_args.func(cli_args)
    m = mocker.patch.object(cmd.repo, "pull", autospec=True)

    assert cmd.run() == 0

    m.assert_called_once_with(
        targets=["target1", "target2"],
        jobs=2,
        remote="remote",
        all_branches=True,
        all_tags=True,
        all_commits=True,
        with_deps=True,
        force=True,
        recursive=True,
        run_cache=True,
        glob=True,
        allow_missing=True,
    )


def test_push(mocker, dvc):
    cli_args = parse_args(
        [
            "push",
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
            "--run-cache",
            "--glob",
        ]
    )
    assert cli_args.func == CmdDataPush

    cmd = cli_args.func(cli_args)
    m = mocker.patch.object(cmd.repo, "push", autospec=True, return_value=0)

    assert cmd.run() == 0

    m.assert_called_once_with(
        targets=["target1", "target2"],
        jobs=2,
        remote="remote",
        all_branches=True,
        all_tags=True,
        all_commits=True,
        with_deps=True,
        recursive=True,
        run_cache=True,
        glob=True,
    )
