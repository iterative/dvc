import logging
from pathlib import Path

from dvc.cli import parse_args
from dvc.commands.add import CmdAdd
from dvc_objects.executors import ThreadPoolExecutor


def test_add(mocker, dvc):
    cli_args = parse_args(
        [
            "add",
            "--recursive",
            "--no-commit",
            "--external",
            "--glob",
            "--file",
            "file",
            "--desc",
            "stage description",
            "data",
        ]
    )
    assert cli_args.func == CmdAdd

    cmd = cli_args.func(cli_args)
    m = mocker.patch.object(cmd.repo, "add", autospec=True)

    assert cmd.run() == 0

    m.assert_called_once_with(
        ["data"],
        recursive=True,
        no_commit=True,
        glob=True,
        fname="file",
        external=True,
        out=None,
        remote=None,
        to_remote=False,
        desc="stage description",
        type=None,
        labels=None,
        meta=None,
        jobs=None,
    )


def test_add_commit_with_jobs(mocker, dvc):
    for name in ("foo", "bar"):
        filepath = Path(dvc.root_dir) / "data" / f"{name}.txt"
        filepath.parent.mkdir(exist_ok=True, parents=True)
        filepath.write_text(name)

    cli_args = parse_args(
        [
            "add",
            "--jobs",
            "1",
            "--desc",
            "stage description",
            "data",
        ]
    )
    assert cli_args.func == CmdAdd

    cmd = cli_args.func(cli_args)
    m1 = mocker.spy(cmd.repo, "add")

    # Custom patching of ThreadPoolExecutor __init__ since doing it with
    # unittest.mock is less clear, longer and just as hacky.
    called_max_workers = None
    executor_init = ThreadPoolExecutor.__init__

    def patch_init(self, max_workers=None, cancel_on_error=False, **kwargs):
        nonlocal called_max_workers
        called_max_workers = max_workers
        return executor_init(self, max_workers, cancel_on_error, **kwargs)

    ThreadPoolExecutor.__init__ = patch_init

    assert cmd.run() == 0

    m1.assert_called_once_with(
        ["data"],
        recursive=False,
        no_commit=False,
        glob=False,
        fname=None,
        external=False,
        out=None,
        remote=None,
        to_remote=False,
        desc="stage description",
        jobs=1,
    )

    assert called_max_workers == 1

    ThreadPoolExecutor.__init__ = executor_init


def test_add_to_remote(mocker):
    cli_args = parse_args(
        [
            "add",
            "s3://bucket/foo",
            "--to-remote",
            "--out",
            "bar",
            "--remote",
            "remote",
        ]
    )
    assert cli_args.func == CmdAdd

    cmd = cli_args.func(cli_args)
    m = mocker.patch.object(cmd.repo, "add", autospec=True)

    assert cmd.run() == 0

    m.assert_called_once_with(
        ["s3://bucket/foo"],
        recursive=False,
        no_commit=False,
        glob=False,
        fname=None,
        external=False,
        out="bar",
        remote="remote",
        to_remote=True,
        desc=None,
        type=None,
        labels=None,
        meta=None,
        jobs=None,
    )


def test_add_to_remote_invalid_combinations(mocker, caplog):
    cli_args = parse_args(
        ["add", "s3://bucket/foo", "s3://bucket/bar", "--to-remote"]
    )
    assert cli_args.func == CmdAdd

    cmd = cli_args.func(cli_args)
    with caplog.at_level(logging.ERROR, logger="dvc"):
        assert cmd.run() == 1
        expected_msg = "multiple targets can't be used with --to-remote"
        assert expected_msg in caplog.text

    for option, value in (("--remote", "remote"),):
        cli_args = parse_args(["add", "foo", option, value])

        cmd = cli_args.func(cli_args)
        with caplog.at_level(logging.ERROR, logger="dvc"):
            assert cmd.run() == 1
            expected_msg = f"{option} can't be used without --to-remote"
            assert expected_msg in caplog.text


def test_add_to_cache_invalid_combinations(mocker, caplog):
    cli_args = parse_args(
        ["add", "s3://bucket/foo", "s3://bucket/bar", "-o", "foo"]
    )
    assert cli_args.func == CmdAdd

    cmd = cli_args.func(cli_args)
    with caplog.at_level(logging.ERROR, logger="dvc"):
        assert cmd.run() == 1
        expected_msg = "multiple targets can't be used with -o"
        assert expected_msg in caplog.text
