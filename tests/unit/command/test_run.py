from dvc.cli import parse_args
from dvc.command.run import CmdRun


def test_run(mocker, dvc):
    cli_args = parse_args(
        [
            "run",
            "--deps",
            "deps",
            "--outs",
            "outs",
            "--outs-no-cache",
            "outs-no-cache",
            "--metrics",
            "metrics",
            "--metrics-no-cache",
            "metrics-no-cache",
            "--file",
            "file",
            "--cwd",
            "cwd",
            "--wdir",
            "wdir",
            "--no-exec",
            "--yes",
            "--overwrite-dvcfile",
            "--ignore-build-cache",
            "--remove-outs",
            "--no-commit",
            "--outs-persist",
            "outs-persist",
            "--outs-persist-no-cache",
            "outs-persist-no-cache",
            "--always-changed",
            "command",
        ]
    )
    assert cli_args.func == CmdRun

    cmd = cli_args.func(cli_args)
    m = mocker.patch.object(cmd.repo, "run", autospec=True)

    assert cmd.run() == 0

    m.assert_called_once_with(
        deps=["deps"],
        outs=["outs"],
        outs_no_cache=["outs-no-cache"],
        metrics=["metrics"],
        metrics_no_cache=["metrics-no-cache"],
        outs_persist=["outs-persist"],
        outs_persist_no_cache=["outs-persist-no-cache"],
        fname="file",
        cwd="cwd",
        wdir="wdir",
        no_exec=True,
        overwrite=True,
        ignore_build_cache=True,
        remove_outs=True,
        no_commit=True,
        always_changed=True,
        cmd="command",
    )


def test_run_args_from_cli(mocker, dvc):
    args = parse_args(["run", "echo", "foo"])
    cmd = args.func(args)
    m = mocker.patch.object(cmd.repo, "run", autospec=True)
    assert cmd.run() == 0
    m.assert_called_once_with(
        deps=[],
        outs=[],
        outs_no_cache=[],
        metrics=[],
        metrics_no_cache=[],
        outs_persist=[],
        outs_persist_no_cache=[],
        fname=None,
        cwd=None,
        wdir=None,
        no_exec=False,
        overwrite=False,
        ignore_build_cache=False,
        remove_outs=False,
        no_commit=False,
        always_changed=False,
        cmd="echo foo",
    )


def test_run_args_with_spaces(mocker, dvc):
    args = parse_args(["run", "echo", "foo bar"])
    cmd = args.func(args)
    m = mocker.patch.object(cmd.repo, "run", autospec=True)
    assert cmd.run() == 0
    m.assert_called_once_with(
        deps=[],
        outs=[],
        outs_no_cache=[],
        metrics=[],
        metrics_no_cache=[],
        outs_persist=[],
        outs_persist_no_cache=[],
        fname=None,
        cwd=None,
        wdir=None,
        no_exec=False,
        overwrite=False,
        ignore_build_cache=False,
        remove_outs=False,
        no_commit=False,
        always_changed=False,
        cmd='echo "foo bar"',
    )
