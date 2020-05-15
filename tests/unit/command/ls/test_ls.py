from dvc.cli import parse_args
from dvc.command.ls import CmdList


def _test_cli(mocker, *args):
    cli_args = parse_args(["list", *args])
    assert cli_args.func == CmdList

    cmd = cli_args.func(cli_args)
    m = mocker.patch("dvc.repo.Repo.ls")

    assert cmd.run() == 0
    return m


def test_list(mocker):
    url = "local_dir"
    m = _test_cli(mocker, url)
    m.assert_called_once_with(
        url, None, recursive=False, rev=None, dvc_only=False
    )


def test_list_recursive(mocker):
    url = "local_dir"
    m = _test_cli(mocker, url, "-R")
    m.assert_called_once_with(
        url, None, recursive=True, rev=None, dvc_only=False
    )


def test_list_git_ssh_rev(mocker):
    url = "git@github.com:repo"
    m = _test_cli(mocker, url, "--rev", "123")
    m.assert_called_once_with(
        url, None, recursive=False, rev="123", dvc_only=False
    )


def test_list_targets(mocker):
    url = "local_dir"
    target = "subdir"
    m = _test_cli(mocker, url, target)
    m.assert_called_once_with(
        url, target, recursive=False, rev=None, dvc_only=False
    )


def test_list_outputs_only(mocker):
    url = "local_dir"
    m = _test_cli(mocker, url, None, "--dvc-only")
    m.assert_called_once_with(
        url, None, recursive=False, rev=None, dvc_only=True
    )
