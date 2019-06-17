from dvc.cli import parse_args
from dvc.command.pkg import CmdPkgInstall, CmdPkgUninstall, CmdPkgImport
from dvc.command.pkg import CmdPkgGet


def test_pkg_install(mocker, dvc_repo):
    args = parse_args(
        ["pkg", "install", "url", "--version", "version", "--name", "name"]
    )
    assert args.func == CmdPkgInstall

    cmd = args.func(args)
    m = mocker.patch.object(cmd.repo.pkg, "install", autospec=True)

    assert cmd.run() == 0

    m.assert_called_once_with("url", version="version", name="name")


def test_pkg_uninstall(mocker, dvc_repo):
    targets = ["target1", "target2"]
    args = parse_args(["pkg", "uninstall"] + targets)
    assert args.func == CmdPkgUninstall

    cmd = args.func(args)
    m = mocker.patch.object(cmd.repo.pkg, "uninstall", autospec=True)

    assert cmd.run() == 0

    calls = []
    for target in targets:
        calls.append(mocker.call(name=target))

    m.assert_has_calls(calls)


def test_pkg_import(mocker, dvc_repo):
    cli_args = parse_args(
        [
            "pkg",
            "import",
            "pkg_name",
            "pkg_path",
            "--out",
            "out",
            "--version",
            "version",
        ]
    )
    assert cli_args.func == CmdPkgImport

    cmd = cli_args.func(cli_args)
    m = mocker.patch.object(cmd.repo.pkg, "imp", autospec=True)

    assert cmd.run() == 0

    m.assert_called_once_with(
        "pkg_name", "pkg_path", out="out", version="version"
    )


def test_pkg_get(mocker, dvc_repo):
    cli_args = parse_args(
        [
            "pkg",
            "get",
            "pkg_url",
            "pkg_path",
            "--out",
            "out",
            "--version",
            "version",
        ]
    )
    assert cli_args.func == CmdPkgGet

    cmd = cli_args.func(cli_args)
    m = mocker.patch("dvc.pkg.PkgManager.get")

    assert cmd.run() == 0

    m.assert_called_once_with(
        "pkg_url", "pkg_path", out="out", version="version"
    )
