from dvc.cli import parse_args
from dvc.command.imp_url import CmdImportUrl


def test_import_url(mocker, dvc_repo):
    cli_args = parse_args(
        ["import-url", "src", "out", "--resume", "--file", "file"]
    )
    assert cli_args.func == CmdImportUrl

    cmd = cli_args.func(cli_args)
    m = mocker.patch.object(cmd.repo, "imp_url", autospec=True)

    assert cmd.run() == 0

    m.assert_called_once_with("src", out="out", resume=True, fname="file")
