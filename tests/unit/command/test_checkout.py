from dvc.cli import parse_args
from dvc.command.checkout import CmdCheckout


def test_checkout(tmp_dir, dvc, mocker):
    cli_args = parse_args(
        ["checkout", "foo.dvc", "bar.dvc", "--relink", "--with-deps"]
    )
    assert cli_args.func == CmdCheckout

    cmd = cli_args.func(cli_args)
    m = mocker.patch("dvc.repo.Repo.checkout")

    assert cmd.run() == 0
    m.assert_called_once_with(
        targets=["foo.dvc", "bar.dvc"],
        force=False,
        recursive=False,
        relink=True,
        with_deps=True,
    )
