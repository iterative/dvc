from dvc.cli import parse_args
from dvc.command.add import CmdAdd


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
        desc="stage description",
    )
