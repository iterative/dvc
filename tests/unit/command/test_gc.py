from dvc.cli import parse_args
from dvc.command.gc import CmdGC


def test_gc(mocker):
    cli_args = parse_args(
        [
            "gc",
            "--remove-all-tags",
            "--remove-all-branches",
            "--remove-all-history",
            "--cloud",
            "--remote",
            "myremote",
            "--force",
            "--jobs",
            "10",
            "--projects",
            "/some/path",
            "/some/other/path",
        ]
    )
    assert cli_args.func == CmdGC

    cmd = cli_args.func(cli_args)
    m = mocker.patch("dvc.repo.Repo.gc")

    assert cmd.run() == 0

    m.assert_called_once_with(
        remove_all_tags=True,
        remove_all_branches=True,
        remove_all_history=True,
        cloud=True,
        remote="myremote",
        force=True,
        jobs=10,
        repos=["/some/path", "/some/other/path"],
    )
