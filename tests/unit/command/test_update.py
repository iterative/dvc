import logging

from dvc.cli import parse_args
from dvc.command.update import CmdUpdate
from dvc.exceptions import UpdateWithRevNotPossibleError


def test_update(dvc_repo, mocker):
    targets = ["target1", "target2", "target3"]
    cli_args = parse_args(["update", "--rev", "develop"] + targets)
    assert cli_args.func == CmdUpdate
    cmd = cli_args.func(cli_args)
    m = mocker.patch("dvc.repo.Repo.update")

    assert cmd.run() == 0

    calls = [mocker.call(target, rev="develop") for target in targets]
    m.assert_has_calls(calls)


def test_update_rev_failed(mocker, caplog, dvc_repo):
    targets = ["target1", "target2", "target3"]
    cli_args = parse_args(["update", "--rev", "develop"] + targets)
    assert cli_args.func == CmdUpdate

    cmd = cli_args.func(cli_args)
    with mocker.patch.object(
        cmd.repo, "update", side_effect=UpdateWithRevNotPossibleError()
    ):
        with caplog.at_level(logging.ERROR, logger="dvc"):
            assert cmd.run() == 1
            expected_error = (
                "Revision option (--rev) is not a feature of non-Git sources."
            )
            assert expected_error in caplog.text
