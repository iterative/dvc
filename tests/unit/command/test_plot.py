import pytest

from dvc.cli import parse_args
from dvc.command.plot import CmdPlotShow, CmdPlotDiff


def test_metrics_diff(mocker):
    cli_args = parse_args(
        [
            "plot",
            "diff",
            "-r",
            "result.extension",
            "-t",
            "template",
            "-d",
            "datafile",
            "--field",
            "column1,column2",
            "--no-html",
            "--stdout",
            "HEAD",
            "tag1",
            "tag2",
        ]
    )
    assert cli_args.func == CmdPlotDiff

    cmd = cli_args.func(cli_args)

    m = mocker.patch.object(cmd.repo, "plot", autospec=True)
    mocker.patch("builtins.open")
    mocker.patch("os.path.join")

    assert cmd.run() == 0

    m.assert_called_once_with(
        datafile="datafile",
        template="template",
        revisions=["HEAD", "tag1", "tag2"],
        fields={"column1", "column2"},
        path=None,
        embed=False,
        csv_header=True,
    )


def test_metrics_show(mocker):
    cli_args = parse_args(
        [
            "plot",
            "show",
            "-r",
            "result.extension",
            "-t",
            "template",
            "-f",
            "$.data",
            "--no-html",
            "--stdout",
            "--no-csv-header",
            "datafile",
        ]
    )
    assert cli_args.func == CmdPlotShow

    cmd = cli_args.func(cli_args)

    m = mocker.patch.object(cmd.repo, "plot", autospec=True)
    mocker.patch("builtins.open")
    mocker.patch("os.path.join")

    assert cmd.run() == 0

    m.assert_called_once_with(
        datafile="datafile",
        template="template",
        revisions=None,
        fields=None,
        path="$.data",
        embed=False,
        csv_header=False,
    )


@pytest.mark.parametrize(
    "arg_revisions,is_dirty,expected_revisions",
    [
        ([], False, ["workspace"]),
        ([], True, ["HEAD", "workspace"]),
        (["v1", "v2", "workspace"], False, ["v1", "v2", "workspace"]),
        (["v1", "v2", "workspace"], True, ["v1", "v2", "workspace"]),
    ],
)
def test_revisions(mocker, arg_revisions, is_dirty, expected_revisions):
    args = mocker.MagicMock()

    cmd = CmdPlotDiff(args)
    mocker.patch.object(args, "revisions", arg_revisions)
    mocker.patch.object(cmd.repo.scm, "is_dirty", return_value=is_dirty)

    assert cmd._revisions() == expected_revisions
