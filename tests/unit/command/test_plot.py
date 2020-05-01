import pytest

from dvc.cli import parse_args
from dvc.command.plot import CmdPlotShow, CmdPlotDiff


def test_metrics_diff(mocker):
    cli_args = parse_args(
        [
            "plot",
            "diff",
            "--file",
            "result.extension",
            "-t",
            "template",
            "-d",
            "datafile",
            "--select",
            "column1,column2",
            "--no-html",
            "--stdout",
            "-x",
            "x_field",
            "-y",
            "y_field",
            "--title",
            "my_title",
            "--xlab",
            "x_title",
            "--ylab",
            "y_title",
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
        x_field="x_field",
        y_field="y_field",
        csv_header=True,
        title="my_title",
        x_title="x_title",
        y_title="y_title",
    )


def test_metrics_show(mocker):
    cli_args = parse_args(
        [
            "plot",
            "show",
            "-f",
            "result.extension",
            "-t",
            "template",
            "-s",
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
        x_field=None,
        y_field=None,
        csv_header=False,
        title=None,
        x_title=None,
        y_title=None,
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
