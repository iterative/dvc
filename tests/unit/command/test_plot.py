from dvc.cli import parse_args
from dvc.command.plot import CmdPlotShow, CmdPlotDiff


def test_metrics_diff(dvc, mocker):
    cli_args = parse_args(
        [
            "plot",
            "diff",
            "-f",
            "result.extension",
            "-t",
            "template",
            "-d",
            "datafile",
            "HEAD",
            "tag1",
            "tag2",
        ]
    )
    assert cli_args.func == CmdPlotDiff

    cmd = cli_args.func(cli_args)

    m = mocker.patch.object(cmd.repo, "plot", autospec=True)
    mocker.patch("os.path.join")

    assert cmd.run() == 0

    m.assert_called_once_with(
        datafile="datafile",
        template="template",
        revisions=["HEAD", "tag1", "tag2"],
        file="result.extension",
    )


def test_metrics_show(dvc, mocker):
    cli_args = parse_args(
        [
            "plot",
            "show",
            "-f",
            "result.extension",
            "-t",
            "template",
            "datafile",
        ]
    )
    assert cli_args.func == CmdPlotShow

    cmd = cli_args.func(cli_args)

    m = mocker.patch.object(cmd.repo, "plot", autospec=True)
    mocker.patch("os.path.join")

    assert cmd.run() == 0

    m.assert_called_once_with(
        datafile="datafile",
        template="template",
        file="result.extension",
        revisions=None,
    )
