from dvc.cli import parse_args
from dvc.command.plots import CmdPlotsDiff, CmdPlotsShow


def test_metrics_diff(dvc, mocker):
    cli_args = parse_args(
        [
            "plots",
            "diff",
            "--out",
            "result.extension",
            "-t",
            "template",
            "--targets",
            "datafile",
            "--select",
            "column1,column2",
            "--show-json",
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
    assert cli_args.func == CmdPlotsDiff

    cmd = cli_args.func(cli_args)
    m = mocker.patch("dvc.repo.plots.diff.diff", return_value={})

    assert cmd.run() == 0

    m.assert_called_once_with(
        cmd.repo,
        targets=["datafile"],
        template="template",
        revs=["HEAD", "tag1", "tag2"],
        fields={"column1", "column2"},
        path=None,
        x_field="x_field",
        y_field="y_field",
        csv_header=True,
        title="my_title",
        x_title="x_title",
        y_title="y_title",
    )


def test_metrics_show(dvc, mocker):
    cli_args = parse_args(
        [
            "plots",
            "show",
            "-o",
            "result.extension",
            "-t",
            "template",
            "-s",
            "$.data",
            "--show-json",
            "--no-csv-header",
            "datafile",
        ]
    )
    assert cli_args.func == CmdPlotsShow

    cmd = cli_args.func(cli_args)

    m = mocker.patch("dvc.repo.plots.show.show", return_value={})

    assert cmd.run() == 0

    m.assert_called_once_with(
        cmd.repo,
        targets=["datafile"],
        template="template",
        fields=None,
        path="$.data",
        x_field=None,
        y_field=None,
        csv_header=False,
        title=None,
        x_title=None,
        y_title=None,
    )


def test_plots_show_json(dvc, mocker, caplog):
    cli_args = parse_args(
        ["plots", "diff", "HEAD~10", "HEAD~1", "--show-json"]
    )
    cmd = cli_args.func(cli_args)
    mocker.patch(
        "dvc.repo.plots.diff.diff", return_value={"plots.csv": "plothtml"}
    )
    assert cmd.run() == 0
    assert '{"plots.csv": "plothtml"}\n' in caplog.text
