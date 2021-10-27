import os
import posixpath
from pathlib import Path

import pytest

from dvc.cli import parse_args
from dvc.command.plots import CmdPlotsDiff, CmdPlotsShow


@pytest.fixture
def plots_data():
    yield {
        "revision": {
            "data": {
                "plot.csv": {"data": [{"val": 1}, {"val": 2}], "props": {}},
                "other.jpg": {"data": b"content"},
            }
        }
    }


def test_plots_diff(dvc, mocker, plots_data):
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
            "--show-vega",
            "-x",
            "x_field",
            "-y",
            "y_field",
            "--title",
            "my_title",
            "--x-label",
            "x_title",
            "--y-label",
            "y_title",
            "--experiment",
            "HEAD",
            "tag1",
            "tag2",
        ]
    )
    assert cli_args.func == CmdPlotsDiff

    cmd = cli_args.func(cli_args)
    m = mocker.patch("dvc.repo.plots.diff.diff", return_value=plots_data)
    render_mock = mocker.patch(
        "dvc.command.plots.render", return_value="html_path"
    )

    assert cmd.run() == 0

    m.assert_called_once_with(
        cmd.repo,
        targets=["datafile"],
        revs=["HEAD", "tag1", "tag2"],
        props={
            "template": "template",
            "x": "x_field",
            "y": "y_field",
            "title": "my_title",
            "x_label": "x_title",
            "y_label": "y_title",
        },
        experiment=True,
    )
    render_mock.assert_not_called()


def test_plots_show_vega(dvc, mocker, plots_data):
    cli_args = parse_args(
        [
            "plots",
            "show",
            "-o",
            "result.extension",
            "-t",
            "template",
            "--show-vega",
            "--no-header",
            "datafile",
        ]
    )
    assert cli_args.func == CmdPlotsShow

    cmd = cli_args.func(cli_args)

    m = mocker.patch(
        "dvc.repo.plots.Plots.show",
        return_value=plots_data,
    )
    render_mock = mocker.patch(
        "dvc.command.plots.render", return_value="html_path"
    )

    assert cmd.run() == 0

    m.assert_called_once_with(
        targets=["datafile"],
        props={"template": "template", "header": False},
    )
    render_mock.assert_not_called()


def test_plots_diff_vega(dvc, mocker, capsys, plots_data):
    cli_args = parse_args(
        [
            "plots",
            "diff",
            "HEAD~10",
            "HEAD~1",
            "--show-vega",
            "--targets",
            "plot.csv",
        ]
    )
    cmd = cli_args.func(cli_args)
    mocker.patch("dvc.repo.plots.diff.diff", return_value=plots_data)
    mocker.patch(
        "dvc.command.plots.find_vega", return_value="vega_json_content"
    )
    render_mock = mocker.patch(
        "dvc.command.plots.render", return_value="html_path"
    )
    assert cmd.run() == 0

    out, _ = capsys.readouterr()

    assert "vega_json_content" in out
    render_mock.assert_not_called()


def test_plots_diff_open(tmp_dir, dvc, mocker, capsys, plots_data):
    mocked_open = mocker.patch("webbrowser.open", return_value=True)
    cli_args = parse_args(
        ["plots", "diff", "--targets", "plots.csv", "--open"]
    )
    cmd = cli_args.func(cli_args)
    mocker.patch("dvc.repo.plots.diff.diff", return_value=plots_data)

    index_path = tmp_dir / "dvc_plots" / "index.html"
    mocker.patch("dvc.command.plots.render", return_value=index_path)

    assert cmd.run() == 0
    mocked_open.assert_called_once_with(index_path.as_uri())

    out, _ = capsys.readouterr()
    assert index_path.as_uri() in out


def test_plots_diff_open_WSL(tmp_dir, dvc, mocker, plots_data):
    mocked_open = mocker.patch("webbrowser.open", return_value=True)
    mocked_uname_result = mocker.MagicMock()
    mocked_uname_result.release = "Microsoft"
    mocker.patch("platform.uname", return_value=mocked_uname_result)

    cli_args = parse_args(
        ["plots", "diff", "--targets", "plots.csv", "--open"]
    )
    cmd = cli_args.func(cli_args)
    mocker.patch("dvc.repo.plots.diff.diff", return_value=plots_data)

    index_path = tmp_dir / "dvc_plots" / "index.html"
    mocker.patch("dvc.command.plots.render", return_value=index_path)

    assert cmd.run() == 0
    mocked_open.assert_called_once_with(Path("dvc_plots") / "index.html")


def test_plots_diff_open_failed(tmp_dir, dvc, mocker, capsys, plots_data):
    mocked_open = mocker.patch("webbrowser.open", return_value=False)
    cli_args = parse_args(
        ["plots", "diff", "--targets", "plots.csv", "--open"]
    )
    cmd = cli_args.func(cli_args)
    mocker.patch(
        "dvc.repo.plots.diff.diff", return_value={"datafile": plots_data}
    )

    assert cmd.run() == 1
    expected_url = tmp_dir / "dvc_plots" / "index.html"
    mocked_open.assert_called_once_with(expected_url.as_uri())

    error_message = "Failed to open. Please try opening it manually."

    out, err = capsys.readouterr()
    assert expected_url.as_uri() in out
    assert error_message in err


@pytest.mark.parametrize(
    "output, expected_url_path",
    [
        (
            "plots file with spaces",
            posixpath.join("plots%20file%20with%20spaces", "index.html"),
        ),
        (
            os.path.join("dir", "..", "plots"),
            posixpath.join("plots", "index.html"),
        ),
    ],
    ids=["quote", "resolve"],
)
def test_plots_path_is_quoted_and_resolved_properly(
    tmp_dir, dvc, mocker, capsys, output, expected_url_path, plots_data
):
    cli_args = parse_args(
        ["plots", "diff", "--targets", "datafile", "--out", output]
    )
    cmd = cli_args.func(cli_args)
    mocker.patch(
        "dvc.repo.plots.diff.diff", return_value={"datafile": plots_data}
    )

    assert cmd.run() == 0
    expected_url = posixpath.join(tmp_dir.as_uri(), expected_url_path)

    out, _ = capsys.readouterr()
    assert expected_url in out


@pytest.mark.parametrize(
    "output", ("some_out", os.path.join("to", "subdir"), None)
)
def test_should_call_render(tmp_dir, mocker, capsys, plots_data, output):
    cli_args = parse_args(
        ["plots", "diff", "--targets", "plots.csv", "--out", output]
    )
    cmd = cli_args.func(cli_args)
    mocker.patch("dvc.repo.plots.diff.diff", return_value=plots_data)

    output = output or "dvc_plots"
    index_path = tmp_dir / output / "index.html"
    render_mock = mocker.patch(
        "dvc.command.plots.render", return_value=index_path
    )

    assert cmd.run() == 0

    out, _ = capsys.readouterr()
    assert index_path.as_uri() in out

    render_mock.assert_called_once_with(
        cmd.repo, plots_data, path=tmp_dir / output, html_template_path=None
    )
