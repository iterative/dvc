import json
import os
import posixpath
from pathlib import Path

import pytest
from funcy import pluck_attr

from dvc.cli import parse_args
from dvc.commands.plots import CmdPlotsDiff, CmdPlotsShow, CmdPlotsTemplates


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
        "dvc.render.utils.render", return_value="html_path"
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
        "dvc.render.utils.render", return_value="html_path"
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
        "dvc.render.VegaRenderer.asdict",
        return_value={"this": "is vega json"},
    )
    render_mock = mocker.patch("dvc.render.utils.render")
    assert cmd.run() == 0

    out, _ = capsys.readouterr()

    assert json.dumps({"this": "is vega json"}) in out
    render_mock.assert_not_called()


@pytest.mark.parametrize("auto_open", [True, False])
def test_plots_diff_open(tmp_dir, dvc, mocker, capsys, plots_data, auto_open):
    mocked_open = mocker.patch("webbrowser.open", return_value=True)

    args = ["plots", "diff", "--targets", "plots.csv"]

    if auto_open:
        with dvc.config.edit() as conf:
            conf["plots"]["auto_open"] = True
    else:
        args.append("--open")

    cli_args = parse_args(args)
    cmd = cli_args.func(cli_args)
    mocker.patch("dvc.repo.plots.diff.diff", return_value=plots_data)

    index_path = tmp_dir / "dvc_plots" / "index.html"
    mocker.patch("dvc.render.utils.render", return_value=index_path)

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
    mocker.patch("dvc.render.utils.render", return_value=index_path)

    assert cmd.run() == 0
    mocked_open.assert_called_once_with(str(Path("dvc_plots") / "index.html"))


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

    error_message = (
        f"Failed to open {expected_url.as_uri()}. "
        "Please try opening it manually."
    )

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
    renderers = mocker.MagicMock()
    mocker.patch("dvc.render.utils.match_renderers", return_value=renderers)
    render_mock = mocker.patch(
        "dvc.render.utils.render", return_value=index_path
    )

    assert cmd.run() == 0

    out, _ = capsys.readouterr()
    assert index_path.as_uri() in out

    render_mock.assert_called_once_with(
        cmd.repo, renderers, path=tmp_dir / output, html_template_path=None
    )


def test_plots_diff_json(dvc, mocker, capsys):
    cli_args = parse_args(
        [
            "plots",
            "diff",
            "HEAD~10",
            "HEAD~1",
            "--json",
            "--targets",
            "plot.csv",
            "-o",
            "out",
        ]
    )
    cmd = cli_args.func(cli_args)

    data = mocker.MagicMock()
    mocker.patch("dvc.repo.plots.diff.diff", return_value=data)

    renderers = mocker.MagicMock()
    mocker.patch("dvc.render.utils.match_renderers", return_value=renderers)
    render_mock = mocker.patch("dvc.render.utils.render")

    show_json_mock = mocker.patch("dvc.commands.plots._show_json")

    assert cmd.run() == 0

    show_json_mock.assert_called_once_with(renderers, "out")

    render_mock.assert_not_called()


def test_show_json_requires_out(dvc, mocker, capsys):
    cli_args = parse_args(
        [
            "plots",
            "diff",
            "HEAD~10",
            "HEAD~1",
            "--json",
            "--targets",
            "plot.csv",
        ]
    )
    cmd = cli_args.func(cli_args)

    mocker.patch("dvc.repo.plots.diff.diff")

    renderer = mocker.MagicMock()
    renderer.needs_output_path = False
    renderer.filename = "foo"
    renderer.as_json.return_value = "{}"
    mocker.patch("dvc.render.utils.match_renderers", return_value=[renderer])

    assert cmd.run() == 0

    renderer.needs_output_path = True

    assert cmd.run() == 1

    cli_args = parse_args(
        [
            "plots",
            "diff",
            "HEAD~10",
            "HEAD~1",
            "--json",
            "--targets",
            "plot.csv",
            "-o",
            "out",
        ]
    )
    cmd = cli_args.func(cli_args)
    assert cmd.run() == 0


@pytest.mark.parametrize("target", (("t1"), (None)))
def test_plots_templates(tmp_dir, dvc, mocker, capsys, target):
    assert not os.path.exists(dvc.plots.templates.templates_dir)
    mocker.patch(
        "dvc.commands.plots.CmdPlotsTemplates.TEMPLATES_CHOICES",
        ["t1", "t2"],
    )

    arguments = ["plots", "templates", "--out", "output"]
    if target:
        arguments += [target]

    cli_args = parse_args(arguments)
    assert cli_args.func == CmdPlotsTemplates

    init_mock = mocker.patch("dvc.repo.plots.template.PlotTemplates.init")
    cmd = cli_args.func(cli_args)

    assert cmd.run() == 0
    out, _ = capsys.readouterr()

    init_mock.assert_called_once_with(
        output=os.path.abspath("output"), targets=[target] if target else None
    )
    assert "Templates have been written into 'output'." in out


def test_plots_templates_choices(tmp_dir, dvc):
    from dvc.repo.plots.template import PlotTemplates

    assert CmdPlotsTemplates.TEMPLATES_CHOICES == list(
        pluck_attr("DEFAULT_NAME", PlotTemplates.TEMPLATES)
    )
