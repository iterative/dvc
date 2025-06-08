import json
import os
import posixpath
from pathlib import Path

import pytest
from funcy import set_in

from dvc.cli import parse_args
from dvc.commands.plots import CmdPlotsDiff, CmdPlotsShow, CmdPlotsTemplates
from dvc.render.match import RendererWithErrors
from dvc.utils.serialize import YAMLFileCorruptedError


@pytest.fixture
def plots_data():
    return {
        "revision": {
            "sources": {
                "data": {
                    "plot.csv": {"data": [{"val": 1}, {"val": 2}], "props": {}},
                    "other.jpg": {"data": b"content"},
                }
            },
            "definitions": {"data": {"dvc.yaml": {"data": {"plot.csv": {}}}}},
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
    render_mock = mocker.patch("dvc_render.render_html", return_value="html_path")

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
    render_mock = mocker.patch("dvc_render.render_html", return_value="html_path")

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
        "dvc_render.VegaRenderer.get_filled_template",
        return_value={"this": "is vega json"},
    )
    render_mock = mocker.patch("dvc_render.render_html")
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
    mocker.patch("dvc_render.render_html", return_value=index_path)

    assert cmd.run() == 0
    mocked_open.assert_called_once_with(index_path.as_uri())

    out, _ = capsys.readouterr()
    assert index_path.as_uri() in out


def test_plots_diff_open_wsl(tmp_dir, dvc, mocker, plots_data):
    mocked_open = mocker.patch("webbrowser.open", return_value=True)
    mocked_uname_result = mocker.MagicMock()
    mocked_uname_result.release = "microsoft"
    mocker.patch("platform.uname", return_value=mocked_uname_result)

    cli_args = parse_args(["plots", "diff", "--targets", "plots.csv", "--open"])
    cmd = cli_args.func(cli_args)
    mocker.patch("dvc.repo.plots.diff.diff", return_value=plots_data)

    index_path = tmp_dir / "dvc_plots" / "index.html"
    mocker.patch("dvc_render.render_html", return_value=index_path)

    assert cmd.run() == 0
    mocked_open.assert_called_once_with(str(Path("dvc_plots") / "index.html"))


def test_plots_diff_open_failed(tmp_dir, dvc, mocker, capsys, plots_data):
    mocked_open = mocker.patch("webbrowser.open", return_value=False)
    cli_args = parse_args(["plots", "diff", "--targets", "plots.csv", "--open"])
    cmd = cli_args.func(cli_args)
    mocker.patch("dvc.repo.plots.diff.diff", return_value=plots_data)

    assert cmd.run() == 1
    expected_url = tmp_dir / "dvc_plots" / "index.html"
    mocked_open.assert_called_once_with(expected_url.as_uri())

    error_message = (
        f"Failed to open {expected_url.as_uri()}. Please try opening it manually."
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
    cli_args = parse_args(["plots", "diff", "--targets", "datafile", "--out", output])
    cmd = cli_args.func(cli_args)
    mocker.patch("dvc.repo.plots.diff.diff", return_value=plots_data)

    assert cmd.run() == 0
    expected_url = posixpath.join(tmp_dir.as_uri(), expected_url_path)

    out, _ = capsys.readouterr()
    assert expected_url in out


def test_should_pass_template_dir(tmp_dir, dvc, mocker, capsys):
    cli_args = parse_args(
        [
            "plots",
            "diff",
            "HEAD~1",
            "--json",
            "--targets",
            "plot.csv",
        ]
    )
    cmd = cli_args.func(cli_args)

    data = mocker.MagicMock()
    mocker.patch("dvc.repo.plots.diff.diff", return_value=data)

    renderers = mocker.MagicMock()
    match_renderers = mocker.patch(
        "dvc.render.match.match_defs_renderers", return_value=renderers
    )

    assert cmd.run() == 0

    match_renderers.assert_called_once_with(
        data=data,
        out="dvc_plots",
        templates_dir=str(tmp_dir / ".dvc/plots"),
    )


@pytest.mark.parametrize("output", ("some_out", os.path.join("to", "subdir"), None))
def test_should_call_render(tmp_dir, mocker, capsys, plots_data, output):
    cli_args = parse_args(["plots", "diff", "--targets", "plots.csv", "--out", output])
    cmd = cli_args.func(cli_args)
    mocker.patch("dvc.repo.plots.diff.diff", return_value=plots_data)

    output = output or "dvc_plots"
    index_path = tmp_dir / output / "index.html"
    renderer = mocker.MagicMock()
    mocker.patch(
        "dvc.render.match.match_defs_renderers",
        return_value=[RendererWithErrors(renderer, {}, {})],
    )
    render_mock = mocker.patch("dvc_render.render_html", return_value=index_path)

    assert cmd.run() == 0

    out, _ = capsys.readouterr()
    assert index_path.as_uri() in out

    render_mock.assert_called_once_with(
        renderers=[renderer],
        output_file=Path(tmp_dir / output / "index.html"),
        html_template=None,
    )


def test_plots_diff_json(dvc, mocker, capsys):
    cli_args = parse_args(
        [
            "plots",
            "diff",
            "HEAD~10",
            "HEAD~1",
            "--json",
            "--split",
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
    mocker.patch("dvc.render.match.match_defs_renderers", return_value=renderers)
    render_mock = mocker.patch("dvc_render.render_html")

    show_json_mock = mocker.patch("dvc.commands.plots._show_json")

    assert cmd.run() == 0

    show_json_mock.assert_called_once_with(renderers, True, errors={})

    render_mock.assert_not_called()


@pytest.mark.parametrize(
    "target,expected_out,expected_rtn",
    (("t1", "\"{'t1'}\"", 0), (None, "t1\nt2", 0), ("t3", "", 1)),
)
def test_plots_templates(dvc, mocker, capsys, target, expected_out, expected_rtn):
    t1 = mocker.Mock()
    t1.DEFAULT_NAME = "t1"
    t1.DEFAULT_CONTENT = "{'t1'}"

    t2 = mocker.Mock()
    t2.DEFAULT_NAME = "t2"
    t2.DEFAULT_CONTENT = "{'t2'}"

    mocker.patch("dvc_render.vega_templates.TEMPLATES", [t1, t2])

    arguments = ["plots", "templates"]
    if target:
        arguments += [target]

    cli_args = parse_args(arguments)
    assert cli_args.func == CmdPlotsTemplates

    cmd = cli_args.func(cli_args)

    rtn = cmd.run()

    out, _ = capsys.readouterr()

    assert out.strip() == expected_out
    assert rtn == expected_rtn


@pytest.mark.parametrize("split", (True, False))
def test_show_json(split, mocker, capsys):
    import dvc.commands.plots

    renderer = mocker.MagicMock()
    renderer_obj = RendererWithErrors(renderer, {}, {})
    renderer.name = "rname"
    to_json_mock = mocker.patch(
        "dvc.render.convert.to_json", return_value={"renderer": "json"}
    )

    dvc.commands.plots._show_json([renderer_obj], split)

    to_json_mock.assert_called_once_with(renderer, split)

    out, _ = capsys.readouterr()
    assert json.dumps({"rname": {"renderer": "json"}}) in out


def test_show_json_no_renderers(capsys):
    import dvc.commands.plots

    dvc.commands.plots._show_json([])

    out, _ = capsys.readouterr()
    assert json.dumps({}) in out


def test_show_json_with_error(dvc, mocker, capsys):
    cli_args = parse_args(["plots", "show", "--json"])
    cmd = cli_args.func(cli_args)

    e = YAMLFileCorruptedError("dvc.yaml")
    data = set_in({}, ["workspace", "definitions", "error"], e)
    cmd._func = mocker.MagicMock(return_value=data)

    cmd.run()
    out, _ = capsys.readouterr()
    assert json.loads(out) == {
        "errors": [
            {
                "rev": "workspace",
                "type": type(e).__name__,
                "msg": e.args[0],
            }
        ]
    }
