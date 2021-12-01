import json

from dvc.compare import TabularData
from dvc.render.html import write
from dvc.render.plotly import ParallelCoordinatesRenderer

# pylint: disable=W1514


def expected_format(result):
    assert "data" in result
    assert "layout" in result
    assert isinstance(result["data"], list)
    assert result["data"][0]["type"] == "parcoords"
    assert isinstance(result["data"][0]["dimensions"], list)
    return True


def test_scalar_columns():
    td = TabularData(["col-1", "col-2", "col-3"])
    td.extend([["0.1", "1", ""], ["2", "0.2", "0"]])
    renderer = ParallelCoordinatesRenderer(td)

    result = json.loads(renderer.as_json())

    assert expected_format(result)

    assert result["data"][0]["dimensions"][0] == {
        "label": "col-1",
        "values": [0.1, 2.0],
    }
    assert result["data"][0]["dimensions"][1] == {
        "label": "col-2",
        "values": [1.0, 0.2],
    }
    assert result["data"][0]["dimensions"][2] == {
        "label": "col-3",
        "values": [None, 0],
    }


def test_categorical_columns():
    td = TabularData(["col-1", "col-2"])
    td.extend([["foo", ""], ["bar", "foobar"], ["foo", ""]])
    renderer = ParallelCoordinatesRenderer(td)

    result = json.loads(renderer.as_json())

    assert expected_format(result)

    assert result["data"][0]["dimensions"][0] == {
        "label": "col-1",
        "values": [1, 0, 1],
        "tickvals": [1, 0, 1],
        "ticktext": ["foo", "bar", "foo"],
    }
    assert result["data"][0]["dimensions"][1] == {
        "label": "col-2",
        "values": [1, 0, 1],
        "tickvals": [1, 0, 1],
        "ticktext": ["Missing", "foobar", "Missing"],
    }


def test_mixed_columns():
    td = TabularData(["categorical", "scalar"])
    td.extend([["foo", "0.1"], ["bar", "2"]])
    renderer = ParallelCoordinatesRenderer(td)

    result = json.loads(renderer.as_json())

    assert expected_format(result)

    assert result["data"][0]["dimensions"][0] == {
        "label": "categorical",
        "values": [1, 0],
        "tickvals": [1, 0],
        "ticktext": ["foo", "bar"],
    }
    assert result["data"][0]["dimensions"][1] == {
        "label": "scalar",
        "values": [0.1, 2.0],
    }


def test_color_by_scalar():
    td = TabularData(["categorical", "scalar"])
    td.extend([["foo", "0.1"], ["bar", "2"]])
    renderer = ParallelCoordinatesRenderer(td, color_by="scalar")

    result = json.loads(renderer.as_json())

    assert expected_format(result)
    assert result["data"][0]["line"] == {
        "color": [0.1, 2.0],
        "showscale": True,
        "colorbar": {"title": "scalar"},
    }


def test_color_by_categorical():
    td = TabularData(["categorical", "scalar"])
    td.extend([["foo", "0.1"], ["bar", "2"]])
    renderer = ParallelCoordinatesRenderer(td, color_by="categorical")

    result = json.loads(renderer.as_json())

    assert expected_format(result)
    assert result["data"][0]["line"] == {
        "color": [1, 0],
        "showscale": True,
        "colorbar": {
            "title": "categorical",
            "tickmode": "array",
            "tickvals": [1, 0],
            "ticktext": ["foo", "bar"],
        },
    }


def test_write_parallel_coordinates(tmp_dir):
    td = TabularData(["categorical", "scalar"])
    td.extend([["foo", "0.1"], ["bar", "2"]])

    renderer = ParallelCoordinatesRenderer(td)
    html_path = write(tmp_dir, renderers=[renderer])

    html_text = html_path.read_text()

    assert ParallelCoordinatesRenderer.SCRIPTS in html_text

    div = ParallelCoordinatesRenderer.DIV.format(
        id="plot_experiments", partial=renderer.as_json()
    )
    assert div in html_text


def test_fill_value():
    td = TabularData(["categorical", "scalar"])
    td.extend([["foo", "-"], ["-", "2"]])
    renderer = ParallelCoordinatesRenderer(td, fill_value="-")

    result = json.loads(renderer.as_json())

    assert expected_format(result)

    assert result["data"][0]["dimensions"][0] == {
        "label": "categorical",
        "values": [0, 1],
        "tickvals": [0, 1],
        "ticktext": ["foo", "Missing"],
    }
    assert result["data"][0]["dimensions"][1] == {
        "label": "scalar",
        "values": [None, 2.0],
    }
