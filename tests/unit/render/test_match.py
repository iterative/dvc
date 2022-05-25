from dvc.render.match import (
    group_by_filename,
    match_renderers,
    squash_plots_properties,
)


def test_group_by_filename():
    error = FileNotFoundError()
    data = {
        "v2": {
            "data": {
                "file.json": {"data": [{"y": 2}, {"y": 3}], "props": {}},
                "other_file.jpg": {"data": "content"},
            }
        },
        "v1": {
            "data": {"file.json": {"data": [{"y": 4}, {"y": 5}], "props": {}}}
        },
        "workspace": {
            "data": {
                "file.json": {"error": error, "props": {}},
                "other_file.jpg": {"data": "content2"},
            }
        },
    }

    results = group_by_filename(data)
    assert results["file.json"] == {
        "v2": {
            "data": {
                "file.json": {"data": [{"y": 2}, {"y": 3}], "props": {}},
            }
        },
        "v1": {
            "data": {"file.json": {"data": [{"y": 4}, {"y": 5}], "props": {}}}
        },
        "workspace": {
            "data": {
                "file.json": {"error": error, "props": {}},
            }
        },
    }
    assert results["other_file.jpg"] == {
        "v2": {
            "data": {
                "other_file.jpg": {"data": "content"},
            }
        },
        "workspace": {
            "data": {
                "other_file.jpg": {"data": "content2"},
            }
        },
    }


def test_squash_plots_properties():
    error = FileNotFoundError()
    group = {
        "v2": {
            "data": {
                "file.json": {
                    "data": [{"y": 2}, {"y": 3}],
                    "props": {"foo": 1},
                },
            }
        },
        "v1": {
            "data": {
                "file.json": {
                    "data": [{"y": 4}, {"y": 5}],
                    "props": {"bar": 1},
                }
            }
        },
        "workspace": {
            "data": {
                "file.json": {"error": error, "props": {}},
            }
        },
    }

    plot_properties = squash_plots_properties(group)

    assert plot_properties == {"foo": 1, "bar": 1}


def test_match_renderers_no_out(mocker):
    from dvc import render

    vega_convert = mocker.spy(render.vega_converter.VegaConverter, "convert")
    image_convert = mocker.spy(
        render.image_converter.ImageConverter, "convert"
    )
    image_encode = mocker.spy(
        render.image_converter.ImageConverter, "_encode_image"
    )
    image_write = mocker.spy(
        render.image_converter.ImageConverter, "_write_image"
    )

    error = FileNotFoundError()
    data = {
        "v2": {
            "data": {
                "file.json": {"data": [{"y": 2}, {"y": 3}], "props": {}},
                "other_file.jpg": {"data": b"content"},
            }
        },
        "v1": {
            "data": {"file.json": {"data": [{"y": 4}, {"y": 5}], "props": {}}}
        },
        "workspace": {
            "data": {
                "file.json": {"error": error, "props": {}},
                "other_file.jpg": {"data": b"content2"},
            }
        },
    }

    renderers = match_renderers(data)

    assert {r.TYPE for r in renderers} == {"vega", "image"}
    vega_convert.assert_called()
    image_convert.assert_called()
    image_encode.assert_called()
    image_write.assert_not_called()


def test_match_renderers_with_out(tmp_dir, mocker):
    from dvc import render

    image_encode = mocker.spy(
        render.image_converter.ImageConverter, "_encode_image"
    )
    image_write = mocker.spy(
        render.image_converter.ImageConverter, "_write_image"
    )

    error = FileNotFoundError()
    data = {
        "v2": {
            "data": {
                "file.json": {"data": [{"y": 2}, {"y": 3}], "props": {}},
                "other_file.jpg": {"data": b"content"},
            }
        },
        "v1": {
            "data": {"file.json": {"data": [{"y": 4}, {"y": 5}], "props": {}}}
        },
        "workspace": {
            "data": {
                "file.json": {"error": error, "props": {}},
                "other_file.jpg": {"data": b"content2"},
            }
        },
    }

    match_renderers(data, out=tmp_dir / "foo")

    image_encode.assert_not_called()
    image_write.assert_called()

    assert (tmp_dir / "foo" / "v2_other_file.jpg").read_bytes() == b"content"
    assert (
        tmp_dir / "foo" / "workspace_other_file.jpg"
    ).read_bytes() == b"content2"


def test_match_renderers_template_dir(mocker):
    from dvc_render import vega

    vega_render = mocker.spy(vega.VegaRenderer, "__init__")
    data = {
        "v1": {
            "data": {"file.json": {"data": [{"y": 4}, {"y": 5}], "props": {}}}
        },
    }

    match_renderers(data, templates_dir="foo")

    assert vega_render.call_args[1]["template_dir"] == "foo"
