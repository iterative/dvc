from dvc.render import REVISION_FIELD
from dvc.render.match import PlotsData, _squash_plots_properties, match_defs_renderers


def test_group_definitions():
    error = FileNotFoundError()
    data = {
        "v1": {
            "definitions": {
                "data": {
                    "config_file_1": {"data": {"plot_id_1": {}, "plot_id_2": {}}},
                    "config_file_2": {"data": {"plot_id_3": {}}},
                }
            }
        },
        "v2": {
            "definitions": {
                "data": {
                    "config_file_1": {"error": error},
                    "config_file_2": {"data": {"plot_id_3": {}}},
                }
            }
        },
    }

    grouped = PlotsData(data).group_definitions()

    assert grouped == {
        "config_file_1::plot_id_1": [("v1", "plot_id_1", {})],
        "config_file_1::plot_id_2": [("v1", "plot_id_2", {})],
        "config_file_2::plot_id_3": [
            ("v1", "plot_id_3", {}),
            ("v2", "plot_id_3", {}),
        ],
    }


def test_match_renderers(mocker):
    data = {
        "v1": {
            "definitions": {
                "data": {
                    "config_file_1": {
                        "data": {
                            "plot_id_1": {
                                "x": "x",
                                "y": {"file.json": "y"},
                            }
                        }
                    }
                },
            },
            "sources": {
                "data": {"file.json": {"data": [{"x": 1, "y": 1}, {"x": 2, "y": 2}]}}
            },
        },
        "errored_revision": {
            "definitions": {
                "data": {"config_file_1": {"error": FileNotFoundError()}},
            },
            "sources": {},
        },
        "revision_with_no_data": {
            "definitions": {
                "data": {
                    "config_file_1": {
                        "data": {
                            "plot_id_1": {
                                "x": "x",
                                "y": {"file.json": "y"},
                            }
                        }
                    }
                },
            },
            "sources": {"data": {"file.json": {"error": FileNotFoundError()}}},
        },
    }

    renderers = match_defs_renderers(data)
    assert len(renderers) == 1
    assert renderers[0].datapoints == [
        {
            "x": 1,
            "y": 1,
            REVISION_FIELD: "v1::file.json::y",
            "dvc_id": "v1::file.json::y",
            "dvc_rev": "v1",
            "dvc_filename": "file.json",
            "dvc_field": "y",
        },
        {
            "x": 2,
            "y": 2,
            REVISION_FIELD: "v1::file.json::y",
            "dvc_id": "v1::file.json::y",
            "dvc_rev": "v1",
            "dvc_filename": "file.json",
            "dvc_field": "y",
        },
    ]
    assert renderers[0].properties == {
        "title": "config_file_1::plot_id_1",
        "x": "x",
        "y": "y",
        "x_label": "x",
        "y_label": "y",
    }


def test_squash_plots_properties():
    group = [
        ("v3", "config_file", "plot_id", {"foo": 1}),
        ("v2", "config_file", "plot_id", {"foo": 2, "bar": 2}),
        ("v1", "config_file", "plot_id", {"baz": 3}),
    ]

    plot_properties = _squash_plots_properties(group)

    assert plot_properties == {"foo": 1, "bar": 2, "baz": 3}
