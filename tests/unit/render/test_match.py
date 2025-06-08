import pytest
from funcy import set_in

from dvc.render import FIELD, FILENAME, REVISION
from dvc.render.converter.vega import VegaConverter
from dvc.render.match import PlotsData, _squash_plots_properties, match_defs_renderers


@pytest.mark.parametrize(
    "data,expected",
    [
        pytest.param(
            {
                "v1": {
                    "definitions": {
                        "data": {"config_file_1": {"data": {"plot_id_1": {}}}}
                    }
                }
            },
            {"plot_id_1": [("v1", "plot_id_1", {})]},
            id="simple",
        ),
        pytest.param(
            {
                "v1": {
                    "definitions": {
                        "data": {
                            "config_file_1": {"data": {"plot_id_1": {}}},
                            "config_file_2": {"data": {"plot_id_1": {}}},
                        }
                    }
                }
            },
            {
                "config_file_1::plot_id_1": [("v1", "plot_id_1", {})],
                "config_file_2::plot_id_1": [("v1", "plot_id_1", {})],
            },
            id="multi_config",
        ),
        pytest.param(
            {
                "v1": {
                    "definitions": {
                        "data": {"config_file_1": {"data": {"plot_id_1": {}}}}
                    }
                },
                "v2": {
                    "definitions": {
                        "data": {"config_file_2": {"data": {"plot_id_1": {}}}}
                    }
                },
            },
            {"plot_id_1": [("v1", "plot_id_1", {}), ("v2", "plot_id_1", {})]},
            id="multi_rev",
        ),
        pytest.param(
            {
                "v1": {
                    "definitions": {
                        "data": {
                            "config_file_1": {"data": {"plot_id_1": {}}},
                            "config_file_2": {"data": {"plot_id_1": {}}},
                        }
                    }
                },
                "v2": {
                    "definitions": {
                        "data": {"config_file_1": {"data": {"plot_id_1": {}}}}
                    }
                },
            },
            {
                "config_file_1::plot_id_1": [("v1", "plot_id_1", {})],
                "config_file_2::plot_id_1": [("v1", "plot_id_1", {})],
                "plot_id_1": [("v2", "plot_id_1", {})],
            },
            id="multi_rev_multi_config",
        ),
        pytest.param(
            {
                "v1": {
                    "definitions": {
                        "data": {
                            "config_file_1": {
                                "data": {"plot_id_1": {}, "plot_id_2": {}}
                            },
                            "config_file_2": {"data": {"plot_id_3": {}}},
                        }
                    }
                },
                "v2": {
                    "definitions": {
                        "data": {
                            "config_file_2": {"data": {"plot_id_3": {}}},
                        }
                    },
                    "source": {
                        "data": {
                            "config_file_1": {"error": FileNotFoundError()},
                        }
                    },
                },
            },
            {
                "plot_id_1": [("v1", "plot_id_1", {})],
                "plot_id_2": [("v1", "plot_id_2", {})],
                "plot_id_3": [
                    ("v1", "plot_id_3", {}),
                    ("v2", "plot_id_3", {}),
                ],
            },
            id="all",
        ),
    ],
)
def test_group_definitions(data, expected):
    grouped = PlotsData(data).group_definitions()
    assert grouped == expected


def test_match_renderers(M):
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

    (renderer_with_errors,) = match_defs_renderers(data)
    renderer = renderer_with_errors[0]
    assert renderer.datapoints == [
        {
            REVISION: "v1",
            FILENAME: "file.json",
            FIELD: "y",
            "x": 1,
            "y": 1,
        },
        {
            REVISION: "v1",
            FILENAME: "file.json",
            FIELD: "y",
            "x": 2,
            "y": 2,
        },
    ]
    assert renderer.properties == {
        "anchors_y_definitions": [{FILENAME: "file.json", FIELD: "y"}],
        "revs_with_datapoints": ["v1"],
        "title": "plot_id_1",
        "x": "x",
        "y": "y",
        "x_label": "x",
        "y_label": "y",
    }
    assert renderer_with_errors.source_errors == {
        "revision_with_no_data": {"file.json": M.instance_of(FileNotFoundError)}
    }
    assert not renderer_with_errors.definition_errors


def test_flat_datapoints_errors_are_caught(M, mocker):
    d = {}
    d = set_in(
        d,
        ["v1", "definitions", "data", "dvc.yaml", "data", "plot_id_1"],
        {"x": "x", "y": {"file.json": "y"}},
    )
    d = set_in(d, ["v1", "sources", "data", "file.json", "data"], [{"x": 1, "y": 1}])
    mocker.patch.object(VegaConverter, "flat_datapoints", side_effect=ValueError)
    (renderer_with_errors,) = match_defs_renderers(d)
    assert not renderer_with_errors.source_errors
    assert renderer_with_errors.definition_errors == {"v1": M.instance_of(ValueError)}


def test_squash_plots_properties_revs():
    group = [
        ("v3", "config_file", "plot_id", {"foo": 1}),
        ("v2", "config_file", "plot_id", {"foo": 2, "bar": 2}),
        ("v1", "config_file", "plot_id", {"baz": 3}),
    ]

    plot_properties = _squash_plots_properties(group)

    assert plot_properties == {"foo": 1, "bar": 2, "baz": 3}


def test_squash_plots_properties_config_files():
    group = [
        ("v1", "config_file1", "plot_id", {"foo": 1}),
        ("v1", "config_file2", "plot_id", {"foo": 2, "bar": 2}),
        ("v1", "config_file3", "plot_id", {"baz": 3}),
    ]

    plot_properties = _squash_plots_properties(group)

    assert plot_properties == {"foo": 1, "bar": 2, "baz": 3}
