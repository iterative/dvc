import pytest

import dpath
from dvc.repo.plots import Plots


def test_plots_definitions_works_with_nested_plots(tmp_dir, scm, dvc):
    data_v1 = [
        {"x": 1, "y": 0.1},
        {"x": 2, "y": 0.2},
        {"x": 3, "y": 0.3},
    ]
    data_subdir_v1 = [
        {"x": 1, "y": 0.2, "z": 0.1},
        {"x": 2, "y": 0.3, "z": 0.2},
        {"x": 3, "y": 0.4, "z": 0.3},
    ]

    plots_dir = tmp_dir / "plots"
    subdir = plots_dir / "subdir"

    plots_dir.mkdir()
    subdir.mkdir()

    (plots_dir / "data_v1.json").dump_json(data_v1)
    (subdir / "data_subdir_v1.json").dump_json(data_subdir_v1)

    plots_config = [
        {
            "plots/subdir/": {
                "x": "z",
            }
        },
        {
            "plots": {
                "x": "x",
            }
        },
    ]

    from dvc.utils.serialize import modify_yaml

    with modify_yaml("dvc.yaml") as dvcfile_content:
        dvcfile_content["plots"] = plots_config

    scm.add(
        [
            "plots/data_v1.json",
            "plots/subdir/data_subdir_v1.json",
            "dvc.yaml",
        ]
    )
    scm.commit("add data sources")

    plots = next(Plots(dvc).collect())

    assert dpath.get(plots, "workspace/definitions/data/dvc.yaml/data") == {
        "plots/data_v1.json": {"x": "x"},
        "plots/subdir/data_subdir_v1.json": {"x": "z"},
    }
