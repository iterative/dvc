import dpath

from dvc.repo.plots import Plots


def test_subdir_config_not_overwritten_by_parents(tmp_dir, scm, dvc):
    plot_data = [
        {"x": 1, "y": 0.1},
        {"x": 2, "y": 0.2},
        {"x": 3, "y": 0.3},
    ]
    subdir_plot_data = [
        {"x": 1, "y": 0.2, "z": 0.1},
        {"x": 2, "y": 0.3, "z": 0.2},
        {"x": 3, "y": 0.4, "z": 0.3},
    ]

    (tmp_dir / "plots").mkdir()
    (tmp_dir / "plots" / "subdir").mkdir()

    (tmp_dir / "plots" / "plot.json").dump_json(plot_data)
    (tmp_dir / "plots" / "subdir" / "plot.json").dump_json(subdir_plot_data)

    plots_config = [
        {
            "plots/subdir/": {
                "x": "z",
                "y": "x",
            }
        },
        {"plots": {"x": "x", "y": "y"}},
        {
            "subdir axis defined by filename": {
                "x": {"plots/subdir/plot.json": "x"},
                "y": {"plots/subdir/plot.json": "y"},
            }
        },
    ]

    from dvc.utils.serialize import modify_yaml

    with modify_yaml("dvc.yaml") as dvcfile_content:
        dvcfile_content["plots"] = plots_config

    scm.add(
        [
            "plots/plot.json",
            "plots/subdir/plot.json",
            "dvc.yaml",
        ]
    )
    scm.commit("add data sources")

    plots = next(Plots(dvc).collect())

    assert dpath.get(plots, "workspace/definitions/data/dvc.yaml/data") == {
        "plots/plot.json": {"x": "x", "y": "y"},
        "plots/subdir/plot.json": {"x": "z", "y": "x"},
        "subdir axis defined by filename": {
            "x": {"plots/subdir/plot.json": "x"},
            "y": {"plots/subdir/plot.json": "y"},
        },
    }
