import pytest


@pytest.fixture
def repo_with_plots(tmp_dir, scm, dvc, run_copy_metrics):
    def make():
        linear_v1 = [
            {"x": 1, "y": 0.1},
            {"x": 2, "y": 0.2},
            {"x": 3, "y": 0.3},
        ]

        confusion_v1 = [
            {"actual": 0, "predicted": 1},
            {"actual": 0, "predicted": 1},
            {"actual": 1, "predicted": 0},
            {"actual": 1, "predicted": 0},
        ]

        image_v1 = b"content"

        (tmp_dir / "linear_src.json").dump_json(linear_v1)
        (tmp_dir / "confusion_src.json").dump_json(confusion_v1)
        (tmp_dir / "image_src.png").write_bytes(image_v1)

        scm.add(["linear_src.json", "confusion_src.json", "image_src.png"])
        scm.commit("add data sources")

        run_copy_metrics(
            "linear_src.json",
            "linear.json",
            name="linear",
            plots=["linear.json"],
            commit="linear",
        )
        run_copy_metrics(
            "confusion_src.json",
            "confusion.json",
            name="confusion",
            plots=["confusion.json"],
            commit="confusion",
        )
        linear_props = {"title": "linear", "x": "x"}
        dvc.plots.modify("linear.json", linear_props)
        confusion_props = {
            "title": "confusion matrix",
            "x": "predicted",
            "y": "actual",
            "template": "confusion",
        }
        dvc.plots.modify("confusion.json", confusion_props)
        run_copy_metrics(
            "image_src.png",
            "image.png",
            name="image",
            plots=["image.png"],
            commit="image",
        )

        scm.add(["dvc.yaml", "dvc.lock"])
        scm.commit("commit dvc files")
        yield image_v1, linear_v1, confusion_v1, confusion_props
        linear_v2 = [
            {"x": 1, "y": 0.2},
            {"x": 2, "y": 0.3},
            {"x": 3, "y": 0.4},
        ]
        confusion_v2 = [
            {"actual": 0, "predicted": 0},
            {"actual": 0, "predicted": 0},
            {"actual": 1, "predicted": 1},
            {"actual": 1, "predicted": 1},
        ]
        image_v2 = b"content2"

        (tmp_dir / "linear_src.json").dump_json(linear_v2)
        (tmp_dir / "confusion_src.json").dump_json(confusion_v2)
        (tmp_dir / "image_src.png").write_bytes(image_v2)

        dvc.reproduce()
        yield image_v2, linear_v2, confusion_v2, confusion_props

    return make


@pytest.fixture
def repo_with_config_plots(tmp_dir, scm, dvc, run_copy_metrics):
    def make():
        # test subdir functionality
        linear_subdir_a_v1 = [
            {"x": 1, "y": 0.2},
            {"x": 2, "y": 0.3},
            {"x": 3, "y": 0.4},
        ]
        # test subdir default values for x = step
        linear_subdir_b_v1 = [
            {"step": 1, "y": 0.2},
            {"step": 2, "y": 0.3},
            {"step": 3, "y": 0.4},
        ]
        linear_train_v1 = [
            {"x": 1, "y": 0.1},
            {"x": 2, "y": 0.2},
            {"x": 3, "y": 0.3},
        ]
        linear_test_v1 = [
            {"x": 1, "y": 0.3},
            {"x": 2, "y": 0.4},
            {"x": 3, "y": 0.5},
        ]

        confusion_train_v1 = [
            {"actual": 0, "predicted": 1},
            {"actual": 0, "predicted": 1},
            {"actual": 1, "predicted": 0},
            {"actual": 1, "predicted": 0},
        ]
        confusion_test_v1 = [
            {"actual": 0, "predicted": 1},
            {"actual": 0, "predicted": 0},
            {"actual": 1, "predicted": 1},
            {"actual": 1, "predicted": 0},
        ]

        (tmp_dir / "linear_subdir_a_src.json").dump_json(linear_subdir_a_v1)
        (tmp_dir / "linear_subdir_b_src.json").dump_json(linear_subdir_b_v1)
        (tmp_dir / "linear_train_src.json").dump_json(linear_train_v1)
        (tmp_dir / "linear_test_src.json").dump_json(linear_test_v1)
        (tmp_dir / "confusion_train_src.json").dump_json(confusion_train_v1)
        (tmp_dir / "confusion_test_src.json").dump_json(confusion_test_v1)

        scm.add(
            [
                "linear_subdir_a_src.json",
                "linear_subdir_b_src.json",
                "linear_train_src.json",
                "linear_test_src.json",
                "confusion_train_src.json",
                "confusion_test_src.json",
            ]
        )
        scm.commit("add data sources")

        (tmp_dir / "subdirA").mkdir()
        (tmp_dir / "subdirB").mkdir()

        run_copy_metrics(
            "linear_subdir_a_src.json",
            "subdirA/linear_subdir.json",
            name="linear_subdir",
            outs=["subdirA/linear_subdir.json"],
            commit="linear_subdir",
        )
        run_copy_metrics(
            "linear_subdir_b_src.json",
            "subdirB/linear_subdir.json",
            name="linear_subdir",
            outs=["subdirB/linear_subdir.json"],
            commit="linear_subdir",
        )
        run_copy_metrics(
            "linear_train_src.json",
            "linear_train.json",
            name="linear_train",
            outs=["linear_train.json"],
            commit="linear_train",
        )
        run_copy_metrics(
            "linear_test_src.json",
            "linear_test.json",
            name="linear_test",
            outs=["linear_test.json"],
            commit="linear_test",
        )
        run_copy_metrics(
            "confusion_train_src.json",
            "confusion_train.json",
            name="confusion_train",
            outs=["confusion_train.json"],
            commit="confusion_train",
        )
        run_copy_metrics(
            "confusion_test_src.json",
            "confusion_test.json",
            name="confusion_test",
            outs=["confusion_test.json"],
            commit="confusion_test",
        )

        subdir_a_config = {
            "x": "x",
            "y": "y",
            "title": "subdir plots with x and y defined",
        }

        subdir_b_config = {"title": "subdir plots with default x and y"}

        other_plots_config = [
            {
                "linear_train_vs_test": {
                    "x": "x",
                    "y": {"linear_train.json": "y", "linear_test.json": "y"},
                    "title": "linear plot",
                }
            },
            {
                "confusion_train_vs_test": {
                    "x": "actual",
                    "y": {
                        "confusion_train.json": "predicted",
                        "confusion_test.json": "predicted",
                    },
                    "template": "confusion",
                }
            },
        ]

        from dvc.utils.serialize import modify_yaml

        with modify_yaml("dvc.yaml") as dvcfile_content:
            dvcfile_content["plots"] = [
                {"subdirA": subdir_a_config},
                {"subdirB": subdir_b_config},
                *other_plots_config,
            ]

        scm.add(["dvc.yaml", "dvc.lock"])
        scm.commit("commit dvc files")

        # remove generate .gitignore that are considered as plot otherwise
        (tmp_dir / "subdirA" / ".gitignore").unlink()
        (tmp_dir / "subdirB" / ".gitignore").unlink()

        yield {
            "data": {
                "subdirA/linear_subdir.json": linear_subdir_a_v1,
                "subdirB/linear_subdir.json": linear_subdir_b_v1,
                "linear_train.json": linear_train_v1,
                "linear_test.json": linear_test_v1,
                "confusion_train.json": confusion_train_v1,
                "confusion_test.json": confusion_test_v1,
            },
            "configs": {
                "dvc.yaml": [
                    {"subdirA/linear_subdir.json": subdir_a_config},
                    {"subdirB/linear_subdir.json": subdir_b_config},
                    *other_plots_config,
                ]
            },
        }

    return make


@pytest.fixture
def repo_with_dvclive_plots(tmp_dir, scm, dvc, run_copy_metrics):
    def make():
        metrics = [
            {"step": 1, "metric": 0.1},
            {"step": 2, "metric": 0.2},
            {"step": 3, "metric": 0.3},
        ]

        metrics_path = tmp_dir / "dvclive" / "plots" / "metrics" / "metric.tsv"
        metrics_path.parent.mkdir(parents=True)
        metrics_path.dump_json(metrics)

        plots_config_v1 = [{"plots/metrics": {"x": "step"}}]

        from dvc.utils.serialize import modify_yaml

        dvcyaml_path_v1 = tmp_dir / "dvclive" / "dvc.yaml"
        with modify_yaml(dvcyaml_path_v1) as dvcfile_content:
            dvcfile_content["plots"] = plots_config_v1
        scm.add([metrics_path, dvcyaml_path_v1])
        scm.commit("add dvclive 2.x plots")

        yield {
            "data": {metrics_path: metrics},
            "configs": {dvcyaml_path_v1: plots_config_v1},
        }

        plots_config_v2 = [{"dvclive/plots/metrics": {"x": "step"}}]

        dvcyaml_path_v1.unlink()
        dvcyaml_path_v2 = tmp_dir / "dvc.yaml"
        with modify_yaml(dvcyaml_path_v2) as dvcfile_content:
            dvcfile_content["plots"] = plots_config_v2

        yield {
            "data": {metrics_path: metrics},
            "configs": {dvcyaml_path_v2: plots_config_v2},
        }

    return make
