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
