import json
import os


def test_plots_order(tmp_dir, dvc, dummy_stage):
    tmp_dir.gen(
        {
            "p.json": json.dumps([{"p1": 1}, {"p1": 2}]),
            "p1.json": json.dumps([{"p2": 1}, {"p2": 2}]),
            "sub": {
                "p3.json": json.dumps([{"p3": 1}, {"p3": 2}]),
                "p4.json": json.dumps([{"p4": 1}, {"p4": 2}]),
            },
        }
    )

    dummy_stage(plots=["p.json", str(tmp_dir / "sub" / "p4.json")])
    dummy_stage(
        path=str(tmp_dir / "sub" / "dvc.yaml"),
        plots=["p1.json", str(tmp_dir / "sub" / "p3.json")],
    )

    assert list(dvc.plots.show()) == [
        "p.json",
        os.path.join("sub", "p4.json"),
        "p1.json",
        os.path.join("sub", "p3.json"),
    ]
