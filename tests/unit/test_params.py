import os

from dvc.utils.serialize import dumps_yaml


def test_params_order(tmp_dir, dvc, dummy_stage):
    tmp_dir.gen(
        {
            "params.yaml": dumps_yaml({"p1": 1}),
            "p1.yaml": dumps_yaml({"p2": 1}),
            "sub": {"p2.yaml": dumps_yaml({"p3": 1})},
        }
    )

    p2_path = os.path.join("sub", "p2.yaml")
    sub_stage = os.path.join("sub", "dvc.yaml")

    dummy_stage(params=[{p2_path: ["p3"]}, {"p1.yaml": ["p2"]}])
    dummy_stage(path=sub_stage, params=["p1"])

    # params are sorted during dumping, therefore p1 is first
    assert list(dvc.params.show()[""]) == ["p1.yaml", p2_path, "params.yaml"]


def test_repro_unicode(tmp_dir, dvc, dummy_stage):
    tmp_dir.gen({"settings.json": '{"Ω_value": 1}'})
    dummy_stage(params=[{"settings.json": ["Ω_value"]}])
    dvc.reproduce(dry=True)
