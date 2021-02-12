import os

from dvc.utils.serialize import dumps_yaml


def test_params_order(tmp_dir, dvc):
    tmp_dir.gen(
        {
            "params.yaml": dumps_yaml({"p1": 1}),
            "p1.yaml": dumps_yaml({"p2": 1}),
            "sub": {"p2.yaml": dumps_yaml({"p3": 1})},
        }
    )

    p2_path = os.path.join("sub", "p2.yaml")
    dvc.stage.add(
        params=[{p2_path: ["p3"]}, {"p1.yaml": ["p2"]}],
        cmd="cmd1",
        name="stage1",
    )
    with (tmp_dir / "sub").chdir():
        dvc.stage.add(params=["p1"], cmd="cmd2", name="stage2")

    # params are sorted during dumping, therefore p1 is first
    assert list(dvc.params.show()[""]) == ["p1.yaml", p2_path, "params.yaml"]


def test_repro_unicode(tmp_dir, dvc):
    tmp_dir.gen({"settings.json": '{"Ω_value": 1}'})
    stage = dvc.stage.add(
        params=[{"settings.json": ["Ω_value"]}], cmd="cmd", name="stage1"
    )
    assert dvc.reproduce(dry=True) == [stage]

    stage.cmd = "foo"
    stage.dump()

    dvc.remove(stage.name)
    assert not (tmp_dir / "dvc.yaml").exists()
    assert not (tmp_dir / "dvc.lock").exists()
