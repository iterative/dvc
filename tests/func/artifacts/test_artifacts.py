import pytest

from dvc.annotations import Artifact

dvcyaml = {
    "artifacts": {
        "myart": {"type": "model"},
        "hello": {"type": "file", "path": "hello.txt"},
    }
}

dvcyaml2 = {
    "artifacts": {
        "model": {"type": "model"},
    }
}


def test_reading_artifacts_subdir(tmp_dir, dvc):
    subdir = tmp_dir / "subdir"
    subdir.mkdir()

    (subdir / "dvc.yaml").dump(dvcyaml)
    assert tmp_dir.dvc.artifacts.read() == {
        name: Artifact(**values) for name, values in dvcyaml["artifacts"].items()
    }


def test_reading_artifacts_two_dvcyamls(tmp_dir, dvc):
    (tmp_dir / "dvc.yaml").dump(dvcyaml)

    subdir = tmp_dir / "subdir"
    subdir.mkdir()

    (subdir / "dvc.yaml").dump(dvcyaml2)

    assert not set(dvcyaml["artifacts"]).intersection(dvcyaml2["artifacts"])
    assert tmp_dir.dvc.artifacts.read() == {
        name: Artifact(**values)
        for name, values in {**dvcyaml["artifacts"], **dvcyaml2["artifacts"]}.items()
    }


def test_exception_same_artifact_name(tmp_dir, dvc):
    (tmp_dir / "dvc.yaml").dump(dvcyaml)

    subdir = tmp_dir / "subdir"
    subdir.mkdir()

    (subdir / "dvc.yaml").dump(dvcyaml)

    with pytest.raises(ValueError):
        tmp_dir.dvc.artifacts.read()
