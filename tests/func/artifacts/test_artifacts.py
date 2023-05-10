import os

import pytest

from dvc.annotations import Artifact
from dvc.repo.artifacts import name_is_compatible
from dvc.utils.strictyaml import YAMLSyntaxError, YAMLValidationError

dvcyaml = {
    "artifacts": {
        "myart": {"type": "model", "path": "myart.pkl"},
        "hello": {"type": "file", "path": "hello.txt"},
        "world": {
            "type": "object",
            "path": "world.txt",
            "desc": "The world is not enough",
            "labels": ["but", "this", "is"],
            "meta": {"such": "a", "perfect": "place to start"},
        },
    }
}


def test_reading_artifacts_subdir(tmp_dir, dvc):
    (tmp_dir / "dvc.yaml").dump(dvcyaml)

    subdir = tmp_dir / "subdir"
    subdir.mkdir()

    (subdir / "dvc.yaml").dump(dvcyaml)

    artifacts = {
        name: Artifact(**values) for name, values in dvcyaml["artifacts"].items()
    }
    assert tmp_dir.dvc.artifacts.read() == {
        "dvc.yaml": artifacts,
        f"subdir{os.path.sep}dvc.yaml": artifacts,
    }


bad_dvcyaml_extra_field = {
    "artifacts": {
        "lol": {"kek": "cheburek", "path": "lol"},
        "hello": {"type": "file", "path": "hello.txt"},
    }
}


bad_dvcyaml_missing_path = {
    "artifacts": {
        "lol": {},
    }
}


@pytest.mark.parametrize(
    "bad_dvcyaml", [bad_dvcyaml_extra_field, bad_dvcyaml_missing_path]
)
def test_broken_dvcyaml_extra_field(tmp_dir, dvc, bad_dvcyaml):
    (tmp_dir / "dvc.yaml").dump(bad_dvcyaml)

    with pytest.raises(YAMLValidationError):
        tmp_dir.dvc.artifacts.read()


bad_dvcyaml_id_duplication = """
artifacts:
  lol:
    type: kek
  lol: {}
"""


def test_broken_dvcyaml_id_duplication(tmp_dir, dvc):
    with open(tmp_dir / "dvc.yaml", "w") as f:
        f.write(bad_dvcyaml_id_duplication)

    with pytest.raises(YAMLSyntaxError):
        tmp_dir.dvc.artifacts.read()


dvcyaml_redirecting = {"artifacts": "artifacts.yaml"}


def test_read_artifacts_yaml(tmp_dir, dvc):
    (tmp_dir / "dvc.yaml").dump(dvcyaml_redirecting)
    (tmp_dir / "artifacts.yaml").dump(dvcyaml["artifacts"])

    artifacts = {
        name: Artifact(**values) for name, values in dvcyaml["artifacts"].items()
    }
    assert tmp_dir.dvc.artifacts.read() == {
        "dvc.yaml": artifacts,
    }


@pytest.mark.parametrize(
    "name",
    [
        "m",
        "nn",
        "m1",
        "model-prod",
        "model-prod-v1",
    ],
)
def test_check_name_is_valid(name):
    assert name_is_compatible(name)


@pytest.mark.parametrize(
    "name",
    [
        "",
        "1",
        "m/",
        "/m",
        "1nn",
        "###",
        "@@@",
        "a model",
        "a_model",
        "-model",
        "model-",
        "model@1",
        "model#1",
        "@namespace/model",
        "namespace/model",
    ],
)
def test_check_name_is_invalid(name):
    assert not name_is_compatible(name)
