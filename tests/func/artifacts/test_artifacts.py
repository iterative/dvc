import pytest

from dvc.annotations import Artifact
from dvc.utils.strictyaml import YAMLSyntaxError, YAMLValidationError

dvcyaml = {
    "artifacts": {
        "myart": {"type": "model"},
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
        "subdir/dvc.yaml": artifacts,
    }


bad_dvcyaml_extra_field = {
    "artifacts": {
        "lol": {"kek": "cheburek"},
        "hello": {"type": "file", "path": "hello.txt"},
    }
}


def test_broken_dvcyaml_extra_field(tmp_dir, dvc):
    (tmp_dir / "dvc.yaml").dump(bad_dvcyaml_extra_field)

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
