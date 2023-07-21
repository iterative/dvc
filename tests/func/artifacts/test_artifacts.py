import logging
import os
from copy import deepcopy

import pytest

from dvc.annotations import Artifact
from dvc.exceptions import InvalidArgumentError
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


def test_artifacts_read_subdir(tmp_dir, dvc):
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


def test_artifacts_read_bad_name(tmp_dir, dvc, caplog):
    bad_name_dvcyaml = deepcopy(dvcyaml)
    bad_name_dvcyaml["artifacts"]["bad_name"] = {"type": "model", "path": "bad.pkl"}

    (tmp_dir / "dvc.yaml").dump(bad_name_dvcyaml)

    artifacts = {
        name: Artifact(**values)
        for name, values in bad_name_dvcyaml["artifacts"].items()
    }

    with caplog.at_level(logging.WARNING):
        assert tmp_dir.dvc.artifacts.read() == {"dvc.yaml": artifacts}
        assert "Can't use 'bad_name' as artifact name (ID)" in caplog.text


def test_artifacts_add_subdir(tmp_dir, dvc):
    subdir = tmp_dir / "subdir"
    subdir.mkdir()

    (subdir / "dvc.yaml").dump(dvcyaml)

    new_art = Artifact(path="path")
    tmp_dir.dvc.artifacts.add("new", new_art, dvcfile="subdir/dvc.yaml")

    artifacts = {
        name: Artifact(**values) for name, values in dvcyaml["artifacts"].items()
    }
    artifacts["new"] = new_art
    assert tmp_dir.dvc.artifacts.read() == {
        f"subdir{os.path.sep}dvc.yaml": artifacts,
    }


def test_artifacts_add_abspath(tmp_dir, dvc):
    subdir = tmp_dir / "subdir"
    subdir.mkdir()

    new_art = Artifact(path="path")
    tmp_dir.dvc.artifacts.add(
        "new", new_art, dvcfile=os.path.abspath("subdir/dvc.yaml")
    )

    assert tmp_dir.dvc.artifacts.read() == {
        f"subdir{os.path.sep}dvc.yaml": {"new": new_art},
    }


def test_artifacts_add_fails_on_dvc_subrepo(tmp_dir, dvc):
    # adding artifact to the DVC subrepo from the parent DVC repo
    # shouldn't work
    subdir = tmp_dir / "subdir"
    (subdir / ".dvc").mkdir(parents=True)

    with pytest.raises(InvalidArgumentError):
        tmp_dir.dvc.artifacts.add(
            "failing", Artifact(path="path"), dvcfile="subdir/dvc.yaml"
        )

    with pytest.raises(InvalidArgumentError):
        tmp_dir.dvc.artifacts.add(
            "failing", Artifact(path="path"), dvcfile="subdir/dvclive/dvc.yaml"
        )


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


def test_artifacts_read_fails_on_id_duplication(tmp_dir, dvc):
    with open(tmp_dir / "dvc.yaml", "w") as f:
        f.write(bad_dvcyaml_id_duplication)

    with pytest.raises(YAMLSyntaxError):
        tmp_dir.dvc.artifacts.read()


@pytest.mark.parametrize(
    "name",
    [
        "1",
        "m",
        "nn",
        "m1",
        "1nn",
        "model-prod",
        "model-prod-v1",
    ],
)
def test_name_is_compatible(name):
    assert name_is_compatible(name)


@pytest.mark.parametrize(
    "name",
    [
        "",
        "m/",
        "/m",
        "###",
        "@@@",
        "a model",
        "a_model",
        "-model",
        "model-",
        "model@1",
        "model#1",
        "@namespace/model",
    ],
)
def test_name_is_compatible_fails(name):
    assert not name_is_compatible(name)
