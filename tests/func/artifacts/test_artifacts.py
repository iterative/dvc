import logging
import os
from copy import deepcopy

import pytest

from dvc.annotations import Artifact
from dvc.exceptions import ArtifactNotFoundError, InvalidArgumentError
from dvc.repo.artifacts import Artifacts, check_name_format
from dvc.testing.tmp_dir import make_subrepo
from dvc.utils import as_posix
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
    bad_name_dvcyaml["artifacts"]["_bad_name_"] = {"type": "model", "path": "bad.pkl"}

    (tmp_dir / "dvc.yaml").dump(bad_name_dvcyaml)

    artifacts = {
        name: Artifact(**values)
        for name, values in bad_name_dvcyaml["artifacts"].items()
    }

    with caplog.at_level(logging.WARNING):
        assert tmp_dir.dvc.artifacts.read() == {"dvc.yaml": artifacts}
        assert "Can't use '_bad_name_' as artifact name (ID)" in caplog.text


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
    assert tmp_dir.dvc.artifacts.read() == {f"subdir{os.path.sep}dvc.yaml": artifacts}


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


bad_dvcyaml_missing_path = {"artifacts": {"lol": {}}}


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
    "name", ["1", "m", "nn", "m1", "1nn", "model-prod", "model-prod-v1"]
)
def test_name_is_compatible(name):
    check_name_format(name)


@pytest.mark.parametrize(
    "name",
    [
        "",
        "m/",
        "/m",
        "###",
        "@@@",
        "a model",
        "-model",
        "model-",
        "model@1",
        "model#1",
        "@namespace/model",
    ],
)
def test_name_is_compatible_fails(name):
    with pytest.raises(InvalidArgumentError):
        check_name_format(name)


def test_get_rev(tmp_dir, dvc, scm):
    scm.tag("myart@v1.0.0#1", annotated=True, message="foo")
    scm.tag("subdir=myart@v2.0.0#1", annotated=True, message="foo")
    scm.tag("myart#dev#1", annotated=True, message="foo")
    rev = scm.get_rev()

    assert dvc.artifacts.get_rev("myart") == rev
    assert dvc.artifacts.get_rev("myart", version="v1.0.0") == rev
    assert dvc.artifacts.get_rev("subdir:myart", version="v2.0.0") == rev
    assert dvc.artifacts.get_rev("subdir/dvc.yaml:myart", version="v2.0.0") == rev
    with pytest.raises(ArtifactNotFoundError):
        dvc.artifacts.get_rev("myart", version="v3.0.0")
    with pytest.raises(ArtifactNotFoundError):
        dvc.artifacts.get_rev("myart", stage="prod")


def test_get_path(tmp_dir, dvc, scm):
    (tmp_dir / "dvc.yaml").dump(dvcyaml)
    subdir = tmp_dir / "subdir"
    subdir.mkdir()
    (subdir / "dvc.yaml").dump(dvcyaml)

    assert dvc.artifacts.get_path("myart") == "myart.pkl"
    assert dvc.artifacts.get_path("subdir:myart") == os.path.join("subdir", "myart.pkl")
    assert dvc.artifacts.get_path("subdir/dvc.yaml:myart") == os.path.join(
        "subdir", "myart.pkl"
    )


def test_parametrized(tmp_dir, dvc):
    (tmp_dir / "params.yaml").dump({"path": "myart.pkl"})
    (tmp_dir / "dvc.yaml").dump(
        {"artifacts": {"myart": {"type": "model", "path": "${path}"}}}
    )
    assert tmp_dir.dvc.artifacts.read() == {
        "dvc.yaml": {"myart": Artifact(path="myart.pkl", type="model")}
    }


def test_get_path_subrepo(tmp_dir, scm, dvc):
    subrepo = tmp_dir / "subrepo"
    make_subrepo(subrepo, scm)
    (subrepo / "dvc.yaml").dump(dvcyaml)

    assert dvc.artifacts.get_path("subrepo:myart") == os.path.join(
        "subrepo", "myart.pkl"
    )
    assert dvc.artifacts.get_path("subrepo/dvc.yaml:myart") == os.path.join(
        "subrepo", "myart.pkl"
    )

    assert subrepo.dvc.artifacts.get_path("subrepo:myart") == os.path.join(
        "subrepo", "myart.pkl"
    )
    assert subrepo.dvc.artifacts.get_path("subrepo/dvc.yaml:myart") == os.path.join(
        "subrepo", "myart.pkl"
    )


def get_tag_and_name(dirname, name, version):
    tagname = f"{name}@{version}"
    if dirname in (os.curdir, ""):
        return tagname, name
    return f"{dirname}={tagname}", f"{dirname}:{name}"


def make_artifact(tmp_dir, name, tag, path) -> Artifact:
    artifact = Artifact(path=path.name, type="model")
    dvcfile = path.with_name("dvc.yaml")

    tmp_dir.scm_gen(path, "hello_world", commit="add myart.pkl")
    tmp_dir.dvc.artifacts.add(name, artifact, dvcfile=os.fspath(dvcfile))
    tmp_dir.scm.add_commit([dvcfile], message="add dvc.yaml")
    tmp_dir.scm.tag(tag, annotated=True, message="foo")
    return artifact


@pytest.mark.parametrize("sub", ["sub", ""])
def test_artifacts_download(tmp_dir, dvc, scm, sub):
    subdir = tmp_dir / sub
    dirname = str(subdir.relative_to(tmp_dir))
    tag, name = get_tag_and_name(as_posix(dirname), "myart", "v2.0.0")
    make_artifact(tmp_dir, "myart", tag, subdir / "myart.pkl")

    result = (1, "myart.pkl")
    assert Artifacts.get(".", name, force=True) == result
    assert Artifacts.get(tmp_dir.fs_path, name, force=True) == result
    assert Artifacts.get(f"file://{tmp_dir.as_posix()}", name, force=True) == result
    assert Artifacts.get(subdir.fs_path, name, force=True) == result
    with subdir.chdir():
        assert Artifacts.get(".", name, force=True) == result


@pytest.mark.parametrize("sub", ["sub", ""])
def test_artifacts_download_subrepo(tmp_dir, scm, sub):
    subrepo = tmp_dir / "subrepo"
    make_subrepo(subrepo, scm)
    subdir = subrepo / sub

    dirname = str(subdir.relative_to(tmp_dir))
    tag, name = get_tag_and_name(as_posix(dirname), "myart", "v2.0.0")
    make_artifact(subrepo, "myart", tag, subdir / "myart.pkl")

    result = (1, "myart.pkl")
    assert Artifacts.get(".", name) == result
    assert Artifacts.get(tmp_dir.fs_path, name, force=True) == result
    assert Artifacts.get(f"file://{tmp_dir.as_posix()}", name, force=True) == result
    assert Artifacts.get(subdir.fs_path, name, force=True) == result
    with subdir.chdir():
        assert Artifacts.get(".", name, force=True) == result


def test_artifacts_download_studio(tmp_dir, dvc, mocker):
    with dvc.config.edit("global") as conf:
        conf["studio"]["token"] = "mytoken"

    download_studio = mocker.patch("dvc.repo.artifacts.Artifacts._download_studio")
    Artifacts.get("myart.pkl", "myart.pkl")
    assert download_studio.call_args.kwargs["dvc_studio_config"]["token"] == "mytoken"
