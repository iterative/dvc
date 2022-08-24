import os
from textwrap import dedent

import pytest

from dvc import api
from dvc.exceptions import DvcException


@pytest.fixture
def params_repo(tmp_dir, scm, dvc):
    tmp_dir.gen("params.yaml", "foo: 1")
    tmp_dir.gen("params.json", '{"bar": 2, "foobar": 3}')
    tmp_dir.gen("other_params.json", '{"foo": {"bar": 4}}')

    dvc.run(
        name="stage-0",
        cmd="echo stage-0",
    )

    dvc.run(
        name="stage-1",
        cmd="echo stage-1",
        params=["foo", "params.json:bar"],
    )

    dvc.run(
        name="stage-2",
        cmd="echo stage-2",
        params=["other_params.json:foo"],
    )

    dvc.run(
        name="stage-3",
        cmd="echo stage-2",
        params=["params.json:foobar"],
    )

    scm.add(
        [
            "params.yaml",
            "params.json",
            "other_params.json",
            "dvc.yaml",
            "dvc.lock",
        ]
    )
    scm.commit("commit dvc files")

    tmp_dir.gen("params.yaml", "foo: 5")
    scm.add(["params.yaml"])
    scm.commit("update params.yaml")


def test_params_show_no_args(params_repo):
    assert api.params_show() == {
        "params.yaml:foo": 5,
        "bar": 2,
        "foobar": 3,
        "other_params.json:foo": {"bar": 4},
    }


def test_params_show_targets(params_repo):
    assert api.params_show("params.yaml") == {"foo": 5}
    assert api.params_show("params.yaml", "params.json") == {
        "foo": 5,
        "bar": 2,
        "foobar": 3,
    }
    assert api.params_show("params.yaml", stages="stage-1") == {
        "foo": 5,
    }


def test_params_show_deps(params_repo):
    params = api.params_show(deps=True)
    assert params == {
        "params.yaml:foo": 5,
        "bar": 2,
        "foobar": 3,
        "other_params.json:foo": {"bar": 4},
    }


def test_params_show_stages(params_repo):
    assert api.params_show(stages="stage-2") == {"foo": {"bar": 4}}

    assert api.params_show() == api.params_show(
        stages=["stage-1", "stage-2", "stage-3"]
    )

    assert api.params_show("params.json", stages="stage-3") == {"foobar": 3}

    with pytest.raises(DvcException, match="No params found"):
        api.params_show(stages="stage-0")


def test_params_show_stage_addressing(tmp_dir, dvc):
    for subdir in {"subdir1", "subdir2"}:
        subdir = tmp_dir / subdir
        subdir.mkdir()
        with subdir.chdir():
            subdir.gen("params.yaml", "foo: 1")

            dvc.run(name="stage-0", cmd="echo stage-0", params=["foo"])

    for s in {"subdir1", "subdir2"}:
        dvcyaml = os.path.join(s, "dvc.yaml")
        assert api.params_show(stages=f"{dvcyaml}:stage-0") == {"foo": 1}

    with subdir.chdir():
        nested = subdir / "nested"
        nested.mkdir()
        with nested.chdir():
            dvcyaml = os.path.join("..", "dvc.yaml")
            assert api.params_show(stages=f"{dvcyaml}:stage-0") == {"foo": 1}


def test_params_show_revs(params_repo):
    assert api.params_show(rev="HEAD~1") == {
        "params.yaml:foo": 1,
        "bar": 2,
        "foobar": 3,
        "other_params.json:foo": {"bar": 4},
    }


def test_params_show_while_running_stage(tmp_dir, dvc):
    (tmp_dir / "params.yaml").dump({"foo": {"bar": 1}})
    (tmp_dir / "params.json").dump({"bar": 2})

    tmp_dir.gen(
        "merge.py",
        dedent(
            """
            import json
            from dvc import api
            with open("merged.json", "w") as f:
                json.dump(api.params_show(stages="merge"), f)
        """
        ),
    )
    dvc.stage.add(
        name="merge",
        cmd="python merge.py",
        params=["foo.bar", {"params.json": ["bar"]}],
        outs=["merged.json"],
    )

    dvc.reproduce()

    assert (tmp_dir / "merged.json").parse() == {"foo": {"bar": 1}, "bar": 2}


def test_params_show_repo(tmp_dir, erepo_dir):
    with erepo_dir.chdir():
        erepo_dir.scm_gen("params.yaml", "foo: 1", commit="Create params.yaml")
        erepo_dir.dvc.run(
            name="stage-1",
            cmd="echo stage-1",
            params=["foo"],
        )
    assert api.params_show(repo=erepo_dir) == {"foo": 1}


def test_params_show_no_params_found(tmp_dir, dvc):
    # Empty repo
    with pytest.raises(DvcException, match="No params found"):
        api.params_show()

    # params.yaml but no dvc.yaml
    (tmp_dir / "params.yaml").dump({"foo": 1})
    assert api.params_show() == {"foo": 1}

    # dvc.yaml but no params.yaml
    (tmp_dir / "params.yaml").unlink()
    dvc.stage.add(name="echo", cmd="echo foo")
    with pytest.raises(DvcException, match="No params found"):
        api.params_show()


def test_params_show_stage_without_params(tmp_dir, dvc):
    tmp_dir.gen("params.yaml", "foo: 1")

    dvc.run(
        name="stage-0",
        cmd="echo stage-0",
    )

    with pytest.raises(DvcException, match="No params found"):
        api.params_show(stages="stage-0")

    with pytest.raises(DvcException, match="No params found"):
        api.params_show(deps=True)


def test_params_show_untracked_target(params_repo, tmp_dir):
    tmp_dir.gen("params_foo.yaml", "foo: 1")

    assert api.params_show("params_foo.yaml") == {"foo": 1}

    with pytest.raises(DvcException, match="No params found"):
        api.params_show("params_foo.yaml", stages="stage-0")

    with pytest.raises(DvcException, match="No params found"):
        api.params_show("params_foo.yaml", deps=True)
