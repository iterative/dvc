from textwrap import dedent

import pytest

from dvc import api


@pytest.fixture
def params_repo(tmp_dir, scm, dvc):
    tmp_dir.gen("params.yaml", "foo: 1")
    tmp_dir.gen("params.json", '{"bar": 2, "foobar": 3}')
    tmp_dir.gen("other_params.json", '{"foo": {"bar": 4}}')

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
