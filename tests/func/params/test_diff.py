from os.path import join

import pytest

from dvc.utils import relpath


def test_diff_no_params(tmp_dir, scm, dvc):
    assert dvc.params.diff() == {}


def test_diff_no_changes(tmp_dir, scm, dvc):
    tmp_dir.gen("params.yaml", "foo: bar")
    dvc.run(cmd="echo params.yaml", params=["foo"], single_stage=True)
    scm.add(["params.yaml", "Dvcfile"])
    scm.commit("bar")
    assert dvc.params.diff() == {}


def test_diff(tmp_dir, scm, dvc):
    tmp_dir.gen("params.yaml", "foo: bar")
    dvc.run(cmd="echo params.yaml", params=["foo"], single_stage=True)
    scm.add(["params.yaml", "Dvcfile"])
    scm.commit("bar")

    tmp_dir.scm_gen("params.yaml", "foo: baz", commit="baz")
    tmp_dir.scm_gen("params.yaml", "foo: qux", commit="qux")

    assert dvc.params.diff(a_rev="HEAD~2") == {
        "params.yaml": {"foo": {"old": "bar", "new": "qux"}}
    }


def test_diff_dirty(tmp_dir, scm, dvc):
    tmp_dir.gen("params.yaml", "foo: bar")
    dvc.run(cmd="echo params.yaml", params=["foo"], single_stage=True)
    scm.add(["params.yaml", "Dvcfile"])
    scm.commit("bar")

    tmp_dir.scm_gen("params.yaml", "foo: baz", commit="baz")
    tmp_dir.gen("params.yaml", "foo: qux")

    assert dvc.params.diff() == {
        "params.yaml": {"foo": {"old": "baz", "new": "qux"}}
    }


def test_diff_new(tmp_dir, scm, dvc):
    tmp_dir.gen("params.yaml", "foo: bar")
    dvc.run(cmd="echo params.yaml", params=["foo"], single_stage=True)

    assert dvc.params.diff() == {
        "params.yaml": {"foo": {"old": None, "new": "bar"}}
    }


def test_diff_deleted(tmp_dir, scm, dvc):
    tmp_dir.gen("params.yaml", "foo: bar")
    dvc.run(cmd="echo params.yaml", params=["foo"], single_stage=True)
    scm.add(["params.yaml", "Dvcfile"])
    scm.commit("bar")

    (tmp_dir / "params.yaml").unlink()

    assert dvc.params.diff() == {
        "params.yaml": {"foo": {"old": "bar", "new": None}}
    }


def test_diff_list(tmp_dir, scm, dvc):
    tmp_dir.gen("params.yaml", "foo:\n- bar\n- baz")
    dvc.run(cmd="echo params.yaml", params=["foo"], single_stage=True)
    scm.add(["params.yaml", "Dvcfile"])
    scm.commit("foo")

    tmp_dir.gen("params.yaml", "foo:\n- bar\n- baz\n- qux")

    assert dvc.params.diff() == {
        "params.yaml": {
            "foo": {"old": "['bar', 'baz']", "new": "['bar', 'baz', 'qux']"}
        }
    }


def test_diff_dict(tmp_dir, scm, dvc):
    tmp_dir.gen("params.yaml", "foo:\n  bar: baz")
    dvc.run(cmd="echo params.yaml", params=["foo"], single_stage=True)
    scm.add(["params.yaml", "Dvcfile"])
    scm.commit("foo")

    tmp_dir.gen("params.yaml", "foo:\n  bar: qux")

    assert dvc.params.diff() == {
        "params.yaml": {"foo.bar": {"old": "baz", "new": "qux"}}
    }


def test_diff_with_unchanged(tmp_dir, scm, dvc):
    tmp_dir.gen("params.yaml", "foo: bar\nxyz: val")
    dvc.run(cmd="echo params.yaml", params=["foo,xyz"], single_stage=True)
    scm.add(["params.yaml", "Dvcfile"])
    scm.commit("bar")

    tmp_dir.scm_gen("params.yaml", "foo: baz\nxyz: val", commit="baz")
    tmp_dir.scm_gen("params.yaml", "foo: qux\nxyz: val", commit="qux")

    assert dvc.params.diff(a_rev="HEAD~2", all=True) == {
        "params.yaml": {
            "foo": {"old": "bar", "new": "qux"},
            "xyz": {"old": "val", "new": "val"},
        }
    }


def test_pipeline_tracked_params(tmp_dir, scm, dvc, run_copy):
    from dvc.dvcfile import PIPELINE_FILE

    tmp_dir.gen({"foo": "foo", "params.yaml": "foo: bar\nxyz: val"})
    run_copy("foo", "bar", name="copy-foo-bar", params=["foo,xyz"])

    scm.add(["params.yaml", PIPELINE_FILE])
    scm.commit("add stage")

    tmp_dir.scm_gen("params.yaml", "foo: baz\nxyz: val", commit="baz")
    tmp_dir.scm_gen("params.yaml", "foo: qux\nxyz: val", commit="qux")

    assert dvc.params.diff(a_rev="HEAD~2") == {
        "params.yaml": {"foo": {"old": "bar", "new": "qux"}}
    }


def test_no_commits(tmp_dir):
    from scmrepo.git import Git

    from dvc.repo import Repo

    git = Git.init(tmp_dir.fs_path)
    assert git.no_commits

    assert Repo.init().params.diff() == {}


def test_vars_shows_on_params_diff(tmp_dir, scm, dvc):
    params_file = tmp_dir / "test_params.yaml"
    param_data = {"vars": {"model1": {"epoch": 15}, "model2": {"epoch": 35}}}
    (tmp_dir / params_file).dump(param_data)
    d = {
        "vars": ["test_params.yaml"],
        "stages": {
            "build": {
                "foreach": "${vars}",
                "do": {"cmd": "script --epoch ${item.epoch}"},
            }
        },
    }
    (tmp_dir / "dvc.yaml").dump(d)
    assert dvc.params.diff() == {
        "test_params.yaml": {
            "vars.model1.epoch": {"new": 15, "old": None},
            "vars.model2.epoch": {"new": 35, "old": None},
        }
    }
    scm.add(["dvc.yaml", "test_params.yaml"])
    scm.commit("added stages")

    param_data["vars"]["model1"]["epoch"] = 20
    (tmp_dir / params_file).dump(param_data)
    assert dvc.params.diff() == {
        "test_params.yaml": {
            "vars.model1.epoch": {"new": 20, "old": 15, "diff": 5}
        }
    }

    data_dir = tmp_dir / "data"
    data_dir.mkdir()
    with data_dir.chdir():
        assert dvc.params.diff() == {
            relpath(params_file): {
                "vars.model1.epoch": {"new": 20, "old": 15, "diff": 5}
            }
        }


def test_diff_targeted(tmp_dir, scm, dvc, run_copy):
    from dvc.dvcfile import PIPELINE_FILE

    tmp_dir.gen(
        {
            "foo": "foo",
            "params.yaml": "foo: bar",
            "other_params.yaml": "xyz: val",
        }
    )
    run_copy(
        "foo",
        "bar",
        name="copy-foo-bar",
        params=["foo", "other_params.yaml:xyz"],
    )

    scm.add(["params.yaml", "other_params.yaml", PIPELINE_FILE])
    scm.commit("add stage")

    tmp_dir.scm_gen(
        {"params.yaml": "foo: baz", "other_params.yaml": "xyz: val2"},
        commit="baz",
    )
    tmp_dir.scm_gen(
        {"params.yaml": "foo: qux", "other_params.yaml": "xyz: val3"},
        commit="qux",
    )

    assert dvc.params.diff(a_rev="HEAD~2") == {
        "params.yaml": {"foo": {"old": "bar", "new": "qux"}},
        "other_params.yaml": {"xyz": {"old": "val", "new": "val3"}},
    }

    assert dvc.params.diff(a_rev="HEAD~2", targets=["params.yaml"]) == {
        "params.yaml": {"foo": {"old": "bar", "new": "qux"}}
    }

    assert dvc.params.diff(a_rev="HEAD~2", targets=["other_params.yaml"]) == {
        "other_params.yaml": {"xyz": {"old": "val", "new": "val3"}}
    }


@pytest.mark.parametrize("file", ["params.yaml", "other_params.yaml"])
def test_diff_without_targets_specified(tmp_dir, dvc, scm, file):
    params_file = tmp_dir / file
    params_file.dump({"foo": {"bar": "bar"}, "x": "0"})
    dvc.stage.add(
        name="test",
        cmd=f"echo {file}",
        params=[{file: None}],
    )
    scm.add_commit([params_file, "dvc.yaml"], message="foo")

    params_file.dump({"foo": {"bar": "baz"}, "y": "100"})
    assert dvc.params.diff() == {
        file: {
            "foo.bar": {"new": "baz", "old": "bar"},
            "x": {"new": None, "old": "0"},
            "y": {"new": "100", "old": None},
        }
    }


@pytest.mark.parametrize(
    "dvcfile, params_file",
    [
        ("dvc.yaml", "my_params.yaml"),
        ("dir/dvc.yaml", "my_params.yaml"),
        ("dir/dvc.yaml", join("..", "my_params.yaml")),
    ],
)
def test_diff_top_level_params(tmp_dir, dvc, scm, dvcfile, params_file):
    directory = (tmp_dir / dvcfile).parent
    directory.mkdir(exist_ok=True)
    (tmp_dir / dvcfile).dump({"params": [params_file]})

    params_file = directory / params_file
    params_file.dump({"foo": 3})
    scm.add_commit([params_file, tmp_dir / dvcfile], message="add params")

    params_file.dump({"foo": 5})
    assert dvc.params.diff() == {
        relpath(directory / params_file): {
            "foo": {"diff": 2, "new": 5, "old": 3}
        }
    }
