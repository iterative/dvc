import shutil
from os.path import join

import pytest

from dvc.dvcfile import PROJECT_FILE
from dvc.repo import Repo
from dvc.repo.metrics.show import FileResult, Result
from dvc_data.index import DataIndexDirError


def test_show_empty(dvc):
    assert dvc.params.show() == {"": {"data": {}}}


def test_show(tmp_dir, dvc):
    tmp_dir.gen("params.yaml", "foo: bar")
    dvc.run(cmd="echo params.yaml", params=["foo"], name="echo-params")
    assert dvc.params.show() == {
        "": {"data": {"params.yaml": {"data": {"foo": "bar"}}}}
    }


def test_show_targets(tmp_dir, dvc):
    tmp_dir.gen("params.yaml", "foo: bar")
    dvc.run(cmd="echo params.yaml", params=["foo"], name="echo-params")
    expected = {"": {"data": {"params.yaml": {"data": {"foo": "bar"}}}}}
    assert dvc.params.show(targets=["params.yaml"]) == expected
    assert dvc.params.show(targets=(tmp_dir / "params.yaml").fs_path) == expected


def test_show_toml(tmp_dir, dvc):
    tmp_dir.gen("params.toml", "[foo]\nbar = 42\nbaz = [1, 2]\n")
    dvc.run(cmd="echo params.toml", params=["params.toml:foo"], name="echo-params")
    assert dvc.params.show() == {
        "": {"data": {"params.toml": {"data": {"foo": {"bar": 42, "baz": [1, 2]}}}}}
    }


def test_show_py(tmp_dir, dvc):
    tmp_dir.gen(
        "params.py",
        "CONST = 1\nIS_DIR: bool = True\n\n\nclass Config:\n    foo = 42\n",
    )
    dvc.run(
        cmd="echo params.py",
        params=["params.py:CONST,IS_DIR,Config.foo"],
        name="echo-params",
    )
    assert dvc.params.show() == {
        "": {
            "data": {
                "params.py": {
                    "data": {"CONST": 1, "Config": {"foo": 42}, "IS_DIR": True}
                }
            }
        }
    }


def test_show_multiple(tmp_dir, dvc):
    tmp_dir.gen("params.yaml", "foo: bar\nbaz: qux\n")
    dvc.run(cmd="echo params.yaml", params=["foo"], name="echo-params1")
    dvc.run(cmd="echo params.yaml", params=["baz"], name="echo-params2")
    assert dvc.params.show() == {
        "": {"data": {"params.yaml": {"data": {"baz": "qux", "foo": "bar"}}}}
    }


def test_show_list(tmp_dir, dvc):
    tmp_dir.gen("params.yaml", "foo:\n- bar\n- baz\n")
    dvc.run(cmd="echo params.yaml", params=["foo"], name="echo-params")
    assert dvc.params.show() == {
        "": {"data": {"params.yaml": {"data": {"foo": ["bar", "baz"]}}}}
    }


def test_show_branch(tmp_dir, scm, dvc):
    tmp_dir.gen("params.yaml", "foo: bar")
    dvc.run(cmd="echo params.yaml", params=["foo"], name="echo-params")
    scm.add(["params.yaml", "Dvcfile"])
    scm.commit("init")

    with tmp_dir.branch("branch", new=True):
        tmp_dir.scm_gen("params.yaml", "foo: baz", commit="branch")

    assert dvc.params.show(revs=["branch"]) == {
        "branch": {"data": {"params.yaml": {"data": {"foo": "baz"}}}},
        "workspace": {"data": {"params.yaml": {"data": {"foo": "bar"}}}},
    }


def test_pipeline_params(tmp_dir, scm, dvc, run_copy):
    tmp_dir.gen({"foo": "foo", "params.yaml": "foo: bar\nxyz: val\nabc: ignore"})
    run_copy("foo", "bar", name="copy-foo-bar", params=["foo,xyz"])
    scm.add(["params.yaml", PROJECT_FILE])
    scm.commit("add stage")

    tmp_dir.scm_gen("params.yaml", "foo: baz\nxyz: val\nabc: ignore", commit="baz")
    tmp_dir.scm_gen("params.yaml", "foo: qux\nxyz: val\nabc: ignore", commit="qux")

    assert dvc.params.show(revs=["master"], deps_only=True) == {
        "master": {"data": {"params.yaml": {"data": {"foo": "qux", "xyz": "val"}}}}
    }
    assert dvc.params.show(revs=["master"]) == {
        "master": {
            "data": {
                "params.yaml": {"data": {"abc": "ignore", "foo": "qux", "xyz": "val"}}
            }
        }
    }


def test_show_no_repo(tmp_dir):
    tmp_dir.gen({"foo": "foo", "params_file.yaml": "foo: bar\nxyz: val"})

    dvc = Repo(uninitialized=True)

    assert dvc.params.show(targets=["params_file.yaml"]) == {
        "": {"data": {"params_file.yaml": {"data": {"foo": "bar", "xyz": "val"}}}}
    }


@pytest.mark.parametrize("file", ["params.yaml", "other_params.yaml"])
def test_show_without_targets_specified(tmp_dir, dvc, scm, file):
    params_file = tmp_dir / file
    data = {"foo": {"bar": "bar"}, "x": "0"}
    params_file.dump(data)
    dvc.stage.add(name="test", cmd=f"echo {file}", params=[{file: None}])

    assert dvc.params.show() == {"": {"data": {file: {"data": data}}}}


def test_deps_multi_stage(tmp_dir, scm, dvc, run_copy):
    tmp_dir.gen({"foo": "foo", "params.yaml": "foo: bar\nxyz: val\nabc: ignore"})
    run_copy("foo", "bar", name="copy-foo-bar", params=["foo"])
    run_copy("foo", "bar1", name="copy-foo-bar-1", params=["xyz"])

    scm.add(["params.yaml", PROJECT_FILE])
    scm.commit("add stage")

    assert dvc.params.show(revs=["master"], deps_only=True) == {
        "master": {"data": {"params.yaml": {"data": {"foo": "bar", "xyz": "val"}}}}
    }


def test_deps_with_targets(tmp_dir, scm, dvc, run_copy):
    tmp_dir.gen({"foo": "foo", "params.yaml": "foo: bar\nxyz: val\nabc: ignore"})
    run_copy("foo", "bar", name="copy-foo-bar", params=["foo"])
    run_copy("foo", "bar1", name="copy-foo-bar-1", params=["xyz"])

    scm.add(["params.yaml", PROJECT_FILE])
    scm.commit("add stage")

    assert dvc.params.show(targets=["params.yaml"], deps_only=True) == {
        "": {
            "data": {
                "params.yaml": {"data": {"abc": "ignore", "foo": "bar", "xyz": "val"}}
            }
        }
    }


def test_cached_params(tmp_dir, dvc, scm, remote):
    tmp_dir.dvc_gen(
        {
            "dir": {"params.yaml": "foo: 3\nbar: 10"},
            "dir2": {"params.yaml": "foo: 42\nbar: 4"},
        }
    )
    dvc.push()
    dvc.cache.local.clear()

    (tmp_dir / "dvc.yaml").dump({"params": ["dir/params.yaml", "dir2"]})

    assert dvc.params.show() == {
        "": {
            "data": {
                join("dir", "params.yaml"): {"data": {"foo": 3, "bar": 10}},
                join("dir2", "params.yaml"): {"data": {"foo": 42, "bar": 4}},
            }
        }
    }


def test_top_level_parametrized(tmp_dir, dvc):
    (tmp_dir / "param.json").dump({"foo": 3, "bar": 10})
    (tmp_dir / "params.yaml").dump({"param_file": "param.json"})
    (tmp_dir / "dvc.yaml").dump({"params": ["${param_file}"]})
    assert dvc.params.show() == {
        "": {
            "data": {
                "param.json": {"data": {"foo": 3, "bar": 10}},
                "params.yaml": {"data": {"param_file": "param.json"}},
            }
        }
    }


def test_param_in_a_tracked_directory_with_missing_dir_file(M, tmp_dir, dvc):
    tmp_dir.dvc_gen({"dir": {"file": "2"}})
    (tmp_dir / "dvc.yaml").dump({"params": [join("dir", "file")]})
    shutil.rmtree(tmp_dir / "dir")  # remove from workspace
    dvc.cache.local.clear()  # remove .dir file

    assert dvc.params.show() == {
        "": Result(
            data={
                join("dir", "file"): FileResult(error=M.instance_of(DataIndexDirError)),
            }
        )
    }
