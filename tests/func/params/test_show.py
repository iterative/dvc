import operator
from functools import reduce

import pytest

from dvc.repo import Repo
from dvc.repo.stage import PROJECT_FILE
from dvc.utils.serialize import YAMLFileCorruptedError


def test_show_empty(dvc):
    assert dvc.params.show() == {}


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

    assert dvc.params.show(revs=["master"], deps=True) == {
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


@pytest.mark.parametrize(
    "file,error_path",
    (
        (PROJECT_FILE, ["v1", "error"]),
        ("params_other.yaml", ["v1", "data", "params_other.yaml", "error"]),
    ),
)
def test_log_errors(tmp_dir, scm, dvc, capsys, file, error_path):
    tmp_dir.gen("params_other.yaml", "foo: bar")
    dvc.run(
        cmd="echo params_other.yaml",
        params=["params_other.yaml:foo"],
        name="train",
    )

    rename = (tmp_dir / file).read_text()
    with open(tmp_dir / file, "a", encoding="utf-8") as fd:
        fd.write("\nmalformed!")

    scm.add([PROJECT_FILE, "params_other.yaml"])
    scm.commit("init")
    scm.tag("v1")

    (tmp_dir / file).write_text(rename)

    result = dvc.params.show(revs=["v1"])

    _, error = capsys.readouterr()

    assert isinstance(
        reduce(operator.getitem, error_path, result), YAMLFileCorruptedError
    )
    assert "DVC failed to load some parameters for following revisions: 'v1'." in error


@pytest.mark.parametrize("file", ["params.yaml", "other_params.yaml"])
def test_show_without_targets_specified(tmp_dir, dvc, scm, file):
    params_file = tmp_dir / file
    data = {"foo": {"bar": "bar"}, "x": "0"}
    params_file.dump(data)
    dvc.stage.add(
        name="test",
        cmd=f"echo {file}",
        params=[{file: None}],
    )

    assert dvc.params.show() == {"": {"data": {file: {"data": data}}}}


def test_deps_multi_stage(tmp_dir, scm, dvc, run_copy):
    tmp_dir.gen({"foo": "foo", "params.yaml": "foo: bar\nxyz: val\nabc: ignore"})
    run_copy("foo", "bar", name="copy-foo-bar", params=["foo"])
    run_copy("foo", "bar1", name="copy-foo-bar-1", params=["xyz"])

    scm.add(["params.yaml", PROJECT_FILE])
    scm.commit("add stage")

    assert dvc.params.show(revs=["master"], deps=True) == {
        "master": {"data": {"params.yaml": {"data": {"foo": "bar", "xyz": "val"}}}}
    }


def test_deps_with_targets(tmp_dir, scm, dvc, run_copy):
    tmp_dir.gen({"foo": "foo", "params.yaml": "foo: bar\nxyz: val\nabc: ignore"})
    run_copy("foo", "bar", name="copy-foo-bar", params=["foo"])
    run_copy("foo", "bar1", name="copy-foo-bar-1", params=["xyz"])

    scm.add(["params.yaml", PROJECT_FILE])
    scm.commit("add stage")

    assert dvc.params.show(targets=["params.yaml"], deps=True) == {
        "": {"data": {"params.yaml": {"data": {"foo": "bar", "xyz": "val"}}}}
    }


def test_deps_with_bad_target(tmp_dir, scm, dvc, run_copy):
    tmp_dir.gen(
        {
            "foo": "foo",
            "foobar": "",
            "params.yaml": "foo: bar\nxyz: val\nabc: ignore",
        }
    )
    run_copy("foo", "bar", name="copy-foo-bar", params=["foo"])
    run_copy("foo", "bar1", name="copy-foo-bar-1", params=["xyz"])
    scm.add(["params.yaml", PROJECT_FILE])
    scm.commit("add stage")
    assert dvc.params.show(targets=["foobar"], deps=True) == {}
