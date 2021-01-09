import os

import pytest

from dvc.utils import relpath
from dvc.utils.serialize import dump_yaml


def test_diff_no_params(tmp_dir, scm, dvc):
    assert dvc.params.diff() == {}


def test_diff_no_changes(tmp_dir, scm, dvc):
    tmp_dir.gen("params.yaml", "foo: bar")
    dvc.run(cmd="echo params.yaml", params=["foo"], single_stage=True)
    scm.add(["params.yaml", "Dvcfile"])
    scm.commit("bar")
    assert dvc.params.diff() == {}


@pytest.mark.parametrize("commit_last", [True, False])
def test_diff_add_similar_files(tmp_dir, scm, dvc, commit_last):
    if commit_last:
        last_commit_msg = "commit #2"
        a_rev = "HEAD~1"
    else:
        last_commit_msg = None
        a_rev = "HEAD"

    tmp_dir.dvc_gen(
        {"dir": {"file": "text1", "subdir": {"file2": "text2"}}},
        commit="commit #1",
    )
    tmp_dir.dvc_gen(
        {"dir2": {"file": "text1", "subdir": {"file2": "text2"}}},
        commit=last_commit_msg,
    )
    assert dvc.diff(a_rev) == {
        "added": [
            {
                "path": os.path.join("dir2", ""),
                "hash": "cb58ee07cb01044db229e4d6121a0dfc.dir",
            },
            {
                "path": os.path.join("dir2", "file"),
                "hash": "cef7ccd89dacf1ced6f5ec91d759953f",
            },
            {
                "path": os.path.join("dir2", "subdir", "file2"),
                "hash": "fe6123a759017e4a2af4a2d19961ed71",
            },
        ],
        "deleted": [],
        "modified": [],
        "renamed": [],
        "not in cache": [],
    }


@pytest.mark.parametrize("commit_last", [True, False])
def test_diff_rename_folder(tmp_dir, scm, dvc, commit_last):
    if commit_last:
        last_commit_msg = "commit #2"
        a_rev = "HEAD~1"
    else:
        last_commit_msg = None
        a_rev = "HEAD"

    tmp_dir.dvc_gen(
        {"dir": {"file": "text1", "subdir": {"file2": "text2"}}},
        commit="commit #1",
    )
    (tmp_dir / "dir").replace(tmp_dir / "dir2")
    tmp_dir.dvc_add("dir2", commit=last_commit_msg)
    assert dvc.diff(a_rev) == {
        "added": [],
        "deleted": [],
        "modified": [],
        "renamed": [
            {
                "path": {
                    "old": os.path.join("dir", ""),
                    "new": os.path.join("dir2", ""),
                },
                "hash": "cb58ee07cb01044db229e4d6121a0dfc.dir",
            },
            {
                "path": {
                    "old": os.path.join("dir", "file"),
                    "new": os.path.join("dir2", "file"),
                },
                "hash": "cef7ccd89dacf1ced6f5ec91d759953f",
            },
            {
                "path": {
                    "old": os.path.join("dir", "subdir", "file2"),
                    "new": os.path.join("dir2", "subdir", "file2"),
                },
                "hash": "fe6123a759017e4a2af4a2d19961ed71",
            },
        ],
        "not in cache": [],
    }


@pytest.mark.parametrize("commit_last", [True, False])
def test_diff_rename_file(tmp_dir, scm, dvc, commit_last):
    if commit_last:
        last_commit_msg = "commit #2"
        a_rev = "HEAD~1"
    else:
        last_commit_msg = None
        a_rev = "HEAD"

    paths = tmp_dir.gen(
        {"dir": {"file": "text1", "subdir": {"file2": "text2"}}}
    )
    tmp_dir.dvc_add(paths, commit="commit #1")
    (tmp_dir / "dir" / "file").replace(tmp_dir / "dir" / "subdir" / "file3")

    tmp_dir.dvc_add(paths, commit=last_commit_msg)
    assert dvc.diff(a_rev) == {
        "added": [],
        "deleted": [],
        "modified": [
            {
                "path": os.path.join("dir", ""),
                "hash": {
                    "old": "cb58ee07cb01044db229e4d6121a0dfc.dir",
                    "new": "a4ac9c339aacc60b6a3152e362c319c8.dir",
                },
            }
        ],
        "renamed": [
            {
                "path": {
                    "old": os.path.join("dir", "file"),
                    "new": os.path.join("dir", "subdir", "file3"),
                },
                "hash": "cef7ccd89dacf1ced6f5ec91d759953f",
            }
        ],
        "not in cache": [],
    }


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


def test_diff_deleted_config(tmp_dir, scm, dvc):
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
    from dvc.repo import Repo
    from dvc.scm.git import Git
    from tests.dir_helpers import git_init

    git_init(".")
    assert Git().no_commits

    assert Repo.init().params.diff() == {}


def test_vars_shows_on_params_diff(tmp_dir, scm, dvc):
    params_file = tmp_dir / "test_params.yaml"
    param_data = {"vars": {"model1": {"epoch": 15}, "model2": {"epoch": 35}}}
    dump_yaml(params_file, param_data)
    d = {
        "vars": ["test_params.yaml"],
        "stages": {
            "build": {
                "foreach": "${vars}",
                "do": {"cmd": "script --epoch ${item.epoch}"},
            }
        },
    }
    dump_yaml("dvc.yaml", d)
    assert dvc.params.diff() == {
        "test_params.yaml": {
            "vars.model1.epoch": {"new": 15, "old": None},
            "vars.model2.epoch": {"new": 35, "old": None},
        }
    }
    scm.add(["dvc.yaml", "test_params.yaml"])
    scm.commit("added stages")

    param_data["vars"]["model1"]["epoch"] = 20
    dump_yaml(params_file, param_data)
    assert dvc.params.diff() == {
        "test_params.yaml": {
            "vars.model1.epoch": {"new": 20, "old": 15, "diff": 5},
        }
    }

    data_dir = tmp_dir / "data"
    data_dir.mkdir()
    with data_dir.chdir():
        assert dvc.params.diff() == {
            relpath(params_file): {
                "vars.model1.epoch": {"new": 20, "old": 15, "diff": 5},
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
        "other_params.yaml": {"xyz": {"old": "val", "new": "val3"}},
    }
