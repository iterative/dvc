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
