import pytest

from dvc.repo.params.show import NoParamsError


def test_show_empty(dvc):
    with pytest.raises(NoParamsError):
        dvc.params.show()


def test_show(tmp_dir, dvc):
    tmp_dir.gen("params.yaml", "foo: bar")
    dvc.run(params=["foo"])
    assert dvc.params.show() == {"": {"params.yaml": {"foo": "bar"}}}


def test_show_multiple(tmp_dir, dvc):
    tmp_dir.gen("params.yaml", "foo: bar\nbaz: qux\n")
    dvc.run(fname="foo.dvc", params=["foo"])
    dvc.run(fname="baz.dvc", params=["baz"])
    assert dvc.params.show() == {
        "": {"params.yaml": {"foo": "bar", "baz": "qux"}}
    }


def test_show_list(tmp_dir, dvc):
    tmp_dir.gen("params.yaml", "foo:\n- bar\n- baz\n")
    dvc.run(params=["foo"])
    assert dvc.params.show() == {"": {"params.yaml": {"foo": ["bar", "baz"]}}}


def test_show_branch(tmp_dir, scm, dvc):
    tmp_dir.gen("params.yaml", "foo: bar")
    dvc.run(params=["foo"])
    scm.add(["params.yaml", "Dvcfile"])
    scm.commit("init")

    with tmp_dir.branch("branch", new=True):
        tmp_dir.scm_gen("params.yaml", "foo: baz", commit="branch")

    assert dvc.params.show(revs=["branch"]) == {
        "working tree": {"params.yaml": {"foo": "bar"}},
        "branch": {"params.yaml": {"foo": "baz"}},
    }
