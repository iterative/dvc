import os

import pytest

from dvc.stage.cache import RunCacheNotSupported
from dvc.utils.fs import remove


def test_repro_pulls_missing_data_source(tmp_dir, dvc, mocker, local_remote):
    (foo,) = tmp_dir.dvc_gen("foo", "foo")

    dvc.push()

    dvc.stage.add(name="copy-foo", cmd="cp foo bar", deps=["foo"], outs=["bar"])
    remove("foo")
    remove(foo.outs[0].cache_path)

    assert dvc.reproduce(pull=True)


def test_repro_pulls_missing_import(tmp_dir, dvc, mocker, erepo_dir, local_remote):
    with erepo_dir.chdir():
        erepo_dir.dvc_gen("foo", "foo", commit="first")

    foo_import = dvc.imp(os.fspath(erepo_dir), "foo")

    dvc.push()

    dvc.stage.add(name="copy-foo", cmd="cp foo bar", deps=["foo"], outs=["bar"])
    remove("foo")
    remove(foo_import.outs[0].cache_path)

    assert dvc.reproduce(pull=True)


def test_repro_pulls_continue_without_run_cache(tmp_dir, dvc, mocker, local_remote):
    (foo,) = tmp_dir.dvc_gen("foo", "foo")

    dvc.push()
    mocker.patch.object(
        dvc.stage_cache, "pull", side_effect=RunCacheNotSupported("foo")
    )
    dvc.stage.add(name="copy-foo", cmd="cp foo bar", deps=["foo"], outs=["bar"])
    remove("foo")
    remove(foo.outs[0].cache_path)

    assert dvc.reproduce(pull=True)


def test_repro_skip_pull_if_no_run_cache_is_passed(tmp_dir, dvc, mocker, local_remote):
    (foo,) = tmp_dir.dvc_gen("foo", "foo")

    dvc.push()
    dvc.stage.add(name="copy-foo", cmd="cp foo bar", deps=["foo"], outs=["bar"])

    assert dvc.reproduce(pull=True)

    remove("foo")
    remove(foo.outs[0].cache_path)
    remove("dvc.lock")
    spy_pull = mocker.spy(dvc.stage_cache, "pull")
    assert dvc.reproduce(pull=True, run_cache=False)
    assert not spy_pull.called


def test_repro_skip_pull_if_single_item_is_passed(tmp_dir, dvc, mocker, local_remote):
    (foo,) = tmp_dir.dvc_gen("foo", "foo")

    dvc.push()

    dvc.stage.add(name="copy-foo", cmd="cp foo bar", deps=["foo"], outs=["bar"])
    remove("foo")
    remove(foo.outs[0].cache_path)

    assert dvc.reproduce(pull=True, single_item=True)


def test_repro_pulls_persisted_output(tmp_dir, dvc, mocker, local_remote):
    tmp_dir.dvc_gen("foo", "foo")

    dvc.push()

    dvc.stage.add(name="copy-foo", cmd="cp foo bar", deps=["foo"], outs_persist=["bar"])
    dvc.reproduce()
    remove("bar")

    # stage is skipped
    assert not dvc.reproduce(pull=True)


@pytest.mark.parametrize("allow_missing", [True, False])
def test_repro_pulls_allow_missing(tmp_dir, dvc, mocker, local_remote, allow_missing):
    tmp_dir.dvc_gen("foo", "foo")

    dvc.push()

    dvc.stage.add(name="copy-foo", cmd="cp foo bar", deps=["foo"], outs=["bar"])
    dvc.reproduce()
    remove("foo")

    # stage is skipped
    assert not dvc.reproduce(pull=True, allow_missing=allow_missing)
    # data only pulled if allow_missing is false
    assert (tmp_dir / "foo").exists() != allow_missing


def test_repro_pull_fails(tmp_dir, dvc, mocker, local_remote):
    tmp_dir.dvc_gen("foo", "foo")

    dvc.stage.add(
        name="concat-foo", cmd="cat foo foo > bar", deps=["foo"], outs=["bar"]
    )
    stages = dvc.reproduce()
    remove("bar")
    remove(stages[0].outs[0].cache_path)

    # stage is run
    assert dvc.reproduce(pull=True)
