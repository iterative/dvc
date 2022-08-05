from datetime import datetime

import pytest
from scmrepo.exceptions import SCMError

from dvc.repo.experiments import ExpRefInfo
from dvc.scm import GitMergeError, iter_revs


@pytest.fixture
def setup_revs(tmp_dir, scm):
    """
    new         other
     │            │
    old (tag) ────┘
     │
    root
    """
    old = scm.active_branch()
    tmp_dir.scm_gen("foo", "init", commit="init")
    rev_root = scm.get_rev()
    tmp_dir.scm_gen("foo", "old", commit="old")
    rev_old = scm.get_rev()
    scm.checkout("new", create_new=True)
    tmp_dir.scm_gen("foo", "new", commit="new")
    rev_new = scm.get_rev()
    scm.checkout(old)
    scm.tag("tag")
    scm.checkout("other", create_new=True)
    tmp_dir.scm_gen("foo", "other", commit="new")
    rev_other = scm.get_rev()

    ref = ExpRefInfo(rev_root, "exp1")
    scm.set_ref(str(ref), rev_new)
    ref = ExpRefInfo(rev_root, "exp2")
    scm.set_ref(str(ref), rev_old)

    return old, rev_root, rev_old, rev_new, rev_other


def test_iter_revs_rev_num(scm, setup_revs):
    _, rev_root, rev_old, rev_new, rev_other = setup_revs

    gen = iter_revs(scm, [rev_root, "new"], 1)
    assert gen == {rev_root: [rev_root], rev_new: ["new"]}
    gen = iter_revs(scm, ["new"], 2)
    assert gen == {rev_new: ["new"], rev_old: [rev_old]}
    gen = iter_revs(scm, ["other"], -1)
    assert gen == {
        rev_other: ["other"],
        rev_old: [rev_old],
        rev_root: [rev_root],
    }
    gen = iter_revs(scm, ["tag"])
    assert gen == {rev_old: ["tag"]}


def test_iter_revs_all(scm, setup_revs):
    old, rev_root, rev_old, rev_new, rev_other = setup_revs

    gen = iter_revs(scm, all_branches=True)
    assert gen == {rev_old: [old], rev_new: ["new"], rev_other: ["other"]}
    gen = iter_revs(scm, all_tags=True)
    assert gen == {rev_old: ["tag"]}
    gen = iter_revs(scm, all_commits=True)
    assert gen == {
        rev_old: [rev_old],
        rev_new: [rev_new],
        rev_other: [rev_other],
        rev_root: [rev_root],
    }
    gen = iter_revs(scm, all_experiments=True)
    assert gen == {
        rev_new: [rev_new],
        rev_old: [rev_old],
        rev_root: [rev_root],
    }


def test_iter_revs_commit_date(scm, setup_revs, mocker):
    _, rev_root, rev_old, rev_new, rev_other = setup_revs

    def _func(rev):
        if rev == rev_root:
            return mocker.Mock(commit_date=datetime(2022, 6, 28).timestamp())
        if rev == rev_old:
            raise SCMError
        return mocker.Mock(commit_date=datetime(2022, 6, 30).timestamp())

    mocker.patch(
        "scmrepo.git.Git.resolve_commit", mocker.MagicMock(side_effect=_func)
    )

    gen = iter_revs(scm, commit_date="2022-06-29")
    assert gen == {
        rev_new: [rev_new],
        rev_old: [rev_old],
        rev_other: [rev_other],
    }


def test_iter_revs_ignore(scm, setup_revs):
    _, rev_root, rev_old, rev_new, rev_other = setup_revs

    gen = iter_revs(scm, all_commits=True, ignore_revs={rev_old, rev_new})
    assert gen == {rev_other: [rev_other], rev_root: [rev_root]}
    gen = iter_revs(scm, all_branches=True, ignore_revs={rev_old, rev_new})
    assert gen == {rev_other: ["other"]}
    gen = iter_revs(scm, all_tags=True, ignore_revs={rev_old, rev_new})
    assert gen == {}
    gen = iter_revs(scm, all_experiments=True, ignore_revs={rev_old, rev_new})
    assert gen == {rev_root: [rev_root]}


def test_merge_error(tmp_dir, scm):
    exc = GitMergeError("Merge failed")
    assert "shallow" not in str(exc)
    tmp_dir.gen({".git": {"shallow": ""}})
    exc = GitMergeError("Merge failed", scm=scm)
    assert "shallow" in str(exc)
