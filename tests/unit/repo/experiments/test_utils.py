import pytest

from dvc.exceptions import InvalidArgumentError
from dvc.repo.experiments.base import ExpRefInfo
from dvc.repo.experiments.utils import (
    check_ref_format,
    get_exp_ref_from_variables,
    resolve_exp_ref,
)


def commit_exp_ref(tmp_dir, scm, file="foo", contents="foo", name="foo"):
    baseline_rev = scm.get_rev()
    tmp_dir.scm_gen(file, contents, commit="init")
    rev = scm.get_rev()
    ref_info = ExpRefInfo(baseline_rev, name)
    ref = str(ref_info)
    scm.gitpython.set_ref(ref, rev)
    scm.checkout(baseline_rev)
    return ref, rev


@pytest.mark.parametrize("use_url", [True, False])
@pytest.mark.parametrize("name_only", [True, False])
def test_resolve_exp_ref(tmp_dir, scm, git_upstream, name_only, use_url):
    tmp_dir.scm_gen("foo", "init", commit="init")
    ref, _ = commit_exp_ref(tmp_dir, scm)
    ref_info = resolve_exp_ref(scm, "foo" if name_only else ref)
    assert isinstance(ref_info, ExpRefInfo)
    assert str(ref_info) == ref

    scm.push_refspec(git_upstream.url, ref, ref)
    remote = git_upstream.url if use_url else git_upstream.remote
    remote_ref_info = resolve_exp_ref(scm, "foo" if name_only else ref, remote)
    assert isinstance(remote_ref_info, ExpRefInfo)
    assert str(remote_ref_info) == ref


@pytest.mark.parametrize(
    "name,result", [("name", True), ("group/name", False), ("na me", False)]
)
def test_run_check_ref_format(scm, name, result):

    ref = ExpRefInfo("abc123", name)
    if result:
        check_ref_format(scm, ref)
    else:
        with pytest.raises(InvalidArgumentError):
            check_ref_format(scm, ref)


@pytest.mark.parametrize("git_remote", [True, None])
@pytest.mark.parametrize(
    "all_,rev,result",
    [
        (True, None, {"exp1", "exp2", "exp3", "exp4"}),
        (None, True, {"exp1", "exp2"}),
    ],
)
def test_get_exp_ref_from_variables(
    tmp_dir,
    scm,
    git_upstream,
    git_remote,
    all_,
    rev,
    result,
):
    tmp_dir.scm_gen("foo", "init", commit="init")
    baseline1 = scm.get_rev()
    ref_list = []
    ref, _ = commit_exp_ref(tmp_dir, scm, contents="1", name="exp1")
    ref_list.append(ref)
    ref, _ = commit_exp_ref(tmp_dir, scm, contents="2", name="exp2")
    ref_list.append(ref)
    tmp_dir.scm_gen("foo", "init2", commit="init")
    _ = scm.get_rev()
    ref, _ = commit_exp_ref(tmp_dir, scm, contents="3", name="exp3")
    ref_list.append(ref)
    ref, _ = commit_exp_ref(tmp_dir, scm, contents="4", name="exp4")
    ref_list.append(ref)

    if rev:
        rev = baseline1[:7]
    if git_remote:
        git_remote = git_upstream.url
        for ref in ref_list:
            scm.push_refspec(git_upstream.url, ref, ref)

    gen = get_exp_ref_from_variables(scm, rev, all_, git_remote)

    assert {exp_ref.name for exp_ref in gen} == result
