import pytest

from dvc.repo.experiments.base import EXPS_NAMESPACE, ExpRefInfo
from dvc.repo.experiments.utils import resolve_exp_ref


def commit_exp_ref(tmp_dir, scm, file="foo", contents="foo", name="foo"):
    tmp_dir.scm_gen(file, contents, commit="init")
    rev = scm.get_rev()
    ref = "/".join([EXPS_NAMESPACE, "ab", "c123", name])
    scm.gitpython.set_ref(ref, rev)
    return ref, rev


@pytest.mark.parametrize("use_url", [True, False])
@pytest.mark.parametrize("name_only", [True, False])
def test_resolve_exp_ref(tmp_dir, scm, git_upstream, name_only, use_url):
    ref, _ = commit_exp_ref(tmp_dir, scm)
    ref_info = resolve_exp_ref(scm, "foo" if name_only else ref)
    assert isinstance(ref_info, ExpRefInfo)
    assert str(ref_info) == ref

    scm.push_refspec(git_upstream.url, ref, ref)
    remote = git_upstream.url if use_url else git_upstream.remote
    remote_ref_info = resolve_exp_ref(scm, "foo" if name_only else ref, remote)
    assert isinstance(remote_ref_info, ExpRefInfo)
    assert str(remote_ref_info) == ref
