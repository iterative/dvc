import pytest

from dvc.exceptions import InvalidArgumentError
from dvc.repo.experiments.base import EXPS_NAMESPACE, ExpRefInfo
from dvc.repo.experiments.utils import check_ref_format, resolve_name


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
    name = "foo" if name_only else ref
    result = resolve_name(scm, [name, "notexist"])
    assert isinstance(result[name], ExpRefInfo)
    assert str(result[name]) == ref
    assert result["notexist"] is None

    scm.push_refspecs(git_upstream.url, f"{ref}:{ref}")
    remote = git_upstream.url if use_url else git_upstream.remote
    name = "foo" if name_only else ref
    remote_ref_info = resolve_name(scm, [name], remote)[name]
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
