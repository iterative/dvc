import pytest
from funcy import first

from dvc.repo.experiments.utils import exp_refs_by_rev, resolve_exp_ref


@pytest.mark.parametrize(
    "full_name, test_remote", [(True, False), (False, True), (False, False)]
)
def test_remove_remote(
    tmp_dir, scm, dvc, exp_stage, git_upstream, full_name, test_remote
):
    remote = None
    results = dvc.experiments.run(exp_stage.addressing, params=["foo=2"])
    exp = first(results)
    ref_info = first(exp_refs_by_rev(scm, exp))
    if test_remote:
        remote = git_upstream.url
        dvc.experiments.push(remote, ref_info.name)

    if full_name:
        exp_name = str(ref_info)
    else:
        exp_name = ref_info.name

    assert resolve_exp_ref(scm, exp_name, remote).name == ref_info.name
