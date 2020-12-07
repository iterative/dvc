import pytest
from funcy import first

from dvc.exceptions import DvcException
from dvc.repo.experiments.utils import exp_refs_by_rev


@pytest.fixture
def git_upstream(tmp_dir, scm, make_tmp_dir):
    name = "git-upstream"
    remote_dir = make_tmp_dir(name, scm=True)
    url = "file://{}".format(remote_dir.resolve().as_posix())
    tmp_dir.scm.gitpython.repo.create_remote("upstream", url)
    remote_dir.remote = "upstream"
    remote_dir.url = url
    return remote_dir


@pytest.fixture
def git_downstream(tmp_dir, scm, dvc, make_tmp_dir):
    name = "git-downstream"
    remote_dir = make_tmp_dir(name, scm=True, dvc=True)
    url = "file://{}".format(tmp_dir.resolve().as_posix())
    remote_dir.scm.gitpython.repo.create_remote("upstream", url)
    remote_dir.remote = "upstream"
    remote_dir.url = url
    return remote_dir


@pytest.mark.parametrize("use_url", [True, False])
def test_push(tmp_dir, scm, dvc, git_upstream, exp_stage, use_url):
    from dvc.exceptions import InvalidArgumentError

    remote = git_upstream.url if use_url else git_upstream.remote
    with pytest.raises(InvalidArgumentError):
        dvc.experiments.push(remote, "foo")

    results = dvc.experiments.run(exp_stage.addressing, params=["foo=2"])
    exp = first(results)
    ref_info = first(exp_refs_by_rev(scm, exp))

    dvc.experiments.push(remote, ref_info.name)
    assert git_upstream.scm.get_ref(str(ref_info)) == exp

    git_upstream.scm.remove_ref(str(ref_info))

    dvc.experiments.push(remote, str(ref_info))
    assert git_upstream.scm.get_ref(str(ref_info)) == exp


def test_push_diverged(tmp_dir, scm, dvc, git_upstream, exp_stage):
    git_upstream.scm_gen("foo", "foo", commit="init")
    remote_rev = git_upstream.scm.get_rev()

    results = dvc.experiments.run(exp_stage.addressing, params=["foo=2"])
    exp = first(results)
    ref_info = first(exp_refs_by_rev(scm, exp))

    git_upstream.scm.set_ref(str(ref_info), remote_rev)

    with pytest.raises(DvcException):
        dvc.experiments.push(git_upstream.remote, ref_info.name)
    assert git_upstream.scm.get_ref(str(ref_info)) == remote_rev

    dvc.experiments.push(git_upstream.remote, ref_info.name, force=True)
    assert git_upstream.scm.get_ref(str(ref_info)) == exp


def test_push_checkpoint(tmp_dir, scm, dvc, git_upstream, checkpoint_stage):
    results = dvc.experiments.run(
        checkpoint_stage.addressing, params=["foo=2"]
    )
    exp_a = first(results)
    ref_info_a = first(exp_refs_by_rev(scm, exp_a))

    dvc.experiments.push(git_upstream.remote, ref_info_a.name, force=True)
    assert git_upstream.scm.get_ref(str(ref_info_a)) == exp_a

    results = dvc.experiments.run(
        checkpoint_stage.addressing, checkpoint_resume=exp_a
    )
    exp_b = first(results)
    ref_info_b = first(exp_refs_by_rev(scm, exp_b))

    dvc.experiments.push(git_upstream.remote, ref_info_b.name, force=True)
    assert git_upstream.scm.get_ref(str(ref_info_b)) == exp_b


@pytest.mark.parametrize("use_url", [True, False])
def test_list_remote(tmp_dir, scm, dvc, git_downstream, exp_stage, use_url):
    baseline_a = scm.get_rev()
    results = dvc.experiments.run(exp_stage.addressing, params=["foo=2"])
    exp_a = first(results)
    ref_info_a = first(exp_refs_by_rev(scm, exp_a))

    results = dvc.experiments.run(exp_stage.addressing, params=["foo=3"])
    exp_b = first(results)
    ref_info_b = first(exp_refs_by_rev(scm, exp_b))

    tmp_dir.scm_gen("new", "new", commit="new")
    baseline_c = scm.get_rev()
    results = dvc.experiments.run(exp_stage.addressing, params=["foo=4"])
    exp_c = first(results)
    ref_info_c = first(exp_refs_by_rev(scm, exp_c))

    remote = git_downstream.url if use_url else git_downstream.remote

    assert git_downstream.scm.get_ref("HEAD") != scm.get_ref("HEAD")
    downstream_exp = git_downstream.dvc.experiments
    assert downstream_exp.list(git_upstream=remote) == {}

    exp_list = downstream_exp.list(rev=baseline_a, git_upstream=remote)
    assert {key: set(val) for key, val in exp_list.items()} == {
        baseline_a: {ref_info_a.name, ref_info_b.name}
    }

    exp_list = downstream_exp.list(all_=True, git_upstream=remote)
    assert {key: set(val) for key, val in exp_list.items()} == {
        baseline_a: {ref_info_a.name, ref_info_b.name},
        baseline_c: {ref_info_c.name},
    }
