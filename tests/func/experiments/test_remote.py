import pytest
from funcy import first

from dvc.exceptions import DvcException
from dvc.repo.experiments.utils import exp_refs_by_rev


@pytest.fixture
def git_remote(tmp_dir, scm, make_tmp_dir):
    name = "git-remote"
    remote_dir = make_tmp_dir(name, scm=True)
    url = "file://{}".format(remote_dir.resolve().as_posix())
    tmp_dir.scm.gitpython.repo.create_remote(name, url)
    remote_dir.remote = name
    remote_dir.url = url
    return remote_dir


@pytest.mark.parametrize("use_url", [True, False])
def test_push(tmp_dir, scm, dvc, git_remote, exp_stage, use_url):
    from dvc.exceptions import InvalidArgumentError

    remote = git_remote.url if use_url else git_remote.remote
    with pytest.raises(InvalidArgumentError):
        dvc.experiments.push(remote, "foo")

    results = dvc.experiments.run(exp_stage.addressing, params=["foo=2"])
    exp = first(results)
    ref_info = first(exp_refs_by_rev(scm, exp))

    dvc.experiments.push(remote, ref_info.name)
    assert git_remote.scm.get_ref(str(ref_info)) == exp


def test_push_diverged(tmp_dir, scm, dvc, git_remote, exp_stage):
    git_remote.scm_gen("foo", "foo", commit="init")
    remote_rev = git_remote.scm.get_rev()

    results = dvc.experiments.run(exp_stage.addressing, params=["foo=2"])
    exp = first(results)
    ref_info = first(exp_refs_by_rev(scm, exp))

    git_remote.scm.set_ref(str(ref_info), remote_rev)

    with pytest.raises(DvcException):
        dvc.experiments.push(git_remote.remote, ref_info.name)
    assert git_remote.scm.get_ref(str(ref_info)) == remote_rev

    dvc.experiments.push(git_remote.remote, ref_info.name, force=True)
    assert git_remote.scm.get_ref(str(ref_info)) == exp


def test_push_checkpoint(tmp_dir, scm, dvc, git_remote, checkpoint_stage):
    results = dvc.experiments.run(
        checkpoint_stage.addressing, params=["foo=2"]
    )
    exp_a = first(results)
    ref_info_a = first(exp_refs_by_rev(scm, exp_a))

    dvc.experiments.push(git_remote.remote, ref_info_a.name, force=True)
    assert git_remote.scm.get_ref(str(ref_info_a)) == exp_a

    results = dvc.experiments.run(
        checkpoint_stage.addressing, checkpoint_resume=exp_a
    )
    exp_b = first(results)
    ref_info_b = first(exp_refs_by_rev(scm, exp_b))

    dvc.experiments.push(git_remote.remote, ref_info_b.name, force=True)
    assert git_remote.scm.get_ref(str(ref_info_b)) == exp_b


@pytest.mark.parametrize("use_url", [True, False])
def test_list_remote(tmp_dir, scm, dvc, git_remote, exp_stage, use_url):
    from dvc.repo.experiments.utils import exp_refs

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

    for info in exp_refs(scm):
        dvc.experiments.push(git_remote.remote, str(info))

    remote = git_remote.url if use_url else git_remote.remote
    assert dvc.experiments.list(git_remote=remote) == {
        baseline_c: [ref_info_c.name],
    }

    exp_list = dvc.experiments.list(rev=baseline_a, git_remote=remote)
    assert {key: set(val) for key, val in exp_list.items()} == {
        baseline_a: {ref_info_a.name, ref_info_b.name}
    }

    exp_list = dvc.experiments.list(all_=True, git_remote=remote)
    assert {key: set(val) for key, val in exp_list.items()} == {
        baseline_a: {ref_info_a.name, ref_info_b.name},
        baseline_c: {ref_info_c.name},
    }
