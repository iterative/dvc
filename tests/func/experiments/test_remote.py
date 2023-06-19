import pytest
from funcy import first

from dvc.repo.experiments.utils import exp_refs_by_rev


@pytest.mark.parametrize("use_url", [True, False])
def test_push(tmp_dir, scm, dvc, git_upstream, exp_stage, use_url):
    from dvc.exceptions import InvalidArgumentError

    remote = git_upstream.url if use_url else git_upstream.remote
    with pytest.raises(InvalidArgumentError):
        dvc.experiments.push(remote, ["foo"])

    results = dvc.experiments.run(exp_stage.addressing, params=["foo=1"])
    exp1 = first(results)
    ref_info1 = first(exp_refs_by_rev(scm, exp1))

    results = dvc.experiments.run(exp_stage.addressing, params=["foo=2"])
    exp2 = first(results)
    ref_info2 = first(exp_refs_by_rev(scm, exp2))

    results = dvc.experiments.run(exp_stage.addressing, params=["foo=3"])
    exp3 = first(results)
    ref_info3 = first(exp_refs_by_rev(scm, exp3))

    dvc.experiments.push(remote, [ref_info1.name, ref_info2.name])
    assert git_upstream.tmp_dir.scm.get_ref(str(ref_info1)) == exp1
    assert git_upstream.tmp_dir.scm.get_ref(str(ref_info2)) == exp2
    assert git_upstream.tmp_dir.scm.get_ref(str(ref_info3)) is None

    git_upstream.tmp_dir.scm.remove_ref(str(ref_info1))
    assert git_upstream.tmp_dir.scm.get_ref(str(ref_info1)) is None

    dvc.experiments.push(remote, [ref_info1.name])
    assert git_upstream.tmp_dir.scm.get_ref(str(ref_info1)) == exp1

    dvc.experiments.push(remote)
    assert git_upstream.tmp_dir.scm.get_ref(str(ref_info3)) == exp3


@pytest.mark.parametrize("all_,rev,result3", [(True, False, True), (False, True, None)])
def test_push_args(tmp_dir, scm, dvc, git_upstream, exp_stage, all_, rev, result3):
    remote = git_upstream.url
    baseline = scm.get_rev()

    results = dvc.experiments.run(exp_stage.addressing, params=["foo=1"])
    exp1 = first(results)
    ref_info1 = first(exp_refs_by_rev(scm, exp1))
    results = dvc.experiments.run(exp_stage.addressing, params=["foo=2"])
    exp2 = first(results)
    ref_info2 = first(exp_refs_by_rev(scm, exp2))

    scm.commit("new_baseline")

    results = dvc.experiments.run(exp_stage.addressing, params=["foo=3"])
    exp3 = first(results)
    ref_info3 = first(exp_refs_by_rev(scm, exp3))

    if rev:
        rev = baseline
    dvc.experiments.push(remote, [], all_commits=all_, rev=rev)
    assert git_upstream.tmp_dir.scm.get_ref(str(ref_info1)) == exp1
    assert git_upstream.tmp_dir.scm.get_ref(str(ref_info2)) == exp2
    if result3:
        result3 = exp3
    assert git_upstream.tmp_dir.scm.get_ref(str(ref_info3)) == result3


def test_push_multi_rev(tmp_dir, scm, dvc, git_upstream, exp_stage):
    remote = git_upstream.url
    baseline = scm.get_rev()

    results = dvc.experiments.run(exp_stage.addressing, params=["foo=1"])
    exp1 = first(results)
    ref_info1 = first(exp_refs_by_rev(scm, exp1))
    results = dvc.experiments.run(exp_stage.addressing, params=["foo=2"])
    exp2 = first(results)
    ref_info2 = first(exp_refs_by_rev(scm, exp2))

    scm.commit("new_baseline")

    results = dvc.experiments.run(exp_stage.addressing, params=["foo=3"])
    exp3 = first(results)
    ref_info3 = first(exp_refs_by_rev(scm, exp3))

    dvc.experiments.push(remote, [], rev=[baseline, scm.get_rev()])
    assert git_upstream.tmp_dir.scm.get_ref(str(ref_info1)) == exp1
    assert git_upstream.tmp_dir.scm.get_ref(str(ref_info2)) == exp2
    assert git_upstream.tmp_dir.scm.get_ref(str(ref_info3)) == exp3


def test_push_diverged(tmp_dir, scm, dvc, git_upstream, exp_stage):
    git_upstream.tmp_dir.scm_gen("foo", "foo", commit="init")
    remote_rev = git_upstream.tmp_dir.scm.get_rev()

    results = dvc.experiments.run(exp_stage.addressing, params=["foo=2"])
    exp = first(results)
    ref_info = first(exp_refs_by_rev(scm, exp))

    git_upstream.tmp_dir.scm.set_ref(str(ref_info), remote_rev)

    assert dvc.experiments.push(git_upstream.remote, [ref_info.name]) == {
        "diverged": [ref_info.name],
        "url": None,
        "uploaded": 0,
    }
    assert git_upstream.tmp_dir.scm.get_ref(str(ref_info)) == remote_rev

    dvc.experiments.push(git_upstream.remote, [ref_info.name], force=True)
    assert git_upstream.tmp_dir.scm.get_ref(str(ref_info)) == exp


def test_push_ambiguous_name(tmp_dir, scm, dvc, git_upstream, exp_stage):
    from dvc.exceptions import InvalidArgumentError

    remote = git_upstream.remote

    results = dvc.experiments.run(exp_stage.addressing, params=["foo=2"], name="foo")
    exp_a = first(results)
    ref_info_a = first(exp_refs_by_rev(scm, exp_a))

    tmp_dir.scm_gen("new", "new", commit="new")
    results = dvc.experiments.run(exp_stage.addressing, params=["foo=3"], name="foo")
    exp_b = first(results)
    ref_info_b = first(exp_refs_by_rev(scm, exp_b))

    dvc.experiments.push(remote, ["foo"])
    assert git_upstream.tmp_dir.scm.get_ref(str(ref_info_b)) == exp_b

    tmp_dir.scm_gen("new", "new 2", commit="new 2")

    with pytest.raises(InvalidArgumentError):
        dvc.experiments.push(remote, ["foo"])

    dvc.experiments.push(remote, [str(ref_info_a)])
    assert git_upstream.tmp_dir.scm.get_ref(str(ref_info_a)) == exp_a


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
    results = dvc.experiments.run(exp_stage.addressing, params=["foo=4"])
    exp_c = first(results)
    ref_info_c = first(exp_refs_by_rev(scm, exp_c))

    remote = git_downstream.url if use_url else git_downstream.remote

    assert git_downstream.tmp_dir.scm.get_ref("HEAD") != scm.get_ref("HEAD")
    downstream_exp = git_downstream.tmp_dir.dvc.experiments
    assert downstream_exp.ls(git_remote=remote) == {}

    git_downstream.tmp_dir.scm.fetch_refspecs(remote, ["master:master"])
    exp_list = downstream_exp.ls(rev=baseline_a, git_remote=remote)
    assert {key: set(val) for key, val in exp_list.items()} == {
        baseline_a: {(ref_info_a.name, None), (ref_info_b.name, None)}
    }

    exp_list = downstream_exp.ls(all_commits=True, git_remote=remote)
    assert {key: set(val) for key, val in exp_list.items()} == {
        baseline_a: {(ref_info_a.name, None), (ref_info_b.name, None)},
        "refs/heads/master": {(ref_info_c.name, None)},
    }


@pytest.mark.parametrize("use_url", [True, False])
def test_pull(tmp_dir, scm, dvc, git_downstream, exp_stage, use_url):
    from dvc.exceptions import InvalidArgumentError

    # fetch and checkout to downstream so both repos start from same commit
    downstream_repo = git_downstream.tmp_dir.scm.gitpython.repo
    fetched = downstream_repo.remote(git_downstream.remote).fetch()
    downstream_repo.git.checkout(fetched)

    remote = git_downstream.url if use_url else git_downstream.remote
    downstream_exp = git_downstream.tmp_dir.dvc.experiments
    with pytest.raises(InvalidArgumentError):
        downstream_exp.pull(remote, ["foo"])

    results = dvc.experiments.run(exp_stage.addressing, params=["foo=1"])
    exp1 = first(results)
    ref_info1 = first(exp_refs_by_rev(scm, exp1))
    results = dvc.experiments.run(exp_stage.addressing, params=["foo=2"])
    exp2 = first(results)
    ref_info2 = first(exp_refs_by_rev(scm, exp2))
    results = dvc.experiments.run(exp_stage.addressing, params=["foo=3"])
    exp3 = first(results)
    ref_info3 = first(exp_refs_by_rev(scm, exp3))

    downstream_exp.pull(
        git_downstream.remote, [ref_info1.name, ref_info2.name], force=True
    )
    assert git_downstream.tmp_dir.scm.get_ref(str(ref_info1)) == exp1
    assert git_downstream.tmp_dir.scm.get_ref(str(ref_info2)) == exp2
    assert git_downstream.tmp_dir.scm.get_ref(str(ref_info3)) is None

    git_downstream.tmp_dir.scm.remove_ref(str(ref_info1))

    downstream_exp.pull(remote, [str(ref_info1)])
    assert git_downstream.tmp_dir.scm.get_ref(str(ref_info1)) == exp1

    downstream_exp.pull(remote)
    assert git_downstream.tmp_dir.scm.get_ref(str(ref_info3)) == exp3


@pytest.mark.parametrize("all_,rev,result3", [(True, False, True), (False, True, None)])
def test_pull_args(tmp_dir, scm, dvc, git_downstream, exp_stage, all_, rev, result3):
    baseline = scm.get_rev()

    results = dvc.experiments.run(exp_stage.addressing, params=["foo=1"])
    exp1 = first(results)
    ref_info1 = first(exp_refs_by_rev(scm, exp1))
    results = dvc.experiments.run(exp_stage.addressing, params=["foo=2"])
    exp2 = first(results)
    ref_info2 = first(exp_refs_by_rev(scm, exp2))

    scm.commit("new_baseline")

    results = dvc.experiments.run(exp_stage.addressing, params=["foo=3"])
    exp3 = first(results)
    ref_info3 = first(exp_refs_by_rev(scm, exp3))

    if rev:
        rev = baseline

    downstream_exp = git_downstream.tmp_dir.dvc.experiments
    git_downstream.tmp_dir.scm.fetch_refspecs(str(tmp_dir), ["master:master"])
    downstream_exp.pull(git_downstream.remote, [], all_commits=all_, rev=rev)
    assert git_downstream.tmp_dir.scm.get_ref(str(ref_info1)) == exp1
    assert git_downstream.tmp_dir.scm.get_ref(str(ref_info2)) == exp2
    if result3:
        result3 = exp3
    assert git_downstream.tmp_dir.scm.get_ref(str(ref_info3)) == result3


def test_pull_multi_rev(tmp_dir, scm, dvc, git_downstream, exp_stage):
    baseline = scm.get_rev()

    results = dvc.experiments.run(exp_stage.addressing, params=["foo=1"])
    exp1 = first(results)
    ref_info1 = first(exp_refs_by_rev(scm, exp1))
    results = dvc.experiments.run(exp_stage.addressing, params=["foo=2"])
    exp2 = first(results)
    ref_info2 = first(exp_refs_by_rev(scm, exp2))

    scm.commit("new_baseline")

    results = dvc.experiments.run(exp_stage.addressing, params=["foo=3"])
    exp3 = first(results)
    ref_info3 = first(exp_refs_by_rev(scm, exp3))

    downstream_exp = git_downstream.tmp_dir.dvc.experiments
    git_downstream.tmp_dir.scm.fetch_refspecs(str(tmp_dir), ["master:master"])
    downstream_exp.pull(git_downstream.remote, [], rev=[baseline, scm.get_rev()])
    assert git_downstream.tmp_dir.scm.get_ref(str(ref_info1)) == exp1
    assert git_downstream.tmp_dir.scm.get_ref(str(ref_info2)) == exp2
    assert git_downstream.tmp_dir.scm.get_ref(str(ref_info3)) == exp3


def test_pull_diverged(tmp_dir, scm, dvc, git_downstream, exp_stage):
    git_downstream.tmp_dir.scm_gen("foo", "foo", commit="init")
    remote_rev = git_downstream.tmp_dir.scm.get_rev()

    results = dvc.experiments.run(exp_stage.addressing, params=["foo=2"])
    exp = first(results)
    ref_info = first(exp_refs_by_rev(scm, exp))

    git_downstream.tmp_dir.scm.set_ref(str(ref_info), remote_rev)

    downstream_exp = git_downstream.tmp_dir.dvc.experiments
    assert downstream_exp.pull(git_downstream.remote, ref_info.name) == []
    assert git_downstream.tmp_dir.scm.get_ref(str(ref_info)) == remote_rev

    downstream_exp.pull(git_downstream.remote, ref_info.name, force=True)
    assert git_downstream.tmp_dir.scm.get_ref(str(ref_info)) == exp


def test_pull_ambiguous_name(tmp_dir, scm, dvc, git_downstream, exp_stage):
    from dvc.exceptions import InvalidArgumentError

    results = dvc.experiments.run(exp_stage.addressing, params=["foo=2"], name="foo")
    exp_a = first(results)
    ref_info_a = first(exp_refs_by_rev(scm, exp_a))

    tmp_dir.scm_gen("new", "new", commit="new")
    results = dvc.experiments.run(exp_stage.addressing, params=["foo=3"], name="foo")
    exp_b = first(results)
    ref_info_b = first(exp_refs_by_rev(scm, exp_b))

    remote = git_downstream.remote
    downstream_exp = git_downstream.tmp_dir.dvc.experiments
    with pytest.raises(InvalidArgumentError):
        downstream_exp.pull(remote, ["foo"])

    downstream_exp.pull(remote, [str(ref_info_b)])
    assert git_downstream.tmp_dir.scm.get_ref(str(ref_info_b)) == exp_b

    with git_downstream.tmp_dir.scm.detach_head(ref_info_a.baseline_sha):
        downstream_exp.pull(remote, ["foo"])
    assert git_downstream.tmp_dir.scm.get_ref(str(ref_info_a)) == exp_a


def test_auth_error_list(tmp_dir, scm, dvc, http_auth_patch):
    from dvc.scm import GitAuthError

    with pytest.raises(
        GitAuthError,
        match=f"Authentication failed for: '{http_auth_patch}'",
    ):
        dvc.experiments.ls(git_remote=http_auth_patch)


def test_auth_error_pull(tmp_dir, scm, dvc, http_auth_patch):
    from dvc.scm import GitAuthError

    with pytest.raises(
        GitAuthError,
        match=f"Authentication failed for: '{http_auth_patch}'",
    ):
        dvc.experiments.pull(http_auth_patch, ["foo"])


def test_auth_error_push(tmp_dir, scm, dvc, exp_stage, http_auth_patch):
    from dvc.scm import GitAuthError

    results = dvc.experiments.run(exp_stage.addressing, params=["foo=2"])
    exp = first(results)
    ref_info = first(exp_refs_by_rev(scm, exp))

    with pytest.raises(
        GitAuthError,
        match=f"Authentication failed for: '{http_auth_patch}'",
    ):
        dvc.experiments.push(http_auth_patch, [ref_info.name])


@pytest.mark.parametrize("use_ref", [True, False])
def test_get(tmp_dir, scm, dvc, exp_stage, erepo_dir, use_ref):
    from dvc.repo import Repo

    results = dvc.experiments.run(exp_stage.addressing, params=["foo=2"])
    exp_rev = first(results)
    exp_ref = first(exp_refs_by_rev(scm, exp_rev))

    with erepo_dir.chdir():
        Repo.get(
            str(tmp_dir),
            "params.yaml",
            rev=exp_ref.name if use_ref else exp_rev,
        )
        assert (erepo_dir / "params.yaml").read_text().strip() == "foo: 2"
