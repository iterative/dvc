import pytest
from funcy import first

from dvc.exceptions import InvalidArgumentError
from dvc.repo.experiments.utils import exp_refs_by_rev


def test_remove_experiments_by_ref(tmp_dir, scm, dvc, exp_stage, caplog):
    queue_length = 3
    ref_info_list = []
    ref_name_list = []

    for i in range(queue_length):
        results = dvc.experiments.run(
            exp_stage.addressing, params=[f"foo={i}"]
        )
        ref_info = first(exp_refs_by_rev(scm, first(results)))
        ref_info_list.append(ref_info)
        ref_name_list.append(str(ref_info))

    with pytest.raises(InvalidArgumentError):
        dvc.experiments.remove(ref_name_list[:2] + ["non-exist"])
    assert scm.get_ref(ref_name_list[0]) is not None
    assert scm.get_ref(ref_name_list[1]) is not None
    assert scm.get_ref(ref_name_list[2]) is not None

    assert set(dvc.experiments.remove(ref_name_list[:2])) == set(
        ref_name_list[:2]
    )
    assert scm.get_ref(ref_name_list[0]) is None
    assert scm.get_ref(ref_name_list[1]) is None
    assert scm.get_ref(ref_name_list[2]) is not None


def test_remove_all_queued_experiments(tmp_dir, scm, dvc, exp_stage):
    queue_length = 3
    ref_list = []
    for i in range(queue_length):
        result = dvc.experiments.run(
            exp_stage.addressing, params=[f"foo={i}"], queue=True
        )
        ref_list.append(first(result)[:7])

    results = dvc.experiments.run(
        exp_stage.addressing, params=[f"foo={queue_length}"]
    )
    ref_info = first(exp_refs_by_rev(scm, first(results)))

    assert len(dvc.experiments.stash) == queue_length
    assert set(dvc.experiments.remove(queue=True)) == set(ref_list)
    assert len(dvc.experiments.stash) == 0
    assert scm.get_ref(str(ref_info)) is not None


def test_remove_special_queued_experiments(tmp_dir, scm, dvc, exp_stage):
    results = dvc.experiments.run(
        exp_stage.addressing, params=["foo=1"], queue=True, name="queue1"
    )
    rev1 = first(results)
    results = dvc.experiments.run(
        exp_stage.addressing, params=["foo=2"], queue=True, name="queue2"
    )
    rev2 = first(results)
    results = dvc.experiments.run(
        exp_stage.addressing, params=["foo=3"], queue=True, name="queue3"
    )
    rev3 = first(results)
    results = dvc.experiments.run(exp_stage.addressing, params=["foo=4"])
    ref_info1 = first(exp_refs_by_rev(scm, first(results)))
    results = dvc.experiments.run(exp_stage.addressing, params=["foo=5"])
    ref_info2 = first(exp_refs_by_rev(scm, first(results)))

    assert rev1 in dvc.experiments.stash_revs
    assert rev2 in dvc.experiments.stash_revs
    assert rev3 in dvc.experiments.stash_revs
    assert scm.get_ref(str(ref_info1)) is not None
    assert scm.get_ref(str(ref_info2)) is not None

    assert set(
        dvc.experiments.remove(["queue1", rev2[:5], str(ref_info1)])
    ) == {"queue1", rev2[:5], str(ref_info1)}
    assert rev1 not in dvc.experiments.stash_revs
    assert rev2 not in dvc.experiments.stash_revs
    assert rev3 in dvc.experiments.stash_revs
    assert scm.get_ref(str(ref_info1)) is None
    assert scm.get_ref(str(ref_info2)) is not None


def test_remove_all(tmp_dir, scm, dvc, exp_stage):
    results = dvc.experiments.run(exp_stage.addressing, params=["foo=1"])
    ref_info1 = first(exp_refs_by_rev(scm, first(results)))
    dvc.experiments.run(exp_stage.addressing, params=["foo=2"], queue=True)
    scm.add(["dvc.yaml", "dvc.lock", "copy.py", "params.yaml", "metrics.yaml"])
    scm.commit("update baseline")

    results = dvc.experiments.run(exp_stage.addressing, params=["foo=3"])
    ref_info2 = first(exp_refs_by_rev(scm, first(results)))
    dvc.experiments.run(exp_stage.addressing, params=["foo=4"], queue=True)

    assert set(dvc.experiments.remove(all_commits=True)) == {
        ref_info1.name,
        ref_info2.name,
    }
    assert len(dvc.experiments.stash) == 2
    assert scm.get_ref(str(ref_info2)) is None
    assert scm.get_ref(str(ref_info1)) is None


@pytest.mark.parametrize("use_url", [True, False])
def test_remove_remote(tmp_dir, scm, dvc, exp_stage, git_upstream, use_url):
    remote = git_upstream.url if use_url else git_upstream.remote

    ref_info_list = []
    exp_list = []
    for i in range(3):
        results = dvc.experiments.run(
            exp_stage.addressing, params=[f"foo={i}"]
        )
        exp = first(results)
        exp_list.append(exp)
        ref_info = first(exp_refs_by_rev(scm, exp))
        ref_info_list.append(ref_info)
        dvc.experiments.push(remote, [ref_info.name])
        assert git_upstream.tmp_dir.scm.get_ref(str(ref_info)) == exp

    dvc.experiments.remove(
        git_remote=remote,
        exp_names=[str(ref_info_list[0]), ref_info_list[1].name],
    )

    assert git_upstream.tmp_dir.scm.get_ref(str(ref_info_list[0])) is None
    assert git_upstream.tmp_dir.scm.get_ref(str(ref_info_list[1])) is None
    assert (
        git_upstream.tmp_dir.scm.get_ref(str(ref_info_list[2])) == exp_list[2]
    )


def test_remove_experiments_by_rev(tmp_dir, scm, dvc, exp_stage):
    baseline = scm.get_rev()

    results = dvc.experiments.run(exp_stage.addressing, params=["foo=1"])
    baseline_exp_ref = first(exp_refs_by_rev(scm, first(results)))

    results = dvc.experiments.run(
        exp_stage.addressing, params=["foo=2"], queue=True, name="queue2"
    )
    baseline_queue_ref = first(results)

    scm.commit("new_baseline")

    results = dvc.experiments.run(exp_stage.addressing, params=["foo=3"])
    ref_info = first(exp_refs_by_rev(scm, first(results)))
    new_exp_ref = str(ref_info)

    results = dvc.experiments.run(
        exp_stage.addressing, params=["foo=4"], queue=True, name="queue4"
    )
    new_queue_ref = first(results)
    assert dvc.experiments.remove(rev=baseline) == [baseline_exp_ref.name]
    assert scm.get_ref(str(baseline_exp_ref)) is None
    assert baseline_queue_ref in dvc.experiments.stash_revs
    assert scm.get_ref(new_exp_ref) is not None
    assert new_queue_ref in dvc.experiments.stash_revs
