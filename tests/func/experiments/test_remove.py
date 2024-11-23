import pytest
from funcy import first

from dvc.exceptions import InvalidArgumentError
from dvc.repo.experiments.exceptions import UnresolvedExpNamesError
from dvc.repo.experiments.utils import exp_refs_by_rev


def test_remove_experiments_by_ref(tmp_dir, scm, dvc, exp_stage, caplog):
    queue_length = 3
    ref_info_list = []
    ref_name_list = []

    for i in range(queue_length):
        results = dvc.experiments.run(exp_stage.addressing, params=[f"foo={i}"])
        ref_info = first(exp_refs_by_rev(scm, first(results)))
        ref_info_list.append(ref_info)
        ref_name_list.append(str(ref_info))

    with pytest.raises(InvalidArgumentError):
        dvc.experiments.remove(ref_name_list[:2] + ["non-exist"])
    assert scm.get_ref(ref_name_list[0]) is not None
    assert scm.get_ref(ref_name_list[1]) is not None
    assert scm.get_ref(ref_name_list[2]) is not None

    assert set(dvc.experiments.remove(ref_name_list[:2])) == set(ref_name_list[:2])
    assert scm.get_ref(ref_name_list[0]) is None
    assert scm.get_ref(ref_name_list[1]) is None
    assert scm.get_ref(ref_name_list[2]) is not None


def test_remove_all_queued_experiments(tmp_dir, scm, dvc, exp_stage):
    queue_length = 3
    for i in range(queue_length):
        dvc.experiments.run(exp_stage.addressing, params=[f"foo={i}"], queue=True)

    results = dvc.experiments.run(exp_stage.addressing, params=[f"foo={queue_length}"])
    ref_info = first(exp_refs_by_rev(scm, first(results)))

    assert len(dvc.experiments.stash_revs) == queue_length
    assert len(dvc.experiments.remove(queue=True)) == queue_length
    assert len(dvc.experiments.stash_revs) == 0
    assert scm.get_ref(str(ref_info)) is not None


def test_remove_special_queued_experiments(tmp_dir, scm, dvc, exp_stage):
    dvc.experiments.run(
        exp_stage.addressing, params=["foo=1"], queue=True, name="queue1"
    )
    dvc.experiments.run(
        exp_stage.addressing, params=["foo=2"], queue=True, name="queue2"
    )
    dvc.experiments.run(
        exp_stage.addressing, params=["foo=3"], queue=True, name="queue3"
    )
    queue_revs = {
        entry.name: entry.stash_rev
        for entry in dvc.experiments.celery_queue.iter_queued()
    }
    assert len(queue_revs) == 3

    results = dvc.experiments.run(exp_stage.addressing, params=["foo=4"])
    ref_info1 = first(exp_refs_by_rev(scm, first(results)))
    results = dvc.experiments.run(exp_stage.addressing, params=["foo=5"])
    ref_info2 = first(exp_refs_by_rev(scm, first(results)))

    assert scm.get_ref(str(ref_info1)) is not None
    assert scm.get_ref(str(ref_info2)) is not None

    rev2 = queue_revs["queue2"]
    assert set(dvc.experiments.remove(["queue1", rev2[:5], str(ref_info1)])) == {
        "queue1",
        rev2[:5],
        str(ref_info1),
    }
    assert len(list(dvc.experiments.celery_queue.iter_queued())) == 1
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
    assert len(dvc.experiments.stash_revs) == 2
    assert scm.get_ref(str(ref_info2)) is None
    assert scm.get_ref(str(ref_info1)) is None


@pytest.mark.parametrize("use_url", [True, False])
def test_remove_remote(tmp_dir, scm, dvc, exp_stage, git_upstream, use_url):
    remote = git_upstream.url if use_url else git_upstream.remote

    ref_info_list = []
    exp_list = []
    for i in range(3):
        results = dvc.experiments.run(exp_stage.addressing, params=[f"foo={i}"])
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
    assert git_upstream.tmp_dir.scm.get_ref(str(ref_info_list[2])) == exp_list[2]

    with pytest.raises(
        UnresolvedExpNamesError, match=f"Experiment 'foo' does not exist in '{remote}'"
    ):
        dvc.experiments.remove(git_remote=remote, exp_names=["foo"])


def test_remove_experiments_by_rev(tmp_dir, scm, dvc, exp_stage):
    baseline = scm.get_rev()

    results = dvc.experiments.run(exp_stage.addressing, params=["foo=1"])
    baseline_exp_ref = first(exp_refs_by_rev(scm, first(results)))

    dvc.experiments.run(
        exp_stage.addressing, params=["foo=2"], queue=True, name="queue2"
    )
    scm.commit("new_baseline")

    results = dvc.experiments.run(exp_stage.addressing, params=["foo=3"])
    ref_info = first(exp_refs_by_rev(scm, first(results)))
    new_exp_ref = str(ref_info)

    dvc.experiments.run(
        exp_stage.addressing, params=["foo=4"], queue=True, name="queue4"
    )

    assert dvc.experiments.remove(rev=baseline) == [baseline_exp_ref.name]
    queue_revs = {
        entry.name: entry.stash_rev
        for entry in dvc.experiments.celery_queue.iter_queued()
    }
    assert scm.get_ref(str(baseline_exp_ref)) is None
    assert "queue2" in queue_revs
    assert scm.get_ref(new_exp_ref) is not None
    assert "queue4" in queue_revs


def test_remove_multi_rev(tmp_dir, scm, dvc, exp_stage):
    baseline = scm.get_rev()

    results = dvc.experiments.run(exp_stage.addressing, params=["foo=1"])
    baseline_exp_ref = first(exp_refs_by_rev(scm, first(results)))

    dvc.experiments.run(
        exp_stage.addressing, params=["foo=2"], queue=True, name="queue2"
    )
    scm.commit("new_baseline")

    results = dvc.experiments.run(exp_stage.addressing, params=["foo=3"])
    new_exp_ref = first(exp_refs_by_rev(scm, first(results)))

    assert set(dvc.experiments.remove(rev=[baseline, scm.get_rev()])) == {
        baseline_exp_ref.name,
        new_exp_ref.name,
    }

    assert scm.get_ref(str(baseline_exp_ref)) is None
    assert scm.get_ref(str(new_exp_ref)) is None

def test_keep_selected_by_name(tmp_dir, scm, dvc, exp_stage):

    # Setup: Run experiments
    results = dvc.experiments.run(exp_stage.addressing, params=["foo=1"], name="exp1")
    exp1_ref = first(exp_refs_by_rev(scm, first(results)))

    results = dvc.experiments.run(exp_stage.addressing, params=["foo=2"], name="exp2")
    exp2_ref = first(exp_refs_by_rev(scm, first(results)))

    results = dvc.experiments.run(exp_stage.addressing, params=["foo=3"], name="exp3")
    exp3_ref = first(exp_refs_by_rev(scm, first(results)))

    # Ensure experiments exist
    assert scm.get_ref(str(exp1_ref)) is not None
    assert scm.get_ref(str(exp2_ref)) is not None
    assert scm.get_ref(str(exp3_ref)) is not None

    # Keep "exp2" and remove others
    removed = dvc.experiments.remove(exp_names=["exp2"], keep_selected=True)
    assert removed == ["exp1", "exp3"]

    # Check remaining experiments
    assert scm.get_ref(str(exp1_ref)) is None
    assert scm.get_ref(str(exp2_ref)) is not None
    assert scm.get_ref(str(exp3_ref)) is None

def test_keep_selected_multiple_by_name(tmp_dir, scm, dvc, exp_stage):

    # Setup: Run experiments
    results = dvc.experiments.run(exp_stage.addressing, params=["foo=1"], name="exp1")
    exp1_ref = first(exp_refs_by_rev(scm, first(results)))

    results = dvc.experiments.run(exp_stage.addressing, params=["foo=2"], name="exp2")
    exp2_ref = first(exp_refs_by_rev(scm, first(results)))

    results = dvc.experiments.run(exp_stage.addressing, params=["foo=3"], name="exp3")
    exp3_ref = first(exp_refs_by_rev(scm, first(results)))

    # Ensure experiments exist
    assert scm.get_ref(str(exp1_ref)) is not None
    assert scm.get_ref(str(exp2_ref)) is not None
    assert scm.get_ref(str(exp3_ref)) is not None

    # Keep "exp1" and "exp2" and remove "exp3"
    removed = dvc.experiments.remove(exp_names=["exp1","exp2"], keep_selected=True)
    assert removed == ["exp3"]

    # Check remaining experiments
    assert scm.get_ref(str(exp1_ref)) is not None
    assert scm.get_ref(str(exp2_ref)) is not None
    assert scm.get_ref(str(exp3_ref)) is None

def test_keep_selected_all_by_name(tmp_dir, scm, dvc, exp_stage):
    # Setup: Run experiments
    results = dvc.experiments.run(exp_stage.addressing, params=["foo=1"], name="exp1")
    exp1_ref = first(exp_refs_by_rev(scm, first(results)))

    results = dvc.experiments.run(exp_stage.addressing, params=["foo=2"], name="exp2")
    exp2_ref = first(exp_refs_by_rev(scm, first(results)))

    results = dvc.experiments.run(exp_stage.addressing, params=["foo=3"], name="exp3")
    exp3_ref = first(exp_refs_by_rev(scm, first(results)))

    # Ensure experiments exist
    assert scm.get_ref(str(exp1_ref)) is not None
    assert scm.get_ref(str(exp2_ref)) is not None
    assert scm.get_ref(str(exp3_ref)) is not None

    # Keep "exp1" and "exp2" and remove "exp3"
    removed = dvc.experiments.remove(exp_names=["exp1","exp2", "exp3"], keep_selected=True)
    assert removed == []

    # Check remaining experiments
    assert scm.get_ref(str(exp1_ref)) is not None
    assert scm.get_ref(str(exp2_ref)) is not None
    assert scm.get_ref(str(exp3_ref)) is not None

def test_keep_selected_by_nonexistent_name(tmp_dir, scm, dvc, exp_stage):
    # Setup: Run experiments
    results = dvc.experiments.run(exp_stage.addressing, params=["foo=1"], name="exp1")
    exp1_ref = first(exp_refs_by_rev(scm, first(results)))

    results = dvc.experiments.run(exp_stage.addressing, params=["foo=2"], name="exp2")
    exp2_ref = first(exp_refs_by_rev(scm, first(results)))

    results = dvc.experiments.run(exp_stage.addressing, params=["foo=3"], name="exp3")
    exp3_ref = first(exp_refs_by_rev(scm, first(results)))

    # Ensure experiments exist
    assert scm.get_ref(str(exp1_ref)) is not None
    assert scm.get_ref(str(exp2_ref)) is not None
    assert scm.get_ref(str(exp3_ref)) is not None

    # Keep "exp1" and "exp2" and remove "exp3"
    removed = dvc.experiments.remove(exp_names=["nonexistent"], keep_selected=True)
    assert removed == ["exp1", "exp2", "exp3"]

    # Check remaining experiments
    assert scm.get_ref(str(exp1_ref)) is None
    assert scm.get_ref(str(exp2_ref)) is None
    assert scm.get_ref(str(exp3_ref)) is None

def test_keep_selected_by_rev(tmp_dir, scm, dvc, exp_stage):
    # Setup: Run experiments and commit
    baseline = scm.get_rev()
    results = dvc.experiments.run(exp_stage.addressing, params=["foo=1"], name="exp1")
    exp1_ref = first(exp_refs_by_rev(scm, first(results)))
    scm.commit("commit1")

    new_results = dvc.experiments.run(exp_stage.addressing, params=["foo=2"], name="exp2")
    exp2_ref = first(exp_refs_by_rev(scm, first(new_results)))
    new_rev = scm.get_rev()

    # Ensure experiments exist
    assert scm.get_ref(str(exp1_ref)) is not None
    assert scm.get_ref(str(exp2_ref)) is not None

    # Keep the experiment from the new revision
    removed = dvc.experiments.remove(rev=new_rev, num=1, keep_selected=True)
    assert removed == ["exp1"]

    # Check remaining experiments
    assert scm.get_ref(str(exp2_ref)) is not None
    assert scm.get_ref(str(exp1_ref)) is None

def test_keep_selected_by_rev_multiple(tmp_dir, scm, dvc, exp_stage):
    # Setup: Run experiments and commit
    baseline = scm.get_rev()
    exp1_rev = scm.get_rev()
    results = dvc.experiments.run(exp_stage.addressing, params=["foo=1"], name="exp1")
    exp1_ref = first(exp_refs_by_rev(scm, first(results)))
    scm.commit("commit1")

    exp2_rev = scm.get_rev()
    results = dvc.experiments.run(exp_stage.addressing, params=["foo=2"], name="exp2")
    exp2_ref = first(exp_refs_by_rev(scm, first(results)))
    scm.commit("commit2")

    exp3_rev = scm.get_rev()
    results = dvc.experiments.run(exp_stage.addressing, params=["foo=3"], name="exp3")
    exp3_ref = first(exp_refs_by_rev(scm, first(results)))
    scm.commit("commit3")

    # Ensure experiments exist
    assert scm.get_ref(str(exp1_ref)) is not None
    assert scm.get_ref(str(exp2_ref)) is not None
    assert scm.get_ref(str(exp3_ref)) is not None

    # Keep the last 2, remove first
    removed = dvc.experiments.remove(rev=exp3_rev, num=2, keep_selected=True)
    assert removed == ["exp1"]

    # Check remaining experiments
    assert scm.get_ref(str(exp3_ref)) is not None
    assert scm.get_ref(str(exp2_ref)) is not None
    assert scm.get_ref(str(exp1_ref)) is None