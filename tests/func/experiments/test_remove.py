from funcy import first

from dvc.repo.experiments.utils import exp_refs_by_rev


def test_remove_experiments_by_ref(tmp_dir, scm, dvc, exp_stage):
    results = dvc.experiments.run(exp_stage.addressing, params=["foo=2"])
    exp = first(results)
    ref_info = first(exp_refs_by_rev(scm, exp))

    removed = dvc.experiments.remove([str(ref_info)])
    assert removed == 1
    assert scm.get_ref(str(ref_info)) is None


def test_remove_all_queued_experiments(tmp_dir, scm, dvc, exp_stage):
    queue_length = 3
    for i in range(queue_length):
        dvc.experiments.run(
            exp_stage.addressing, params=[f"foo={i}"], queue=True
        )

    assert len(dvc.experiments.stash) == queue_length
    removed = dvc.experiments.remove(queue=True)
    assert removed == queue_length
    assert len(dvc.experiments.stash) == 0


def test_remove_special_queued_experiments(tmp_dir, scm, dvc, exp_stage):
    results = dvc.experiments.run(
        exp_stage.addressing, params=["foo=1"], queue=True, name="queue1"
    )
    rev1 = first(results)
    results = dvc.experiments.run(
        exp_stage.addressing, params=["foo=2"], queue=True, name="queue2"
    )
    rev2 = first(results)
    assert rev1 in dvc.experiments.stash_revs
    assert rev2 in dvc.experiments.stash_revs

    assert dvc.experiments.remove(["queue1"]) == 1
    assert rev1 not in dvc.experiments.stash_revs
    assert rev2 in dvc.experiments.stash_revs

    assert dvc.experiments.remove([rev2[:5]]) == 1
    assert len(dvc.experiments.stash) == 0
