import pytest
from funcy import first

from dvc.exceptions import InvalidArgumentError
from dvc.repo.experiments.utils import exp_refs_by_rev


def test_remove_experiments_by_ref(tmp_dir, scm, dvc, exp_stage, caplog):
    queue_length = 3
    ref_list = []

    for i in range(queue_length):
        results = dvc.experiments.run(
            exp_stage.addressing, params=[f"foo={i}"]
        )
        ref_info = first(exp_refs_by_rev(scm, first(results)))
        ref_list.append(str(ref_info))

    with pytest.raises(InvalidArgumentError):
        assert dvc.experiments.remove(ref_list[:2] + ["non-exist"])
    assert scm.get_ref(str(ref_list[0])) is None
    assert scm.get_ref(str(ref_list[1])) is None
    assert scm.get_ref(str(ref_list[2])) is not None


def test_remove_all_queued_experiments(tmp_dir, scm, dvc, exp_stage):
    queue_length = 3

    for i in range(queue_length):
        dvc.experiments.run(
            exp_stage.addressing, params=[f"foo={i}"], queue=True
        )

    results = dvc.experiments.run(
        exp_stage.addressing, params=[f"foo={queue_length}"]
    )
    ref_info = first(exp_refs_by_rev(scm, first(results)))

    assert len(dvc.experiments.stash) == queue_length
    assert dvc.experiments.remove(queue=True) == queue_length
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

    assert dvc.experiments.remove(["queue1", rev2[:5], str(ref_info1)]) == 3
    assert rev1 not in dvc.experiments.stash_revs
    assert rev2 not in dvc.experiments.stash_revs
    assert rev3 in dvc.experiments.stash_revs
    assert scm.get_ref(str(ref_info1)) is None
    assert scm.get_ref(str(ref_info2)) is not None
