import datetime
import random
from typing import Dict, List

import pytest

from dvc.repo.experiments.collect import ExpRange, ExpState, SerializableExp, collect


@pytest.mark.vscode
def test_collect_stable_sorting(dvc, scm, mocker):
    """
    Check that output is deterministically sorted even for
    commits with the same timestamp. This affects the experience
    in vs-code to avoid experiments "bouncing around" when "exp show"
    is called repeatedly
    """
    expected_revs = [
        "c" * 40,
        "b" * 40,
        "a" * 40,
        "7" * 40,
    ]

    def collect_queued_patched(_, baseline_revs) -> Dict[str, List["ExpRange"]]:
        single_timestamp = datetime.datetime(2023, 6, 20, 0, 0, 0)

        exp_ranges = [
            ExpRange(
                revs=[
                    ExpState(
                        rev=rev,
                        name=f"exp-state-{rev[0]}",
                        data=SerializableExp(rev=rev, timestamp=single_timestamp),
                    )
                ],
                name=f"exp-range-{rev[0]}",
            )
            for rev in expected_revs
        ]

        # shuffle collection order
        random.shuffle(exp_ranges)

        return {baseline_rev: exp_ranges for baseline_rev in baseline_revs}

    mocker.patch("dvc.repo.experiments.collect.collect_queued", collect_queued_patched)
    mocker.patch("dvc.repo.experiments.collect.collect_active", return_value={})
    mocker.patch("dvc.repo.experiments.collect.collect_failed", return_value={})
    mocker.patch("dvc.repo.experiments.collect.collect_successful", return_value={})

    # repeat (shuffling collection order in collect_queued_patched)
    for _ in range(20):
        collected = collect(repo=dvc, all_commits=True)
        assert collected[0].rev == "workspace"
        assert collected[0].experiments is None
        assert collected[1].rev == scm.get_rev()
        _assert_experiment_rev_order(collected[1].experiments, expected_revs)


def _assert_experiment_rev_order(
    actual: List["ExpRange"],
    expected_revs: List[str],
):
    expected_revs = expected_revs.copy()

    for actual_exp_range in actual:
        for exp_state in actual_exp_range.revs:
            assert exp_state.rev == expected_revs.pop(0)
