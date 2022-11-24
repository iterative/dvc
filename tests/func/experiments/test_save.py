from contextlib import nullcontext

import pytest
from funcy import first

from dvc.repo.experiments.exceptions import ExperimentExistsError
from dvc.repo.experiments.utils import exp_refs_by_rev
from dvc.scm import resolve_rev
from dvc.stage.exceptions import StageCommitError


@pytest.mark.parametrize("name", (None, "test"))
def test_exp_save(tmp_dir, dvc, scm, exp_stage, name):
    baseline = scm.get_rev()

    exp = dvc.experiments.save(name=name)
    ref_info = first(exp_refs_by_rev(scm, exp))
    assert ref_info and ref_info.baseline_sha == baseline

    exp_name = name if name else ref_info.name
    assert dvc.experiments.get_exact_name([exp])[exp] == exp_name
    assert resolve_rev(scm, exp_name) == exp


@pytest.mark.parametrize(
    ("force", "expected_raises"),
    (
        (False, pytest.raises(StageCommitError)),
        (True, nullcontext()),
    ),
)
def test_exp_save_force(tmp_dir, dvc, scm, exp_stage, force, expected_raises):
    with open(tmp_dir / "copy.py", "a", encoding="utf-8") as fh:
        fh.write("\n# dummy change")

    with expected_raises:
        dvc.experiments.save(force=force)


def test_exp_save_overwrite_experiment(tmp_dir, dvc, scm, exp_stage):
    dvc.experiments.save(name="dummy")

    with open(tmp_dir / "copy.py", "a", encoding="utf-8") as fh:
        fh.write("\n# dummy change")

    with pytest.raises(ExperimentExistsError):
        dvc.experiments.save(name="dummy")

    dvc.experiments.save(name="dummy", force=True)


def test_exp_save_multiple(tmp_dir, dvc, scm):
    baseline = scm.get_rev()
    for i in range(2):
        name = f"exp-{i}"
        tmp_dir.gen({name: f"{name} content"})
        dvc.experiments.save(name=name)

    assert dvc.experiments.ls()[baseline] == ["exp-0", "exp-1"]

    for i in range(2):
        scm.reset(hard=True)
        name = f"exp-{i}"
        dvc.experiments.apply(name)
        assert (tmp_dir / name).read_text() == f"{name} content"


def test_exp_save_after_commit(tmp_dir, dvc, scm, exp_stage):
    baseline = scm.get_rev()
    dvc.experiments.save(name="exp-1")

    tmp_dir.scm_gen({"new_file": "new_file"}, commit="new baseline")
    new_baseline = scm.get_rev()
    dvc.experiments.save(name="exp-2")

    all_exps = dvc.experiments.ls(all_commits=True)
    assert all_exps[baseline] == ["exp-1"]
    assert all_exps[new_baseline] == ["exp-2"]


def test_exp_save_with_staged_changes(tmp_dir, dvc, scm):
    tmp_dir.gen({"new_file": "new_file"})
    scm.add("new_file")

    dvc.experiments.save(name="exp")

    _, _, unstaged = scm.status()
    assert "new_file" in unstaged