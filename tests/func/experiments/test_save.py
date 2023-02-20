import logging

import pytest
from funcy import first

from dvc.repo.experiments.exceptions import ExperimentExistsError, InvalidArgumentError
from dvc.repo.experiments.utils import exp_refs_by_rev
from dvc.scm import resolve_rev


@pytest.fixture
def modified_exp_stage(exp_stage, tmp_dir):
    with open(tmp_dir / "copy.py", "a", encoding="utf-8") as fh:
        fh.write("\n# dummy change")


def test_exp_save_unchanged(tmp_dir, dvc, scm, exp_stage):
    dvc.experiments.save()


@pytest.mark.parametrize("name", (None, "test"))
def test_exp_save(tmp_dir, dvc, scm, exp_stage, name, modified_exp_stage):
    baseline = scm.get_rev()

    exp = dvc.experiments.save(name=name)
    ref_info = first(exp_refs_by_rev(scm, exp))
    assert ref_info
    assert ref_info.baseline_sha == baseline

    exp_name = name if name else ref_info.name
    assert dvc.experiments.get_exact_name([exp])[exp] == exp_name
    assert resolve_rev(scm, exp_name) == exp


def test_exp_save_overwrite_experiment(
    tmp_dir, dvc, scm, exp_stage, modified_exp_stage
):
    name = "dummy"
    dvc.experiments.save(name=name)

    with pytest.raises(ExperimentExistsError):
        dvc.experiments.save(name=name)

    dvc.experiments.save(name=name, force=True)


@pytest.mark.parametrize(
    "name",
    (
        "invalid/name",
        "invalid..name",
        "invalid~name",
        "invalid?name",
        "invalidname.",
    ),
)
def test_exp_save_invalid_name(tmp_dir, dvc, scm, exp_stage, name):
    with pytest.raises(InvalidArgumentError):
        dvc.experiments.save(name=name, force=True)


def test_exp_save_after_commit(tmp_dir, dvc, scm, exp_stage):
    baseline = scm.get_rev()
    dvc.experiments.save(name="exp-1", force=True)

    tmp_dir.scm_gen({"new_file": "new_file"}, commit="new baseline")
    dvc.experiments.save(name="exp-2", force=True)

    all_exps = dvc.experiments.ls(all_commits=True)
    assert all_exps[baseline[:7]] == ["exp-1"]
    assert all_exps["master"] == ["exp-2"]


def test_exp_save_with_staged_changes(tmp_dir, dvc, scm):
    tmp_dir.gen({"new_file": "new_file"})
    scm.add("new_file")

    dvc.experiments.save(name="exp")

    _, _, unstaged = scm.status()
    assert "new_file" in unstaged


def test_exp_save_include_untracked(tmp_dir, dvc, scm, exp_stage):
    new_file = tmp_dir / "new_file"
    for i in range(2):
        new_file.write_text(f"exp-{i}")
        dvc.experiments.save(name=f"exp-{i}", include_untracked=["new_file"])

    _, _, unstaged = scm.status()
    assert "new_file" in unstaged
    assert new_file.read_text() == f"exp-{i}"

    dvc.experiments.apply("exp-0")
    assert new_file.read_text() == "exp-0"


def test_exp_save_include_untracked_warning(tmp_dir, dvc, scm, caplog, exp_stage):
    """Regression test for https://github.com/iterative/dvc/issues/9061"""
    new_dir = tmp_dir / "new_dir"
    new_dir.mkdir()
    (new_dir / "foo").write_text("foo")
    (new_dir / "bar").write_text("bar")

    with caplog.at_level(logging.WARNING, logger="dvc.repo.experiments.save"):
        dvc.experiments.save(name="exp", include_untracked=["new_dir"])
        assert not caplog.records
