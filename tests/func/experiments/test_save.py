import pytest
from funcy import first

from dvc.repo.experiments.exceptions import ExperimentExistsError, InvalidArgumentError
from dvc.repo.experiments.utils import exp_refs_by_rev
from dvc.scm import resolve_rev


def setup_stage(tmp_dir, dvc, scm):
    tmp_dir.gen("params.yaml", "foo: 1")
    dvc.run(name="echo-foo", outs=["bar"], cmd="echo foo > bar")
    scm.add(["dvc.yaml", "dvc.lock", ".gitignore", "params.yaml"])
    scm.commit("init")


def test_exp_save_unchanged(tmp_dir, dvc, scm):
    setup_stage(tmp_dir, dvc, scm)
    dvc.experiments.save()


@pytest.mark.parametrize("name", (None, "test"))
def test_exp_save(tmp_dir, dvc, scm, name):
    setup_stage(tmp_dir, dvc, scm)
    baseline = scm.get_rev()

    exp = dvc.experiments.save(name=name)
    ref_info = first(exp_refs_by_rev(scm, exp))
    assert ref_info
    assert ref_info.baseline_sha == baseline

    exp_name = name if name else ref_info.name
    assert dvc.experiments.get_exact_name([exp])[exp] == exp_name
    assert resolve_rev(scm, exp_name) == exp


def test_exp_save_overwrite_experiment(tmp_dir, dvc, scm):
    setup_stage(tmp_dir, dvc, scm)
    name = "dummy"
    dvc.experiments.save(name=name)

    tmp_dir.gen("params.yaml", "foo: 2")
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
def test_exp_save_invalid_name(tmp_dir, dvc, scm, name):
    setup_stage(tmp_dir, dvc, scm)
    with pytest.raises(InvalidArgumentError):
        dvc.experiments.save(name=name, force=True)


def test_exp_save_after_commit(tmp_dir, dvc, scm):
    setup_stage(tmp_dir, dvc, scm)
    baseline = scm.get_rev()
    dvc.experiments.save(name="exp-1", force=True)

    tmp_dir.scm_gen({"new_file": "new_file"}, commit="new baseline")
    baseline_new = scm.get_rev()
    dvc.experiments.save(name="exp-2", force=True)

    all_exps = dvc.experiments.ls(all_commits=True)
    assert all_exps[baseline][0][0] == "exp-1"
    assert all_exps[baseline_new][0][0] == "exp-2"


def test_exp_save_with_staged_changes(tmp_dir, dvc, scm):
    setup_stage(tmp_dir, dvc, scm)
    tmp_dir.gen({"deleted": "deleted", "modified": "modified"})
    scm.add_commit(["deleted", "modified"], "init")

    (tmp_dir / "deleted").unlink()
    tmp_dir.gen({"new_file": "new_file"})
    (tmp_dir / "modified").write_text("foo")
    scm.add(["deleted", "new_file", "modified"])

    exp_rev = dvc.experiments.save(name="exp")
    scm.checkout(exp_rev, force=True)
    assert not (tmp_dir / "deleted").exists()
    assert (tmp_dir / "new_file").exists()
    assert (tmp_dir / "modified").read_text() == "foo"


def test_exp_save_include_untracked(tmp_dir, dvc, scm):
    setup_stage(tmp_dir, dvc, scm)

    new_file = tmp_dir / "new_file"
    new_file.write_text("new_file")
    dvc.experiments.save(name="exp", include_untracked=["new_file"])

    _, _, unstaged = scm.status()
    assert "new_file" in unstaged
    assert new_file.read_text() == "new_file"


def test_exp_save_include_untracked_warning(tmp_dir, dvc, scm, mocker):
    """Regression test for https://github.com/iterative/dvc/issues/9061"""
    setup_stage(tmp_dir, dvc, scm)

    new_dir = tmp_dir / "new_dir"
    new_dir.mkdir()
    (new_dir / "foo").write_text("foo")
    (new_dir / "bar").write_text("bar")

    logger = mocker.patch("dvc.repo.experiments.executor.base.logger")

    dvc.experiments.save(name="exp", include_untracked=["new_dir"])
    assert not logger.warning.called


def test_untracked_top_level_files_are_included_in_exp(tmp_dir, scm, dvc):
    (tmp_dir / "dvc.yaml").dump(
        {"metrics": ["metrics.json"], "params": ["params.yaml"], "plots": ["plots.csv"]}
    )
    stage = dvc.stage.add(
        cmd="touch metrics.json && touch params.yaml && touch plots.csv",
        name="top-level",
    )
    scm.add_commit(["dvc.yaml"], message="add dvc.yaml")
    dvc.reproduce(stage.addressing)
    exp = dvc.experiments.save()
    fs = scm.get_fs(exp)
    for file in ["metrics.json", "params.yaml", "plots.csv", "dvc.lock"]:
        assert fs.exists(file)


def test_untracked_dvclock_is_included_in_exp(tmp_dir, scm, dvc):
    stage = dvc.stage.add(cmd="echo foo", name="foo")
    scm.add_commit(["dvc.yaml"], message="add dvc.yaml")
    dvc.reproduce(stage.addressing)

    # dvc.reproduce automatically stages `dvc.lock`
    # force it to be untracked
    scm.reset()

    exp = dvc.experiments.save()
    fs = scm.get_fs(exp)
    assert fs.exists("dvc.lock")


def test_exp_save_include_untracked_force(tmp_dir, dvc, scm):
    setup_stage(tmp_dir, dvc, scm)

    new_file = tmp_dir / "new_file"
    new_file.write_text("new_file")
    dvc.scm.ignore(new_file)
    exp = dvc.experiments.save(include_untracked=["new_file"])

    fs = scm.get_fs(exp)
    assert fs.exists("new_file")


def test_exp_save_custom_message(tmp_dir, dvc, scm):
    setup_stage(tmp_dir, dvc, scm)

    exp = dvc.experiments.save(message="custom commit message")
    assert scm.gitpython.repo.commit(exp).message == "custom commit message"


def test_exp_save_target(tmp_dir, dvc, scm):
    setup_stage(tmp_dir, dvc, scm)
    orig_dvclock = (tmp_dir / "dvc.lock").read_text()
    (tmp_dir / "bar").write_text("modified")

    tmp_dir.dvc_gen({"file": "orig"}, commit="add files")
    orig_dvcfile = (tmp_dir / "file.dvc").read_text()
    (tmp_dir / "file").write_text("modified")

    dvc.experiments.save(["file"])
    assert (tmp_dir / "bar").read_text() == "modified"
    assert (tmp_dir / "dvc.lock").read_text() == orig_dvclock
    assert (tmp_dir / "file").read_text() == "modified"
    assert (tmp_dir / "file.dvc").read_text() != orig_dvcfile
