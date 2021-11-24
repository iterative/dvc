import logging

import pytest
from funcy import first

from dvc.config import NoRemoteError
from dvc.env import DVC_EXP_AUTO_PUSH, DVC_EXP_GIT_REMOTE
from dvc.exceptions import DvcException
from dvc.repo.experiments import MultipleBranchError
from dvc.repo.experiments.base import EXEC_APPLY, EXEC_CHECKPOINT
from dvc.repo.experiments.executor.base import BaseExecutor
from dvc.repo.experiments.utils import exp_refs_by_rev
from dvc.scm import InvalidRemoteSCMRepo


@pytest.mark.parametrize("workspace", [True, False])
@pytest.mark.parametrize("links", ["reflink,copy", "hardlink,symlink"])
def test_new_checkpoint(
    tmp_dir, scm, dvc, checkpoint_stage, mocker, workspace, links
):
    with dvc.config.edit() as conf:
        conf["cache"]["type"] = links

    new_mock = mocker.spy(dvc.experiments, "new")
    results = dvc.experiments.run(
        checkpoint_stage.addressing, params=["foo=2"], tmp_dir=not workspace
    )
    exp = first(results)

    new_mock.assert_called_once()
    for rev in dvc.brancher([exp]):
        if rev == "workspace":
            continue
        fs = dvc.repo_fs
        with fs.open((tmp_dir / "foo").fs_path) as fobj:
            assert fobj.read().strip() == str(checkpoint_stage.iterations)
        with fs.open((tmp_dir / "metrics.yaml").fs_path) as fobj:
            assert fobj.read().strip() == "foo: 2"

    if workspace:
        assert scm.get_ref(EXEC_APPLY) == exp
    assert scm.get_ref(EXEC_CHECKPOINT) == exp
    if workspace:
        assert (tmp_dir / "foo").read_text().strip() == str(
            checkpoint_stage.iterations
        )
        assert (tmp_dir / "metrics.yaml").read_text().strip() == "foo: 2"


@pytest.mark.parametrize(
    "checkpoint_resume, workspace",
    [(None, True), (None, False), ("foo", True), ("foo", False)],
)
def test_resume_checkpoint(
    tmp_dir, scm, dvc, checkpoint_stage, checkpoint_resume, workspace
):
    results = dvc.experiments.run(
        checkpoint_stage.addressing, params=["foo=2"], tmp_dir=not workspace
    )

    with pytest.raises(DvcException):
        dvc.experiments.run(
            checkpoint_stage.addressing,
            checkpoint_resume="abc1234",
            tmp_dir=not workspace,
        )

    if checkpoint_resume:
        checkpoint_resume = first(results)

    if not workspace:
        dvc.experiments.apply(first(results))
    results = dvc.experiments.run(
        checkpoint_stage.addressing,
        checkpoint_resume=checkpoint_resume,
        tmp_dir=not workspace,
    )
    exp = first(results)

    for rev in dvc.brancher([exp]):
        if rev == "workspace":
            continue
        fs = dvc.repo_fs
        with fs.open((tmp_dir / "foo").fs_path) as fobj:
            assert fobj.read().strip() == str(2 * checkpoint_stage.iterations)
        with fs.open((tmp_dir / "metrics.yaml").fs_path) as fobj:
            assert fobj.read().strip() == "foo: 2"

    if workspace:
        assert scm.get_ref(EXEC_APPLY) == exp
    assert scm.get_ref(EXEC_CHECKPOINT) == exp


@pytest.mark.parametrize("workspace", [True, False])
def test_reset_checkpoint(
    tmp_dir, scm, dvc, checkpoint_stage, caplog, workspace
):
    dvc.experiments.run(
        checkpoint_stage.addressing, name="foo", tmp_dir=not workspace
    )

    results = dvc.experiments.run(
        checkpoint_stage.addressing,
        params=["foo=2"],
        name="foo",
        tmp_dir=not workspace,
        reset=True,
    )
    exp = first(results)

    for rev in dvc.brancher([exp]):
        if rev == "workspace":
            continue
        fs = dvc.repo_fs
        with fs.open((tmp_dir / "foo").fs_path) as fobj:
            assert fobj.read().strip() == str(checkpoint_stage.iterations)
        with fs.open((tmp_dir / "metrics.yaml").fs_path) as fobj:
            assert fobj.read().strip() == "foo: 2"

    if workspace:
        assert scm.get_ref(EXEC_APPLY) == exp
    assert scm.get_ref(EXEC_CHECKPOINT) == exp


@pytest.mark.parametrize("workspace", [True, False])
def test_resume_branch(tmp_dir, scm, dvc, checkpoint_stage, workspace):
    results = dvc.experiments.run(
        checkpoint_stage.addressing, params=["foo=2"], tmp_dir=not workspace
    )
    branch_rev = first(results)
    if not workspace:
        dvc.experiments.apply(branch_rev)

    results = dvc.experiments.run(
        checkpoint_stage.addressing,
        checkpoint_resume=branch_rev,
        tmp_dir=not workspace,
    )
    checkpoint_a = first(results)

    dvc.experiments.apply(branch_rev)
    results = dvc.experiments.run(
        checkpoint_stage.addressing,
        checkpoint_resume=branch_rev,
        params=["foo=100"],
        tmp_dir=not workspace,
    )
    checkpoint_b = first(results)

    for rev in dvc.brancher([checkpoint_a]):
        if rev == "workspace":
            continue
        fs = dvc.repo_fs
        with fs.open((tmp_dir / "foo").fs_path) as fobj:
            assert fobj.read().strip() == str(2 * checkpoint_stage.iterations)
        with fs.open((tmp_dir / "metrics.yaml").fs_path) as fobj:
            assert fobj.read().strip() == "foo: 2"

    for rev in dvc.brancher([checkpoint_b]):
        if rev == "workspace":
            continue
        fs = dvc.repo_fs
        with fs.open((tmp_dir / "foo").fs_path) as fobj:
            assert fobj.read().strip() == str(2 * checkpoint_stage.iterations)
        with fs.open((tmp_dir / "metrics.yaml").fs_path) as fobj:
            assert fobj.read().strip() == "foo: 100"

    with pytest.raises(MultipleBranchError):
        dvc.experiments.get_branch_by_rev(branch_rev)

    assert branch_rev == dvc.experiments.scm.gitpython.repo.git.merge_base(
        checkpoint_a, checkpoint_b
    )


@pytest.mark.parametrize("workspace", [True, False])
def test_resume_non_head_checkpoint(
    tmp_dir, scm, dvc, checkpoint_stage, workspace
):
    orig_head = scm.get_rev()
    results = dvc.experiments.run(
        checkpoint_stage.addressing, params=["foo=2"], tmp_dir=not workspace
    )
    checkpoint_head = first(results)
    orig_branch = dvc.experiments.get_branch_by_rev(checkpoint_head)

    rev = list(scm.branch_revs(checkpoint_head, orig_head))[-1]
    dvc.experiments.apply(rev)

    with pytest.raises(DvcException):
        dvc.experiments.run(checkpoint_stage.addressing, tmp_dir=not workspace)

    results = dvc.experiments.run(
        checkpoint_stage.addressing, params=["foo=100"], tmp_dir=not workspace
    )
    new_head = first(results)
    assert orig_branch != dvc.experiments.get_branch_by_rev(new_head)


@pytest.fixture
def clear_env(monkeypatch):
    yield
    monkeypatch.delenv(DVC_EXP_GIT_REMOTE, raising=False)
    monkeypatch.delenv(DVC_EXP_AUTO_PUSH, raising=False)


@pytest.mark.parametrize("use_url", [True, False])
def test_auto_push_during_iterations(
    tmp_dir,
    scm,
    dvc,
    checkpoint_stage,
    git_upstream,
    local_remote,
    use_url,
    monkeypatch,
    mocker,
    clear_env,
):
    # set up remote repo
    remote = git_upstream.url if use_url else git_upstream.remote
    git_upstream.tmp_dir.scm.fetch_refspecs(str(tmp_dir), ["master:master"])
    monkeypatch.setenv(DVC_EXP_GIT_REMOTE, remote)
    auto_push_spy = mocker.spy(BaseExecutor, "_auto_push")

    # without auto push
    results = dvc.experiments.run(checkpoint_stage.addressing)
    assert auto_push_spy.call_count == 0

    # add auto push
    monkeypatch.setenv(DVC_EXP_AUTO_PUSH, "true")
    results = dvc.experiments.run(checkpoint_stage.addressing)
    assert (tmp_dir / "foo").read_text() == "4"
    exp = first(results)
    ref_info = first(exp_refs_by_rev(scm, exp))
    assert git_upstream.tmp_dir.scm.get_ref(str(ref_info)) == exp

    assert auto_push_spy.call_count == 2
    assert auto_push_spy.call_args[0][2] == remote


def test_auto_push_error_url(
    dvc, scm, checkpoint_stage, local_remote, monkeypatch, clear_env
):
    monkeypatch.setenv(DVC_EXP_GIT_REMOTE, "true")
    monkeypatch.setenv(DVC_EXP_AUTO_PUSH, "true")
    with pytest.raises(InvalidRemoteSCMRepo):
        dvc.experiments.run(checkpoint_stage.addressing, params=["foo=2"])


def test_auto_push_no_remote(
    dvc, scm, checkpoint_stage, git_upstream, monkeypatch, clear_env
):
    monkeypatch.setenv(DVC_EXP_GIT_REMOTE, git_upstream.url)
    monkeypatch.setenv(DVC_EXP_AUTO_PUSH, "true")
    with pytest.raises(NoRemoteError):
        dvc.experiments.run(checkpoint_stage.addressing, params=["foo=2"])


def test_auto_push_self_remote(
    tmp_dir,
    dvc,
    scm,
    checkpoint_stage,
    local_remote,
    caplog,
    monkeypatch,
    clear_env,
):
    root_dir = str(tmp_dir)
    monkeypatch.setenv(DVC_EXP_GIT_REMOTE, root_dir)
    monkeypatch.setenv(DVC_EXP_AUTO_PUSH, "true")
    assert (
        dvc.experiments.run(checkpoint_stage.addressing, params=["foo=2"])
        != {}
    )

    with caplog.at_level(logging.WARNING, logger="dvc.repo.experiments"):
        assert (
            f"'{root_dir}' points to the current Git repo, experiment "
            "Git refs will not be pushed. But DVC cache and run cache will "
            "automatically be pushed to the default DVC remote (if any) "
            "on each experiment commit." in caplog.text
        )
