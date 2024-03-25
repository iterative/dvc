import pytest
from funcy import first

from dvc.env import (
    DVC_EXP_GIT_REMOTE,
    DVC_STUDIO_OFFLINE,
    DVC_STUDIO_REPO_URL,
    DVC_STUDIO_TOKEN,
    DVC_STUDIO_URL,
)
from dvc.repo import Repo
from dvc.testing.scripts import COPY_SCRIPT
from dvc.utils.studio import get_subrepo_relpath
from dvc_studio_client import env, post_live_metrics


@pytest.mark.studio
@pytest.mark.parametrize("tmp", [True, False])
@pytest.mark.parametrize("offline", [True, False])
@pytest.mark.parametrize("dvc_exp_git_remote", [None, "DVC_EXP_GIT_REMOTE"])
def test_post_to_studio(
    tmp_dir, dvc, scm, exp_stage, mocker, monkeypatch, tmp, offline, dvc_exp_git_remote
):
    valid_response = mocker.MagicMock()
    valid_response.status_code = 200
    live_metrics = mocker.spy(post_live_metrics, "post_live_metrics")
    mocked_post = mocker.patch("requests.post", return_value=valid_response)

    monkeypatch.setenv(DVC_STUDIO_REPO_URL, "STUDIO_REPO_URL")
    monkeypatch.setenv(DVC_STUDIO_TOKEN, "STUDIO_TOKEN")
    monkeypatch.setenv(DVC_STUDIO_URL, "https://0.0.0.0")
    monkeypatch.setenv(DVC_STUDIO_OFFLINE, offline)
    if dvc_exp_git_remote:
        monkeypatch.setenv(DVC_EXP_GIT_REMOTE, dvc_exp_git_remote)

    baseline_sha = scm.get_rev()
    exp_rev = first(
        dvc.experiments.run(exp_stage.addressing, params=["foo=1"], tmp_dir=tmp)
    )
    name = dvc.experiments.get_exact_name([exp_rev])[exp_rev]

    assert live_metrics.call_count == 2
    start_call, done_call = live_metrics.call_args_list

    if offline:
        assert mocked_post.call_count == 0

    else:
        start_call, done_call = live_metrics.call_args_list
        assert start_call.kwargs["dvc_studio_config"]["token"] == "STUDIO_TOKEN"
        assert start_call.kwargs["dvc_studio_config"]["repo_url"] == "STUDIO_REPO_URL"

        assert mocked_post.call_count == 2

        start_call, done_call = mocked_post.call_args_list

        assert start_call.kwargs["json"] == {
            "type": "start",
            "repo_url": dvc_exp_git_remote or "STUDIO_REPO_URL",
            "baseline_sha": baseline_sha,
            "name": name,
            "params": {"params.yaml": {"foo": 1}},
            "client": "dvc",
        }

        assert done_call.kwargs["json"] == {
            "type": "done",
            "repo_url": dvc_exp_git_remote or "STUDIO_REPO_URL",
            "baseline_sha": baseline_sha,
            "name": name,
            "client": "dvc",
            "experiment_rev": exp_rev,
            "metrics": {"metrics.yaml": {"data": {"foo": 1}}},
        }


@pytest.mark.studio
@pytest.mark.parametrize("tmp", [True, False])
def test_post_to_studio_subdir(tmp_dir, scm, mocker, monkeypatch, tmp):
    live_exp_subdir = "project_a"

    tmp_dir.scm_gen(
        {
            live_exp_subdir: {
                "params.yaml": "foo: 1",
                "metrics.yaml": "foo: 1",
                "copy.py": COPY_SCRIPT.encode("utf-8"),
            },
        },
        commit="git init",
    )

    project_a_dvc = Repo.init(tmp_dir / live_exp_subdir, subdir=True)
    with monkeypatch.context() as m:
        m.chdir(project_a_dvc.root_dir)

        exp_stage = project_a_dvc.run(
            cmd="python copy.py params.yaml metrics.yaml",
            metrics_no_cache=["metrics.yaml"],
            params=["foo"],
            name="copy-file",
        )

        scm.add(
            [
                ".gitignore",
                "copy.py",
                "dvc.lock",
                "dvc.yaml",
                "metrics.yaml",
                "params.yaml",
            ]
        )
        scm.commit("dvc init project_a")

    valid_response = mocker.MagicMock()
    valid_response.status_code = 200
    mocked_post = mocker.patch("requests.post", return_value=valid_response)

    monkeypatch.setenv(env.STUDIO_ENDPOINT, "https://0.0.0.0")
    monkeypatch.setenv(env.STUDIO_REPO_URL, "STUDIO_REPO_URL")
    monkeypatch.setenv(env.STUDIO_TOKEN, "STUDIO_TOKEN")

    baseline_sha = scm.get_rev()
    with monkeypatch.context() as m:
        m.chdir(project_a_dvc.root_dir)
        exp_rev = first(
            project_a_dvc.experiments.run(
                exp_stage.addressing, params=["foo=24"], tmp_dir=tmp
            )
        )

    name = project_a_dvc.experiments.get_exact_name([exp_rev])[exp_rev]
    project_a_dvc.close()
    assert mocked_post.call_count == 2

    start_call = mocked_post.call_args_list[0]

    assert start_call.kwargs["json"] == {
        "type": "start",
        "repo_url": "STUDIO_REPO_URL",
        "baseline_sha": baseline_sha,
        "name": name,
        "params": {"params.yaml": {"foo": 24}},
        "subdir": live_exp_subdir,
        "client": "dvc",
    }


@pytest.mark.studio
def test_monorepo_relpath(tmp_dir, scm):
    from dvc.repo.destroy import destroy

    tmp_dir.gen({"project_a": {}, "subdir/project_b": {}})

    non_monorepo = Repo.init(tmp_dir)
    assert get_subrepo_relpath(non_monorepo) == ""

    destroy(non_monorepo)

    monorepo_project_a = Repo.init(tmp_dir / "project_a", subdir=True)

    assert get_subrepo_relpath(monorepo_project_a) == "project_a"

    monorepo_project_b = Repo.init(tmp_dir / "subdir" / "project_b", subdir=True)

    assert get_subrepo_relpath(monorepo_project_b) == "subdir/project_b"


@pytest.mark.studio
def test_virtual_monorepo_relpath(tmp_dir, scm):
    from dvc.fs.git import GitFileSystem
    from dvc.repo.destroy import destroy

    tmp_dir.gen({"project_a": {}, "subdir/project_b": {}})
    scm.commit("initial commit")
    gfs = GitFileSystem(scm=scm, rev="master")

    non_monorepo = Repo.init(tmp_dir)
    non_monorepo.fs = gfs
    non_monorepo.root_dir = "/"

    assert get_subrepo_relpath(non_monorepo) == ""

    destroy(non_monorepo)

    monorepo_project_a = Repo.init(tmp_dir / "project_a", subdir=True)
    monorepo_project_a.fs = gfs
    monorepo_project_a.root_dir = "/project_a"

    assert get_subrepo_relpath(monorepo_project_a) == "project_a"

    monorepo_project_b = Repo.init(tmp_dir / "subdir" / "project_b", subdir=True)
    monorepo_project_b.fs = gfs
    monorepo_project_b.root_dir = "/subdir/project_b"

    assert get_subrepo_relpath(monorepo_project_b) == "subdir/project_b"
