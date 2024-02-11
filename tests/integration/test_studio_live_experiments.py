import json

import pytest
from dvc_studio_client import env, post_live_metrics
from funcy import first

from dvc.env import (
    DVC_STUDIO_OFFLINE,
    DVC_STUDIO_REPO_URL,
    DVC_STUDIO_TOKEN,
    DVC_STUDIO_URL,
)
from dvc.repo import Repo
from dvc.utils.studio import get_dvc_experiment_parent_data, get_subrepo_relpath


@pytest.mark.studio
@pytest.mark.parametrize("tmp", [True, False])
@pytest.mark.parametrize("offline", [True, False])
def test_post_to_studio(
    M, tmp_dir, dvc, scm, exp_stage, mocker, monkeypatch, tmp, offline
):
    valid_response = mocker.MagicMock()
    valid_response.status_code = 200
    live_metrics = mocker.spy(post_live_metrics, "post_live_metrics")
    mocked_post = mocker.patch("requests.post", return_value=valid_response)

    monkeypatch.setenv(DVC_STUDIO_REPO_URL, "STUDIO_REPO_URL")
    monkeypatch.setenv(DVC_STUDIO_TOKEN, "STUDIO_TOKEN")
    monkeypatch.setenv(DVC_STUDIO_URL, "https://0.0.0.0")
    monkeypatch.setenv(DVC_STUDIO_OFFLINE, offline)

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
            "repo_url": "STUDIO_REPO_URL",
            "baseline_sha": baseline_sha,
            "name": name,
            "params": {"params.yaml": {"foo": 1}},
            "client": "dvc",
            "dvc_experiment_parent_data": {
                "author": {
                    "email": "dvctester@example.com",
                    "name": "DVC Tester",
                },
                "date": M.any,
                "message": "init",
                "parent_shas": M.any,
                "title": "init",
                "sha": baseline_sha,
            },
        }

        assert done_call.kwargs["json"] == {
            "type": "done",
            "repo_url": "STUDIO_REPO_URL",
            "baseline_sha": baseline_sha,
            "name": name,
            "client": "dvc",
            "experiment_rev": exp_rev,
            "metrics": {"metrics.yaml": {"data": {"foo": 1}}},
        }


@pytest.mark.studio
@pytest.mark.parametrize("tmp", [True, False])
def test_post_to_studio_custom_message(
    M, tmp_dir, dvc, scm, exp_stage, mocker, monkeypatch, tmp
):
    valid_response = mocker.MagicMock()
    valid_response.status_code = 200
    mocked_post = mocker.patch("requests.post", return_value=valid_response)

    monkeypatch.setenv(env.STUDIO_ENDPOINT, "https://0.0.0.0")
    monkeypatch.setenv(env.STUDIO_REPO_URL, "STUDIO_REPO_URL")
    monkeypatch.setenv(env.STUDIO_TOKEN, "STUDIO_TOKEN")

    baseline_sha = scm.get_rev()
    exp_rev = first(
        dvc.experiments.run(
            exp_stage.addressing, params=["foo=1"], tmp_dir=tmp, message="foo"
        )
    )
    name = dvc.experiments.get_exact_name([exp_rev])[exp_rev]
    assert mocked_post.call_count == 2

    start_call = mocked_post.call_args_list[0]

    assert start_call.kwargs["json"] == {
        "type": "start",
        "repo_url": "STUDIO_REPO_URL",
        "baseline_sha": baseline_sha,
        "name": name,
        "params": {"params.yaml": {"foo": 1}},
        "client": "dvc",
        "message": "foo",
        "dvc_experiment_parent_data": {
            "author": {
                "email": "dvctester@example.com",
                "name": "DVC Tester",
            },
            "date": M.any,
            "message": "init",
            "parent_shas": M.any,
            "title": "init",
            "sha": baseline_sha,
        },
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


@pytest.mark.studio
def test_get_dvc_experiment_parent_data(M, tmp_dir, scm, dvc):
    parent_shas = [scm.get_rev()]

    for i in range(5):
        tmp_dir.scm_gen({"metrics.json": json.dumps({"metric": i})}, commit=f"step {i}")
        parent_shas.insert(0, scm.get_rev())

    title = "a final commit with a fairly long message"
    message = f"{title}\nthat is split over two lines"

    tmp_dir.scm_gen({"metrics.json": json.dumps({"metric": 100})}, commit=message)

    head_sha = scm.get_rev()

    assert isinstance(head_sha, str)
    assert head_sha not in parent_shas

    dvc_experiment_parent_data = get_dvc_experiment_parent_data(dvc, head_sha)

    assert dvc_experiment_parent_data is not None
    assert isinstance(dvc_experiment_parent_data["date"], str)

    assert dvc_experiment_parent_data == {
        "author": {
            "email": "dvctester@example.com",
            "name": "DVC Tester",
        },
        "date": M.any,
        "message": message,
        "parent_shas": parent_shas,
        "title": title,
        "sha": head_sha,
    }, (
        "up to 100 parent_shas are sent "
        "to Studio these are used to identify where to insert the parent "
        "information if the baseline_rev does not exist in the Studio DB"
    )
