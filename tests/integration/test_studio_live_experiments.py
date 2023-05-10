import pytest
from dvc_studio_client import post_live_metrics
from funcy import first

from dvc.env import (
    DVC_STUDIO_OFFLINE,
    DVC_STUDIO_REPO_URL,
    DVC_STUDIO_TOKEN,
    DVC_STUDIO_URL,
)


@pytest.mark.parametrize("tmp", [True, False])
@pytest.mark.parametrize("offline", [True, False])
def test_post_to_studio(
    tmp_dir, dvc, scm, exp_stage, mocker, monkeypatch, tmp, offline
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
