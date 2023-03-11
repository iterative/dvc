import pytest
from dvc_studio_client import env
from funcy import first


@pytest.mark.parametrize("tmp", [True, False])
def test_post_to_studio(tmp_dir, dvc, scm, exp_stage, mocker, monkeypatch, tmp):
    valid_response = mocker.MagicMock()
    valid_response.status_code = 200
    mocked_post = mocker.patch("requests.post", return_value=valid_response)

    monkeypatch.setenv(env.STUDIO_ENDPOINT, "https://0.0.0.0")
    monkeypatch.setenv(env.STUDIO_REPO_URL, "STUDIO_REPO_URL")
    monkeypatch.setenv(env.STUDIO_TOKEN, "STUDIO_TOKEN")

    baseline_sha = scm.get_rev()
    exp_rev = first(
        dvc.experiments.run(exp_stage.addressing, params=["foo=1"], tmp_dir=tmp)
    )
    name = dvc.experiments.get_exact_name([exp_rev])[exp_rev]
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
