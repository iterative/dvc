from requests import Response

from dvc.repo.experiments.push import STUDIO_ENDPOINT, _notify_studio


def test_notify_studio_for_exp_push(mocker):
    valid_response = Response()
    valid_response.status_code = 200
    mock_post = mocker.patch("requests.Session.post", return_value=valid_response)

    _notify_studio(
        ["ref1", "ref2", "ref3"],
        "git@github.com:iterative/dvc.git",
        "TOKEN",
    )

    assert mock_post.called
    assert mock_post.call_args == mocker.call(
        STUDIO_ENDPOINT,
        json={
            "repo_url": "git@github.com:iterative/dvc.git",
            "client": "dvc",
            "refs": ["ref1", "ref2", "ref3"],
        },
        headers={"Authorization": "token TOKEN"},
        timeout=5,
    )
