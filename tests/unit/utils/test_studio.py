from urllib.parse import urljoin

import pytest
from requests import Response

from dvc.env import (
    DVC_STUDIO_OFFLINE,
    DVC_STUDIO_REPO_URL,
    DVC_STUDIO_TOKEN,
    DVC_STUDIO_URL,
)
from dvc.utils.studio import (
    STUDIO_URL,
    config_to_env,
    env_to_config,
    get_dvc_experiment_parent_data,
    notify_refs,
)

CONFIG = {"offline": True, "repo_url": "repo_url", "token": "token", "url": "url"}


ENV = {
    DVC_STUDIO_OFFLINE: True,
    DVC_STUDIO_REPO_URL: "repo_url",
    DVC_STUDIO_TOKEN: "token",
    DVC_STUDIO_URL: "url",
}


@pytest.mark.studio
@pytest.mark.parametrize(
    "status_code, side_effect",
    [
        (200, {}),  # success
        (401, {"detail": "unauthorized"}),  # should not fail on client errors
        (500, ValueError),  # should not fail even on server errors
    ],
)
def test_notify_refs(mocker, status_code, side_effect):
    response = Response()
    response.status_code = status_code
    mocker.patch.object(response, "json", side_effect=[side_effect])

    mock_post = mocker.patch("requests.Session.post", return_value=response)

    notify_refs(
        "git@github.com:iterative/dvc.git",
        "TOKEN",
        pushed=["p1", "p2"],
        removed=["r1", "r2"],
    )

    assert mock_post.called
    assert mock_post.call_args == mocker.call(
        urljoin(STUDIO_URL, "/webhook/dvc"),
        json={
            "repo_url": "git@github.com:iterative/dvc.git",
            "client": "dvc",
            "refs": {
                "pushed": ["p1", "p2"],
                "removed": ["r1", "r2"],
            },
        },
        headers={"Authorization": "token TOKEN"},
        timeout=5,
        allow_redirects=False,
    )


@pytest.mark.studio
def test_config_to_env():
    assert config_to_env(CONFIG) == ENV


@pytest.mark.studio
def test_env_to_config():
    assert env_to_config(ENV) == CONFIG


@pytest.mark.studio
def test_error_in_get_dvc_experiment_parent_data(mocker, scm, dvc):
    from dvc.scm import SCMError

    mocker.patch.object(scm, "resolve_commit", sideEffect=SCMError)

    assert get_dvc_experiment_parent_data(dvc, scm.get_rev()) is None


@pytest.mark.parametrize("func", ["get_rev", "resolve_commit"])
@pytest.mark.studio
def test_no_dvc_experiment_parent_data(mocker, scm, dvc, func):
    mocker.patch.object(scm, func, return_value=None)

    assert get_dvc_experiment_parent_data(dvc, scm.get_rev()) is None
