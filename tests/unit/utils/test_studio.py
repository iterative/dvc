from urllib.parse import urljoin

import pytest
from dulwich.porcelain import remote_add as git_remote_add
from requests import Response

from dvc.env import (
    DVC_EXP_GIT_REMOTE,
    DVC_STUDIO_OFFLINE,
    DVC_STUDIO_REPO_URL,
    DVC_STUDIO_TOKEN,
    DVC_STUDIO_URL,
)
from dvc.utils.studio import (
    STUDIO_URL,
    config_to_env,
    env_to_config,
    get_repo_url,
    notify_refs,
)

CONFIG = {"offline": True, "repo_url": "repo_url", "token": "token", "url": "url"}


ENV = {
    DVC_STUDIO_OFFLINE: True,
    DVC_STUDIO_REPO_URL: "repo_url",
    DVC_STUDIO_TOKEN: "token",
    DVC_STUDIO_URL: "url",
}


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


def test_config_to_env():
    assert config_to_env(CONFIG) == ENV


def test_env_to_config():
    assert env_to_config(ENV) == CONFIG


@pytest.mark.parametrize(
    "exp_git_remote, repo_url",
    [
        (None, None),
        ("origin", "git@url"),
        ("http://url", "http://url"),
    ],
)
def test_get_repo_url(dvc, scm, monkeypatch, exp_git_remote, repo_url):
    git_remote_add(scm.root_dir, "origin", "git@url")

    if exp_git_remote:
        monkeypatch.setenv(DVC_EXP_GIT_REMOTE, exp_git_remote)
    assert get_repo_url(dvc) == repo_url
