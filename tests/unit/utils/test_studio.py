from urllib.parse import urljoin

import pytest
import requests
from requests import Response

from dvc.env import (
    DVC_STUDIO_OFFLINE,
    DVC_STUDIO_REPO_URL,
    DVC_STUDIO_TOKEN,
    DVC_STUDIO_URL,
)
from dvc.utils.studio import (
    STUDIO_URL,
    check_token_authorization,
    config_to_env,
    env_to_config,
    notify_refs,
    start_device_login,
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


def test_start_device_login(mocker):
    mock_post = mocker.patch(
        "requests.Session.post",
        return_value=mock_response(mocker, 200, {"user_code": "MOCKCODE"}),
    )

    start_device_login(
        data={"client_name": "dvc", "token_name": "token_name", "scopes": "live"},
        base_url="https://example.com",
    )
    assert mock_post.called
    assert mock_post.call_args == mocker.call(
        "https://example.com/api/device-login",
        json={"client_name": "dvc", "token_name": "token_name", "scopes": "live"},
        headers=None,
        timeout=5,
        allow_redirects=False,
    )


def test_check_token_authorization_expired(mocker):
    mocker.patch("time.sleep")
    mock_post = mocker.patch(
        "requests.Session.post",
        side_effect=[
            mock_response(mocker, 400, {"detail": "authorization_pending"}),
            mock_response(mocker, 400, {"detail": "authorization_expired"}),
        ],
    )

    assert (
        check_token_authorization(
            uri="https://example.com/token_uri", device_code="random_device_code"
        )
        is None
    )

    assert mock_post.call_count == 2
    assert mock_post.call_args == mocker.call(
        "https://example.com/token_uri",
        json={"code": "random_device_code"},
        timeout=5,
        allow_redirects=False,
    )


def test_check_token_authorization_error(mocker):
    mocker.patch("time.sleep")
    mock_post = mocker.patch(
        "requests.Session.post",
        side_effect=[
            mock_response(mocker, 400, {"detail": "authorization_pending"}),
            mock_response(mocker, 500, {"detail": "unexpected_error"}),
        ],
    )

    with pytest.raises(requests.RequestException):
        check_token_authorization(
            uri="https://example.com/token_uri", device_code="random_device_code"
        )

    assert mock_post.call_count == 2
    assert mock_post.call_args == mocker.call(
        "https://example.com/token_uri",
        json={"code": "random_device_code"},
        timeout=5,
        allow_redirects=False,
    )


def test_check_token_authorization_success(mocker):
    mocker.patch("time.sleep")
    mock_post = mocker.patch(
        "requests.Session.post",
        side_effect=[
            mock_response(mocker, 400, {"detail": "authorization_pending"}),
            mock_response(mocker, 400, {"detail": "authorization_pending"}),
            mock_response(mocker, 200, {"access_token": "isat_token"}),
        ],
    )

    assert (
        check_token_authorization(
            uri="https://example.com/token_uri", device_code="random_device_code"
        )
        == "isat_token"
    )

    assert mock_post.call_count == 3
    assert mock_post.call_args == mocker.call(
        "https://example.com/token_uri",
        json={"code": "random_device_code"},
        timeout=5,
        allow_redirects=False,
    )


def mock_response(mocker, status_code, json):
    response = Response()
    response.status_code = status_code
    mocker.patch.object(response, "json", side_effect=[json])

    return response
