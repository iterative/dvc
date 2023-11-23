from requests import Response

from dvc.cli import main
from dvc.commands.studio import DEFAULT_SCOPES
from dvc.utils.studio import STUDIO_URL
from tests.unit.command.test_studio import MOCK_RESPONSE


def test_auth_expired(mocker, M):
    mock_login_post = mocker.patch(
        "requests.post", return_value=_mock_response(mocker, 200, MOCK_RESPONSE)
    )
    mock_poll_post = mocker.patch(
        "requests.Session.post",
        side_effect=[
            _mock_response(mocker, 400, {"detail": "authorization_expired"}),
        ],
    )

    assert main(["studio", "login"]) == 1

    assert mock_login_post.call_args == mocker.call(
        url=f"{STUDIO_URL}/api/device-login",
        json={
            "client_name": "dvc",
            "scopes": DEFAULT_SCOPES.split(","),
        },
        headers={"Content-type": "application/json"},
        timeout=5,
    )

    assert mock_poll_post.call_args_list == [
        mocker.call(
            f"{STUDIO_URL}/api/device-login/token",
            json={"code": "random-value"},
            timeout=5,
            allow_redirects=False,
        ),
    ]


def test_studio_success(mocker, dvc):
    mocker.patch("time.sleep")
    mock_login_post = mocker.patch(
        "requests.post", return_value=_mock_response(mocker, 200, MOCK_RESPONSE)
    )
    mock_poll_post = mocker.patch(
        "requests.Session.post",
        side_effect=[
            _mock_response(mocker, 400, {"detail": "authorization_pending"}),
            _mock_response(mocker, 200, {"access_token": "isat_access_token"}),
        ],
    )

    assert (
        main(
            [
                "studio",
                "login",
                "--name",
                "token_name",
                "--hostname",
                "https://example.com",
                "--scopes",
                "live",
            ]
        )
        == 0
    )

    assert mock_login_post.call_args_list == [
        mocker.call(
            url="https://example.com/api/device-login",
            json={"client_name": "dvc", "token_name": "token_name", "scopes": ["live"]},
            headers={"Content-type": "application/json"},
            timeout=5,
        )
    ]
    assert mock_poll_post.call_count == 2
    assert mock_poll_post.call_args_list == [
        mocker.call(
            f"{STUDIO_URL}/api/device-login/token",
            json={"code": "random-value"},
            timeout=5,
            allow_redirects=False,
        ),
        mocker.call(
            f"{STUDIO_URL}/api/device-login/token",
            json={"code": "random-value"},
            timeout=5,
            allow_redirects=False,
        ),
    ]

    config = dvc.config.load_one("global")
    assert config["studio"]["token"] == "isat_access_token"
    assert config["studio"]["url"] == "https://example.com"


def _mock_response(mocker, status_code, json):
    response = Response()
    response.status_code = status_code
    mocker.patch.object(response, "json", side_effect=[json])

    return response
