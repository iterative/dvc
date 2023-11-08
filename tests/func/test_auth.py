from dvc.cli import main
from dvc.commands.auth import DEFAULT_SCOPES
from dvc.utils.studio import STUDIO_URL
from tests.unit.command.test_auth import MOCK_RESPONSE
from tests.unit.utils.test_studio import mock_response


def test_auth_expired(mocker, M):
    mock_post = mocker.patch(
        "requests.Session.post",
        side_effect=[
            mock_response(mocker, 200, MOCK_RESPONSE),
            mock_response(mocker, 400, {"detail": "authorization_expired"}),
        ],
    )

    assert main(["auth", "login"]) == 1

    assert mock_post.call_count == 2
    assert mock_post.call_args_list == [
        mocker.call(
            f"{STUDIO_URL}/api/device-login",
            json={"client_name": "dvc", "token_name": M.any, "scopes": DEFAULT_SCOPES},
            headers=None,
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


def test_auth_success(mocker, dvc):
    mocker.patch("time.sleep")
    mock_post = mocker.patch(
        "requests.Session.post",
        side_effect=[
            mock_response(mocker, 200, MOCK_RESPONSE),
            mock_response(mocker, 400, {"detail": "authorization_pending"}),
            mock_response(mocker, 200, {"access_token": "isat_access_token"}),
        ],
    )

    assert (
        main(
            [
                "auth",
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

    assert mock_post.call_count == 3
    assert mock_post.call_args_list == [
        mocker.call(
            "https://example.com/api/device-login",
            json={"client_name": "dvc", "token_name": "token_name", "scopes": "live"},
            headers=None,
            timeout=5,
            allow_redirects=False,
        ),
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
