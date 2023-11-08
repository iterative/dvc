from unittest import mock

from dvc.cli import main
from dvc.commands.auth import DEFAULT_SCOPES
from dvc.utils.studio import STUDIO_URL

MOCK_RESPONSE = {
    "verification_uri": STUDIO_URL + "/auth/device-login",
    "user_code": "MOCKCODE",
    "device_code": "random-value",
    "token_uri": STUDIO_URL + "/api/device-login/token",
}


def test_auth_login_invalid_scope():
    assert main(["auth", "login", "--scopes", "invalid!"]) == 1


@mock.patch("dvc.utils.studio.check_token_authorization")
@mock.patch("dvc.utils.studio.start_device_login")
def test_auth_login_token_check_failed(
    mock_start_device_login, mock_check_token_authorization
):
    mock_start_device_login.return_value = MOCK_RESPONSE
    mock_check_token_authorization.return_value = None

    assert main(["auth", "login"]) == 1


@mock.patch("dvc.utils.studio.check_token_authorization")
@mock.patch("dvc.utils.studio.start_device_login")
def test_auth_login_success(
    mock_start_device_login, mock_check_token_authorization, dvc, M
):
    mock_start_device_login.return_value = MOCK_RESPONSE
    mock_check_token_authorization.return_value = "isat_access_token"

    assert main(["auth", "login"]) == 0
    assert mock_start_device_login.call_args.kwargs == {
        "base_url": STUDIO_URL,
        "data": {"client_name": "dvc", "scopes": DEFAULT_SCOPES, "token_name": M.any},
    }
    mock_check_token_authorization.assert_called_with(
        uri=MOCK_RESPONSE["token_uri"], device_code=MOCK_RESPONSE["device_code"]
    )

    config = dvc.config.load_one("global")
    assert config["studio"]["token"] == "isat_access_token"
    assert config["studio"]["url"] == STUDIO_URL


@mock.patch("dvc.utils.studio.check_token_authorization")
@mock.patch("dvc.utils.studio.start_device_login")
def test_auth_login_arguments(mock_start_device_login, mock_check_token_authorization):
    mock_start_device_login.return_value = MOCK_RESPONSE
    mock_check_token_authorization.return_value = "isat_access_token"

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

    mock_start_device_login.assert_called_with(
        data={"client_name": "dvc", "token_name": "token_name", "scopes": "live"},
        base_url="https://example.com",
    )
    mock_check_token_authorization.assert_called()


@mock.patch("dvc.utils.studio.check_token_authorization")
@mock.patch("dvc.utils.studio.start_device_login")
@mock.patch("webbrowser.open")
def test_auth_device_code(
    mock_webbrowser_open, mock_start_device_login, mock_check_token_authorization
):
    mock_start_device_login.return_value = MOCK_RESPONSE
    mock_check_token_authorization.return_value = "isat_access_token"

    assert main(["auth", "login", "--use-device-code"]) == 0

    mock_webbrowser_open.assert_not_called()


def test_auth_logout(dvc):
    with dvc.config.edit("global") as conf:
        conf["studio"]["token"] = "isat_access_token"

    assert main(["auth", "logout"]) == 0
    config = dvc.config.load_one("global")
    assert "token" not in config["studio"]

    assert main(["auth", "logout"]) == 1


@mock.patch("dvc.ui.ui.write")
def test_auth_token(mock_write, dvc):
    with dvc.config.edit("global") as conf:
        conf["studio"]["token"] = "isat_access_token"

    assert main(["auth", "token"]) == 0
    mock_write.assert_called_with("isat_access_token")

    with dvc.config.edit("global") as conf:
        del conf["studio"]["token"]

    assert main(["auth", "token"]) == 1
