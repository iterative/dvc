from unittest import mock

from dvc_studio_client.auth import AuthorizationExpired

from dvc.cli import main
from dvc.utils.studio import STUDIO_URL


def test_studio_login_invalid_scope():
    assert main(["studio", "login", "--scopes", "invalid!"]) == 1


@mock.patch("dvc_studio_client.auth.get_access_token")
def test_studio_login_token_check_failed(mock_get_access_token):
    mock_get_access_token.side_effect = AuthorizationExpired

    assert main(["studio", "login"]) == 1


@mock.patch("dvc_studio_client.auth.get_access_token")
def test_studio_login_success(mock_get_access_token, dvc):
    mock_get_access_token.return_value = ("token_name", "isat_access_token")
    assert main(["studio", "login"]) == 0

    config = dvc.config.load_one("global")
    assert config["studio"]["token"] == "isat_access_token"
    assert config["studio"]["url"] == STUDIO_URL


@mock.patch("dvc_studio_client.auth.get_access_token")
def test_studio_login_arguments(mock_get_access_token):
    mock_get_access_token.return_value = ("token_name", "isat+access_token")

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
                "experiments",
            ]
        )
        == 0
    )

    mock_get_access_token.assert_called_with(
        token_name="token_name",
        hostname="https://example.com",
        scopes="experiments",
        use_device_code=False,
        client_name="dvc",
    )


@mock.patch("dvc_studio_client.auth.get_access_token")
@mock.patch("webbrowser.open")
def test_studio_device_code(mock_webbrowser_open, mock_get_access_token):
    mock_get_access_token.return_value = ("token_name", "isat+access_token")

    assert main(["studio", "login", "--use-device-code"]) == 0

    mock_webbrowser_open.assert_not_called()


def test_studio_logout(dvc):
    with dvc.config.edit("global") as conf:
        conf["studio"]["token"] = "isat_access_token"

    assert main(["studio", "logout"]) == 0
    config = dvc.config.load_one("global")
    assert "token" not in config["studio"]

    assert main(["studio", "logout"]) == 1


@mock.patch("dvc.ui.ui.write")
def test_studio_token(mock_write, dvc):
    with dvc.config.edit("global") as conf:
        conf["studio"]["token"] = "isat_access_token"

    assert main(["studio", "token"]) == 0
    mock_write.assert_called_with("isat_access_token")

    with dvc.config.edit("global") as conf:
        del conf["studio"]["token"]

    assert main(["studio", "token"]) == 1
