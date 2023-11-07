from unittest import mock

from dvc.cli import main
from dvc.utils.studio import STUDIO_URL


def test_auth_login_invalid_scope():
    assert main(["auth", "login", "--scopes", "invalid!"]) == 1


@mock.patch("dvc.utils.studio.check_token_authorization")
@mock.patch("dvc.utils.studio.start_device_login")
def test_auth_login(mock_start_device_login, mock_check_token_authorization, dvc):
    mock_response = {
        "verification_uri": STUDIO_URL + "/auth/device-login",
        "user_code": "MOCKCODE",
        "device_code": "random-value",
        "token_uri": STUDIO_URL + "/api/device-login/token",
    }
    mock_start_device_login.return_value = mock_response
    mock_check_token_authorization.return_value = None

    assert main(["auth", "login"]) == 1

    mock_check_token_authorization.return_value = "isat_access_token"
    assert main(["auth", "login"]) == 0
    config = dvc.config.load_one("global")
    assert config["studio"]["token"] == "isat_access_token"
    assert config["studio"]["url"] == STUDIO_URL


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
