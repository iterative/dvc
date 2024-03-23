import pytest

from dvc import env
from dvc.cli import main
from dvc.utils.studio import STUDIO_URL
from dvc_studio_client.auth import AuthorizationExpiredError


@pytest.fixture(autouse=True)
def global_config_dir(monkeypatch, tmp_dir_factory):
    monkeypatch.setenv(
        env.DVC_GLOBAL_CONFIG_DIR, str(tmp_dir_factory.mktemp("studio-login"))
    )


def test_studio_login_token_check_failed(mocker):
    mocker.patch(
        "dvc_studio_client.auth.get_access_token",
        side_effect=AuthorizationExpiredError,
    )
    assert main(["studio", "login"]) == 1


def test_studio_login_success(mocker, dvc):
    mocker.patch(
        "dvc_studio_client.auth.get_access_token",
        return_value=("token_name", "isat_access_token"),
    )

    assert main(["studio", "login"]) == 0

    config = dvc.config.load_one("global")
    assert config["studio"]["token"] == "isat_access_token"
    assert config["studio"]["url"] == STUDIO_URL


def test_studio_login_arguments(mocker):
    mock = mocker.patch(
        "dvc_studio_client.auth.get_access_token",
        return_value=("token_name", "isat_access_token"),
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
                "experiments",
                "--no-open",
            ]
        )
        == 0
    )

    mock.assert_called_with(
        token_name="token_name",
        hostname="https://example.com",
        scopes="experiments",
        client_name="DVC",
        open_browser=False,
    )


def test_studio_logout(dvc):
    with dvc.config.edit("global") as conf:
        conf["studio"]["token"] = "isat_access_token"

    assert main(["studio", "logout"]) == 0
    config = dvc.config.load_one("global")
    assert "token" not in config["studio"]

    assert main(["studio", "logout"]) == 1


def test_studio_token(dvc, capsys):
    with dvc.config.edit("global") as conf:
        conf["studio"]["token"] = "isat_access_token"

    assert main(["studio", "token"]) == 0
    assert capsys.readouterr().out == "isat_access_token\n"

    with dvc.config.edit("global") as conf:
        del conf["studio"]["token"]

    assert main(["studio", "token"]) == 1
