import pytest

from dvc.fs import get_cloud_fs
from dvc_webdav import WebDAVFileSystem, WebDAVSFileSystem
from tests.utils.asserts import issubset

url_fmt = "{scheme}://{user}@example.com/public.php/webdav"
url = "webdav://example.com/public.php/webdav"
user = "username"
password = "password"
token = "4MgjsNM5aSJjxIKM"
custom_auth_header = "Custom-Header"


def test_common():
    fs = WebDAVFileSystem(
        url=url,
        cert_path="cert/path",
        key_path="key/path",
        ssl_verify="bundle.pem",
        timeout=10,
        prefix="/public.php/webdav",
        user=None,
        password=None,
        ask_password=False,
        token=None,
        custom_auth_header=None,
    )
    assert issubset(
        {
            "headers": {},
            "auth": None,
            "base_url": url,
            "cert": ("cert/path", "key/path"),
            "verify": "bundle.pem",
            "timeout": 10,
        },
        fs.fs_args,
    )
    assert fs.prefix == "/public.php/webdav"


def test_user():
    fs = WebDAVFileSystem(url=url, user=user)
    assert issubset({"auth": (user, None), "headers": {}}, fs.fs_args)


def test_password():
    config = {"url": url, "user": user, "password": password}
    fs = WebDAVFileSystem(**config)
    assert issubset(
        {
            "headers": {},
            "auth": (user, password),
        },
        fs.fs_args,
    )


def test_token():
    config = {"token": token, "url": url}
    fs = WebDAVFileSystem(**config)
    assert issubset(
        {"headers": {"Authorization": f"Bearer {token}"}, "auth": None},
        fs.fs_args,
    )


def test_ask_password(mocker):
    ask_password_mocked = mocker.patch("dvc_webdav.ask_password", return_value="pass")
    host = "host"

    # it should not ask for password as password is set
    config = {
        "url": url,
        "user": user,
        "password": password,
        "ask_password": True,
        "host": host,
    }
    fs = WebDAVFileSystem(**config)
    assert issubset({"auth": (user, password), "headers": {}}, fs.fs_args)

    config.pop("password")
    fs = WebDAVFileSystem(**config)
    assert issubset({"auth": (user, "pass"), "headers": {}}, fs.fs_args)
    ask_password_mocked.assert_called_once_with(host, user)


def test_custom_auth_header():
    config = {
        "url": url,
        "custom_auth_header": custom_auth_header,
        "password": password,
    }
    fs = WebDAVFileSystem(**config)
    assert issubset(
        {"headers": {custom_auth_header: password}, "auth": None},
        fs.fs_args,
    )


def test_ask_password_custom_auth_header(mocker):
    ask_password_mocked = mocker.patch("dvc_webdav.ask_password", return_value="pass")
    host = "host"

    # it should not ask for password as password is set
    config = {
        "url": url,
        "custom_auth_header": custom_auth_header,
        "password": password,
        "ask_password": True,
        "host": host,
    }
    fs = WebDAVFileSystem(**config)
    assert issubset(
        {"headers": {custom_auth_header: password}, "auth": None}, fs.fs_args
    )

    config.pop("password")
    fs = WebDAVFileSystem(**config)
    assert issubset({"headers": {custom_auth_header: "pass"}, "auth": None}, fs.fs_args)
    ask_password_mocked.assert_called_once_with(host, custom_auth_header)


def test_ssl_verify_custom_cert():
    config = {"url": url, "ssl_verify": "/path/to/custom/cabundle.pem"}

    fs = WebDAVFileSystem(**config)
    assert fs.fs_args["verify"] == "/path/to/custom/cabundle.pem"


@pytest.mark.parametrize(
    "base_url, fs_cls",
    [
        (url_fmt.format(scheme="webdav", user=user), WebDAVFileSystem),
        (url_fmt.format(scheme="webdavs", user=user), WebDAVSFileSystem),
    ],
)
def test_remote_with_jobs(dvc, base_url, fs_cls):
    scheme = "http" + ("s" if fs_cls is WebDAVSFileSystem else "")
    remote_config = {"url": base_url}

    dvc.config["remote"]["dav"] = remote_config
    cls, config, _ = get_cloud_fs(dvc.config, name="dav")
    assert config["user"] == user
    assert f"{scheme}://{user}@example.com" in config["host"]
    assert cls is fs_cls

    # config from remote takes priority
    remote_config.update({"user": "admin"})
    cls, config, _ = get_cloud_fs(dvc.config, name="dav")
    assert config["user"] == "admin"
    assert f"{scheme}://{user}@example.com" in config["host"]
    assert cls is fs_cls
