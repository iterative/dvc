import pytest

from dvc.fs import get_cloud_fs
from dvc.fs.webdav import WebDAVFileSystem, WebDAVSFileSystem

# Test configuration
url = "webdav://example.com/public.php/webdav"
user = "username"
userurl = f"webdav://{user}@example.com/public.php/webdav"
password = "password"
https_url = "webdavs://example.com/public.php/webdav"


def test_user():
    fs = WebDAVFileSystem(url=url, user=user)
    assert fs.user == user


def test_password():
    config = {"url": url, "user": user, "password": password}
    fs = WebDAVFileSystem(**config)
    assert fs.password == password


def test_ssl_verify_custom_cert():
    config = {
        "url": url,
        "ssl_verify": "/path/to/custom/cabundle.pem",
    }

    fs = WebDAVFileSystem(**config)
    assert fs.fs_args["verify"] == "/path/to/custom/cabundle.pem"


@pytest.mark.parametrize(
    "base_url, fs_cls",
    [(url, WebDAVFileSystem), (https_url, WebDAVSFileSystem)],
)
def test_remote_with_jobs(dvc, base_url, fs_cls):
    remote_config = {"url": base_url, "user": user}

    dvc.config["remote"]["dav"] = remote_config
    cls, config, _ = get_cloud_fs(dvc, name="dav")
    assert config["user"] == user
    assert cls is fs_cls

    # config from remote takes priority
    remote_config.update({"user": "admin"})
    cls, config, _ = get_cloud_fs(dvc, name="dav")
    assert config["user"] == "admin"
    assert cls is fs_cls
