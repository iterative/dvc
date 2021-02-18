import pytest

from dvc.fs.webdav import WebDAVFileSystem

# Test configuration
url = "webdavs://example.com/public.php/webdav"
user = "username"
userurl = f"webdavs://{user}@example.com/public.php/webdav"
password = "password"


# Test minimum requiered configuration (url)
def test_init(dvc):
    config = {"url": url}
    fs = WebDAVFileSystem(dvc, config)

    assert fs.path_info == url


# Test username from configuration
@pytest.mark.parametrize(
    "config", [{"url": url, "user": user}, {"url": userurl}]
)
def test_user(dvc, config):
    fs = WebDAVFileSystem(dvc, config)

    assert fs.user == user


# Test username extraction from url
def test_userurl(dvc):
    config = {"url": userurl}
    fs = WebDAVFileSystem(dvc, config)

    assert fs.path_info == userurl
    assert fs.user == user
    assert fs.path_info.user == user


# test password from config
def test_password(dvc):
    config = {"url": url, "user": user, "password": password}
    fs = WebDAVFileSystem(dvc, config)

    assert fs.password == password
