from dvc.fs.webdav import WebDAVFileSystem

# Test configuration
url = "webdavs://example.com/public.php/webdav"
user = "username"
userurl = f"webdavs://{user}@example.com/public.php/webdav"
password = "password"


def test_user(dvc):
    fs = WebDAVFileSystem(url=url, user=user)
    assert fs.user == user


def test_password(dvc):
    config = {"url": url, "user": user, "password": password}
    fs = WebDAVFileSystem(**config)
    assert fs.password == password
