from dvc.tree.webdav import WebDAVTree

# Test configuration
url = "webdavs://example.com/public.php/webdav"
user = "username"
userurl = f"webdavs://{user}@example.com/public.php/webdav"
password = "password"


# Test minimum requiered configuration (url)
def test_init(dvc):
    config = {"url": url}
    tree = WebDAVTree(dvc, config)

    assert tree.path_info == url


# Test username from configuration
def test_user(dvc):
    config = {"url": url, "user": user}
    tree = WebDAVTree(dvc, config)

    assert tree.user == user
    assert tree.path_info.user == user


# Test username extraction from url
def test_userurl(dvc):
    config = {"url": userurl}
    tree = WebDAVTree(dvc, config)

    assert tree.path_info == userurl
    assert tree.user == user
    assert tree.path_info.user == user


# test password from config
def test_password(dvc):
    config = {"url": url, "user": user, "password": password}
    tree = WebDAVTree(dvc, config)

    assert tree.password == password
