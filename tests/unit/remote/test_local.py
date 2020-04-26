import os
import errno

import pytest

from dvc.cache import NamedCache
from dvc.path_info import PathInfo
from dvc.remote.local import LocalRemote


def test_status_download_optimization(mocker, dvc):
    """When comparing the status to pull a remote cache,
        And the desired files to fetch are already on the local cache,
        Don't check the existence of the desired files on the remote cache
    """
    remote = LocalRemote(dvc, {})

    infos = NamedCache()
    infos.add("local", "acbd18db4cc2f85cedef654fccc4a4d8", "foo")
    infos.add("local", "37b51d194a7513e45b56f6524f2d51f2", "bar")

    local_exists = list(infos["local"])
    mocker.patch.object(remote, "cache_exists", return_value=local_exists)

    other_remote = mocker.Mock()
    other_remote.url = "other_remote"
    other_remote.cache_exists.return_value = []

    remote.status(infos, other_remote, download=True)

    assert other_remote.cache_exists.call_count == 0


@pytest.mark.parametrize("link_name", ["hardlink", "symlink"])
def test_is_protected(tmp_dir, dvc, link_name):
    remote = LocalRemote(dvc, {})
    link_method = getattr(remote, link_name)

    (tmp_dir / "foo").write_text("foo")

    foo = PathInfo(tmp_dir / "foo")
    link = PathInfo(tmp_dir / "link")

    link_method(foo, link)

    assert not remote.is_protected(foo)
    assert not remote.is_protected(link)

    remote.protect(foo)

    assert remote.is_protected(foo)
    assert remote.is_protected(link)

    remote.unprotect(link)

    assert not remote.is_protected(link)
    if os.name == "nt" and link_name == "hardlink":
        # NOTE: NTFS doesn't allow deleting read-only files, which forces us to
        # set write perms on the link, which propagates to the source.
        assert not remote.is_protected(foo)
    else:
        assert remote.is_protected(foo)


@pytest.mark.parametrize("err", [errno.EPERM, errno.EACCES])
def test_protect_ignore_errors(tmp_dir, mocker, err):
    tmp_dir.gen("foo", "foo")
    foo = PathInfo("foo")
    remote = LocalRemote(None, {})

    remote.protect(foo)

    mock_chmod = mocker.patch(
        "os.chmod", side_effect=OSError(err, "something")
    )
    remote.protect(foo)
    assert mock_chmod.called


def test_protect_ignore_erofs(tmp_dir, mocker):
    tmp_dir.gen("foo", "foo")
    foo = PathInfo("foo")
    remote = LocalRemote(None, {})

    mock_chmod = mocker.patch(
        "os.chmod", side_effect=OSError(errno.EROFS, "read-only fs")
    )
    remote.protect(foo)
    assert mock_chmod.called
