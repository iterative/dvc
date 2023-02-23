import errno
import os

import pytest

from dvc.fs import LocalFileSystem
from dvc_data.hashfile.db.local import LocalHashFileDB
from dvc_data.hashfile.hash_info import HashInfo


def test_status_download_optimization(mocker, dvc):
    """When comparing the status to pull a remote cache,
    And the desired files to fetch are already on the local cache,
    Don't check the existence of the desired files on the remote cache
    """
    from dvc_data.hashfile.status import compare_status

    odb = LocalHashFileDB(LocalFileSystem(), os.getcwd())
    obj_ids = {
        HashInfo("md5", "acbd18db4cc2f85cedef654fccc4a4d8"),
        HashInfo("md5", "37b51d194a7513e45b56f6524f2d51f2"),
    }

    local_exists = [hash_info.value for hash_info in obj_ids]
    mocker.patch.object(odb, "oids_exist", return_value=local_exists)

    src_odb = mocker.Mock()

    compare_status(src_odb, odb, obj_ids, check_deleted=False)
    assert src_odb.oids_exist.call_count == 0


@pytest.mark.parametrize("link_name", ["hardlink", "symlink"])
def test_is_protected(tmp_dir, dvc, link_name):
    odb = dvc.cache.local
    fs = odb.fs
    link_method = getattr(fs, link_name)

    (tmp_dir / "foo").write_text("foo")

    foo = tmp_dir / "foo"
    link = tmp_dir / "link"

    link_method(foo, link)

    assert not odb.is_protected(foo)
    assert not odb.is_protected(link)

    odb.protect(foo)

    assert odb.is_protected(foo)
    assert odb.is_protected(link)

    odb.unprotect(link)

    assert not odb.is_protected(link)
    if os.name == "nt" and link_name == "hardlink":
        # NOTE: NTFS doesn't allow deleting read-only files, which forces us to
        # set write perms on the link, which propagates to the source.
        assert not odb.is_protected(foo)
    else:
        assert odb.is_protected(foo)


@pytest.mark.parametrize("err", [errno.EPERM, errno.EACCES, errno.EROFS])
def test_protect_ignore_errors(tmp_dir, dvc, mocker, err):
    tmp_dir.gen("foo", "foo")

    mock_chmod = mocker.patch("os.chmod", side_effect=OSError(err, "something"))
    dvc.cache.local.protect("foo")
    assert mock_chmod.called


@pytest.mark.parametrize("err", [errno.EPERM, errno.EACCES, errno.EROFS])
def test_set_exec_ignore_errors(tmp_dir, dvc, mocker, err):
    tmp_dir.gen("foo", "foo")

    mock_chmod = mocker.patch("os.chmod", side_effect=OSError(err, "something"))
    dvc.cache.local.set_exec("foo")
    assert mock_chmod.called


def test_staging_file(tmp_dir, dvc):
    from dvc_data.hashfile import check
    from dvc_data.hashfile.build import build
    from dvc_data.hashfile.transfer import transfer

    tmp_dir.gen("foo", "foo")
    fs = LocalFileSystem()

    local_odb = dvc.cache.local
    staging_odb, _, obj = build(local_odb, (tmp_dir / "foo").fs_path, fs, "md5")

    assert not local_odb.exists(obj.hash_info.value)
    assert staging_odb.exists(obj.hash_info.value)

    with pytest.raises(FileNotFoundError):
        check(local_odb, obj)
    check(staging_odb, obj)

    transfer(staging_odb, local_odb, {obj.hash_info}, hardlink=True)
    check(local_odb, obj)
    check(staging_odb, obj)

    path = local_odb.oid_to_path(obj.hash_info.value)
    assert fs.exists(path)


def test_staging_dir(tmp_dir, dvc):
    from dvc_data.hashfile import check
    from dvc_data.hashfile.build import build
    from dvc_data.hashfile.transfer import transfer

    tmp_dir.gen({"dir": {"foo": "foo", "bar": "bar"}})
    fs = LocalFileSystem()
    local_odb = dvc.cache.local

    staging_odb, _, obj = build(local_odb, (tmp_dir / "dir").fs_path, fs, "md5")

    assert not local_odb.exists(obj.hash_info.value)
    assert staging_odb.exists(obj.hash_info.value)

    with pytest.raises(FileNotFoundError):
        check(local_odb, obj)
    check(staging_odb, obj)

    transfer(staging_odb, local_odb, {obj.hash_info}, shallow=False, hardlink=True)
    check(local_odb, obj)
    check(staging_odb, obj)

    path = local_odb.oid_to_path(obj.hash_info.value)
    assert fs.exists(path)
