import os

import pytest

from dvc.exceptions import DownloadError, UploadError
from dvc.remote.base import Remote
from dvc.remote.index import RemoteIndex
from dvc.remote.local import LocalRemote
from dvc.tree.local import LocalTree
from dvc.utils.fs import remove


@pytest.fixture(scope="function")
def remote(tmp_dir, dvc, tmp_path_factory, mocker):
    url = os.fspath(tmp_path_factory.mktemp("upstream"))
    dvc.config["remote"]["upstream"] = {"url": url}
    dvc.config["core"]["remote"] = "upstream"

    # patch hashes_exist since the LocalRemote normally overrides
    # BaseTree.hashes_exist.
    def hashes_exist(self, *args, **kwargs):
        return Remote.hashes_exist(self, *args, **kwargs)

    mocker.patch.object(LocalRemote, "hashes_exist", hashes_exist)

    # patch index class since LocalRemote normally overrides index class
    mocker.patch.object(LocalRemote, "INDEX_CLS", RemoteIndex)

    return dvc.cloud.get_remote("upstream")


def test_indexed_on_status(tmp_dir, dvc, tmp_path_factory, remote):
    foo = tmp_dir.dvc_gen({"foo": "foo content"})[0].outs[0]
    bar = tmp_dir.dvc_gen({"bar": {"baz": "baz content"}})[0].outs[0]
    baz = bar.dir_cache[0]
    dvc.push()
    with remote.index:
        remote.index.clear()

    dvc.status(cloud=True)
    with remote.index:
        assert {bar.checksum, baz["md5"]} == set(remote.index.hashes())
        assert [bar.checksum] == list(remote.index.dir_hashes())
        assert foo.checksum not in remote.index.hashes()


def test_indexed_on_push(tmp_dir, dvc, tmp_path_factory, remote):
    foo = tmp_dir.dvc_gen({"foo": "foo content"})[0].outs[0]
    bar = tmp_dir.dvc_gen({"bar": {"baz": "baz content"}})[0].outs[0]
    baz = bar.dir_cache[0]

    dvc.push()
    with remote.index:
        assert {bar.checksum, baz["md5"]} == set(remote.index.hashes())
        assert [bar.checksum] == list(remote.index.dir_hashes())
        assert foo.checksum not in remote.index.hashes()


def test_indexed_dir_missing(tmp_dir, dvc, tmp_path_factory, remote):
    bar = tmp_dir.dvc_gen({"bar": {"baz": "baz content"}})[0].outs[0]
    with remote.index:
        remote.index.update([bar.checksum], [])
    dvc.status(cloud=True)
    with remote.index:
        assert not list(remote.index.hashes())


def test_clear_on_gc(tmp_dir, dvc, tmp_path_factory, remote, mocker):
    (foo,) = tmp_dir.dvc_gen({"foo": "foo content"})
    dvc.push()
    dvc.remove(foo.relpath)

    mocked_clear = mocker.patch.object(remote.INDEX_CLS, "clear")
    dvc.gc(workspace=True, cloud=True)
    mocked_clear.assert_called_with()


def test_clear_on_download_err(tmp_dir, dvc, tmp_path_factory, remote, mocker):
    tmp_dir.dvc_gen({"foo": "foo content"})
    dvc.push()
    remove(dvc.cache.local.cache_dir)

    mocked_clear = mocker.patch.object(remote.INDEX_CLS, "clear")
    mocker.patch.object(LocalTree, "_download", side_effect=Exception)
    with pytest.raises(DownloadError):
        dvc.pull()
    mocked_clear.assert_called_once_with()


def test_partial_upload(tmp_dir, dvc, tmp_path_factory, remote, mocker):
    tmp_dir.dvc_gen({"foo": "foo content"})
    tmp_dir.dvc_gen({"bar": {"baz": "baz content"}})

    original = LocalTree._upload

    def unreliable_upload(self, from_file, to_info, name=None, **kwargs):
        if "baz" in name:
            raise Exception("stop baz")
        return original(self, from_file, to_info, name, **kwargs)

    mocker.patch.object(LocalTree, "_upload", unreliable_upload)
    with pytest.raises(UploadError):
        dvc.push()
    with remote.index:
        assert not list(remote.index.hashes())
