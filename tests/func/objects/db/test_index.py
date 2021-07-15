import hashlib
import os

import pytest

from dvc.exceptions import DownloadError, UploadError
from dvc.fs.local import LocalFileSystem
from dvc.objects.db.base import ObjectDB
from dvc.objects.db.index import ObjectDBIndex
from dvc.objects.db.local import LocalObjectDB
from dvc.utils.fs import remove


@pytest.fixture(scope="function")
def index(tmp_dir, dvc, tmp_path_factory, mocker):
    url = os.fspath(tmp_path_factory.mktemp("upstream"))
    dvc.config["remote"]["upstream"] = {"url": url}
    dvc.config["core"]["remote"] = "upstream"

    # patch hashes_exist since the LocalRemote normally overrides
    # BaseFileSystem.hashes_exist.
    def hashes_exist(self, *args, **kwargs):
        return ObjectDB.hashes_exist(self, *args, **kwargs)

    mocker.patch.object(LocalObjectDB, "hashes_exist", hashes_exist)

    # force get_index to return index for local remotes

    def get_index_patched(odb):
        if os.fspath(odb.path_info).endswith(os.path.join(".dvc", "cache")):
            return None
        return ObjectDBIndex(
            odb.tmp_dir,
            hashlib.sha256(odb.path_info.url.encode("utf-8")).hexdigest(),
            odb.fs.CHECKSUM_DIR_SUFFIX,
        )

    mocker.patch("dvc.objects.db.get_index", get_index_patched)

    return get_index_patched(dvc.cloud.get_remote_odb("upstream"))


def test_indexed_on_status(tmp_dir, dvc, tmp_path_factory, index):
    foo = tmp_dir.dvc_gen({"foo": "foo content"})[0].outs[0]
    bar = tmp_dir.dvc_gen({"bar": {"baz": "baz content"}})[0].outs[0]
    baz_hash = bar.obj.trie.get(("baz",)).hash_info
    dvc.push()
    with index:
        index.clear()

    dvc.status(cloud=True)
    with index:
        assert {bar.hash_info.value, baz_hash.value} == set(index.hashes())
        assert [bar.hash_info.value] == list(index.dir_hashes())
        assert foo.hash_info.value not in index.hashes()


def test_indexed_on_push(tmp_dir, dvc, tmp_path_factory, index):
    foo = tmp_dir.dvc_gen({"foo": "foo content"})[0].outs[0]
    bar = tmp_dir.dvc_gen({"bar": {"baz": "baz content"}})[0].outs[0]
    baz_hash = bar.obj.trie.get(("baz",)).hash_info

    dvc.push()
    with index:
        assert {bar.hash_info.value, baz_hash.value} == set(index.hashes())
        assert [bar.hash_info.value] == list(index.dir_hashes())
        assert foo.hash_info.value not in index.hashes()


def test_indexed_dir_missing(tmp_dir, dvc, tmp_path_factory, index):
    bar = tmp_dir.dvc_gen({"bar": {"baz": "baz content"}})[0].outs[0]
    with index:
        index.update([bar.hash_info.value], [])
    dvc.status(cloud=True)
    with index:
        assert not list(index.hashes())


def test_clear_on_gc(tmp_dir, dvc, tmp_path_factory, index, mocker):
    (foo,) = tmp_dir.dvc_gen({"dir": {"foo": "foo content"}})
    dvc.push()
    dvc.remove(foo.relpath)

    with index:
        assert list(index.hashes())
    dvc.gc(workspace=True, cloud=True)
    with index:
        assert not list(index.hashes())


def test_clear_on_download_err(tmp_dir, dvc, tmp_path_factory, index, mocker):
    out = tmp_dir.dvc_gen({"dir": {"foo": "foo content"}})[0].outs[0]
    dvc.push()

    for _, entry in out.obj:
        remove(dvc.odb.local.get(entry.hash_info).path_info)
    remove(out.path_info)

    with index:
        assert list(index.hashes())

    mocker.patch(
        "dvc.fs.local.LocalFileSystem.upload_fobj", side_effect=Exception
    )
    with pytest.raises(DownloadError):
        dvc.pull()
    with index:
        assert not list(index.hashes())


def test_partial_upload(tmp_dir, dvc, tmp_path_factory, index, mocker):
    tmp_dir.dvc_gen({"foo": "foo content"})
    tmp_dir.dvc_gen({"bar": {"baz": "baz content"}})

    original = LocalFileSystem._upload

    def unreliable_upload(self, from_file, to_info, name=None, **kwargs):
        if "baz" in name:
            raise Exception("stop baz")
        return original(self, from_file, to_info, name, **kwargs)

    mocker.patch.object(LocalFileSystem, "upload_fobj", unreliable_upload)
    with pytest.raises(UploadError):
        dvc.push()
    with index:
        assert not list(index.hashes())
