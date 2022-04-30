import pytest

from dvc.data.db import get_index
from dvc.exceptions import DownloadError, UploadError
from dvc.fs.local import LocalFileSystem
from dvc.utils.fs import remove
from tests.utils import clean_staging


@pytest.fixture
def index(dvc, local_remote, mocker):
    odb = dvc.cloud.get_remote_odb("upstream")
    return get_index(odb)


def test_indexed_on_status(tmp_dir, dvc, index):
    foo = tmp_dir.dvc_gen({"foo": "foo content"})[0].outs[0]
    bar = tmp_dir.dvc_gen({"bar": {"baz": "baz content"}})[0].outs[0]
    baz_hash = bar.obj._trie.get(("baz",))[1]
    clean_staging()
    dvc.push()
    index.clear()

    dvc.status(cloud=True)
    assert {bar.hash_info.value, baz_hash.value} == set(index.hashes())
    assert [bar.hash_info.value] == list(index.dir_hashes())
    assert foo.hash_info.value not in index.hashes()


def test_indexed_on_push(tmp_dir, dvc, index):
    foo = tmp_dir.dvc_gen({"foo": "foo content"})[0].outs[0]
    bar = tmp_dir.dvc_gen({"bar": {"baz": "baz content"}})[0].outs[0]
    baz_hash = bar.obj._trie.get(("baz",))[1]
    clean_staging()

    dvc.push()
    assert {bar.hash_info.value, baz_hash.value} == set(index.hashes())
    assert [bar.hash_info.value] == list(index.dir_hashes())
    assert foo.hash_info.value not in index.hashes()


def test_indexed_dir_missing(tmp_dir, dvc, index):
    bar = tmp_dir.dvc_gen({"bar": {"baz": "baz content"}})[0].outs[0]
    index.update([bar.hash_info.value], [])
    dvc.status(cloud=True)
    assert not list(index.hashes())


def test_clear_on_gc(tmp_dir, dvc, index):
    (foo,) = tmp_dir.dvc_gen({"dir": {"foo": "foo content"}})
    dvc.push()
    dvc.remove(foo.relpath)

    assert list(index.hashes())
    dvc.gc(workspace=True, cloud=True)
    assert not list(index.hashes())


def test_clear_on_download_err(tmp_dir, dvc, index, mocker):
    out = tmp_dir.dvc_gen({"dir": {"foo": "foo content"}})[0].outs[0]
    dvc.push()

    for _, _, oid in out.obj:
        remove(dvc.odb.local.get(oid).fs_path)
    remove(out.fs_path)

    assert list(index.hashes())

    mocker.patch("dvc.fs.utils.transfer", side_effect=Exception)
    with pytest.raises(DownloadError):
        dvc.pull()
    assert not list(index.hashes())


def test_partial_upload(tmp_dir, dvc, index, mocker):
    tmp_dir.dvc_gen({"foo": "foo content"})
    tmp_dir.dvc_gen({"bar": {"baz": "baz content"}})

    original = LocalFileSystem.upload

    def unreliable_upload(self, from_file, to_info, name=None, **kwargs):
        if "baz" in name:
            raise Exception("stop baz")
        return original(self, from_file, to_info, name, **kwargs)

    mocker.patch("dvc.fs.utils.transfer", unreliable_upload)
    with pytest.raises(UploadError):
        dvc.push()
    assert not list(index.hashes())
