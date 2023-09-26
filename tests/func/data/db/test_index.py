import os

import pytest

from dvc.exceptions import DownloadError, UploadError
from dvc.utils.fs import remove
from dvc_data.hashfile.db import get_index


@pytest.fixture
def index(tmp_dir, dvc, local_remote):
    odb = dvc.cloud.get_remote_odb("upstream")
    return get_index(odb)


def test_indexed_on_status(tmp_dir, dvc, index):
    foo = tmp_dir.dvc_gen({"foo": "foo content"})[0].outs[0]
    bar = tmp_dir.dvc_gen({"bar": {"baz": "baz content"}})[0].outs[0]
    baz_hash = bar.obj._trie.get(("baz",))[1]
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

    assert list(index.hashes())

    for _, _, hi in out.obj:
        remove(dvc.cache.local.get(hi.value).path)
        remove(dvc.cloud.get_remote().odb.get(hi.value).path)
    remove(out.fs_path)

    with pytest.raises(DownloadError):
        dvc.pull()
    assert not list(index.hashes())


def test_partial_upload(tmp_dir, dvc, index, mocker):
    from dvc_objects.fs import generic

    tmp_dir.dvc_gen({"foo": "foo content"})
    baz = tmp_dir.dvc_gen({"bar": {"baz": "baz content"}})[0].outs[0]

    original = generic.transfer
    odb = dvc.cloud.get_remote_odb("upstream")

    def unreliable_upload(from_fs, from_info, to_fs, to_info, **kwargs):
        on_error = kwargs["on_error"]
        assert on_error
        if isinstance(from_info, str):
            from_info = [from_info]
        else:
            from_info = list(from_info)
        if isinstance(to_info, str):
            to_info = [to_info]
        else:
            to_info = list(to_info)
        for i in range(len(from_info) - 1, -1, -1):
            from_i = from_info[i]
            to_i = to_info[i]
            if os.path.abspath(to_i) == os.path.abspath(
                odb.get(baz.hash_info.value).path
            ):
                if on_error:
                    on_error(from_i, to_i, Exception("stop baz"))
                del from_info[i]
                del to_info[i]

        return original(from_fs, from_info, to_fs, to_info, **kwargs)

    mocker.patch("dvc_objects.fs.generic.transfer", unreliable_upload)
    with pytest.raises(UploadError):
        dvc.push()
    assert not list(index.hashes())
