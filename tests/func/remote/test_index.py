import pytest

from dvc.compat import fspath
from dvc.exceptions import DownloadError
from dvc.remote.base import RemoteBASE
from dvc.remote.local import RemoteLOCAL
from dvc.utils.fs import remove


@pytest.fixture(scope="function")
def remote(tmp_dir, dvc, tmp_path_factory, mocker):
    url = fspath(tmp_path_factory.mktemp("upstream"))
    dvc.config["remote"]["upstream"] = {"url": url}
    dvc.config["core"]["remote"] = "upstream"
    remote = dvc.cloud.get_remote("upstream")

    # patch cache_exists since the local implementation
    # normally overrides RemoteBASE.cache_exists.
    def cache_exists(self, *args, **kwargs):
        return RemoteBASE.cache_exists(self, *args, **kwargs)

    mocker.patch.object(RemoteLOCAL, "cache_exists", cache_exists)
    with remote.index:
        return remote


def test_indexed_on_status(tmp_dir, dvc, tmp_path_factory, remote, mocker):
    foo = tmp_dir.dvc_gen({"foo": "foo content"})[0].outs[0]
    bar = tmp_dir.dvc_gen({"bar": {"baz": "baz content"}})[0].outs[0]
    baz = bar.dir_cache[0]
    dvc.push()

    expected = {foo.checksum, bar.checksum, baz["md5"]}
    mocked_replace = mocker.patch.object(remote.INDEX_CLS, "replace_all")
    dvc.status(cloud=True, clear_index=True)
    mocked_replace.assert_called_with(expected)


def test_indexed_on_push(tmp_dir, dvc, tmp_path_factory, remote, mocker):
    foo = tmp_dir.dvc_gen({"foo": "foo content"})[0].outs[0]
    bar = tmp_dir.dvc_gen({"bar": {"baz": "baz content"}})[0].outs[0]
    baz = bar.dir_cache[0]

    mocked_update = mocker.patch.object(remote.INDEX_CLS, "update")
    dvc.push()
    call_args = mocked_update.call_args
    dir_checksums, file_checksums = call_args[0]
    assert [bar.checksum] == list(dir_checksums)
    assert [foo.checksum, baz["md5"]] == list(file_checksums)


def test_indexed_dir_missing(tmp_dir, dvc, tmp_path_factory, remote, mocker):
    bar = tmp_dir.dvc_gen({"bar": {"baz": "baz content"}})[0].outs[0]
    mocker.patch.object(
        remote.INDEX_CLS, "intersection", return_value=[bar.checksum]
    )
    mocked_clear = mocker.patch.object(remote.INDEX_CLS, "clear")
    dvc.status(cloud=True)
    mocked_clear.assert_called_with()


def test_clear_index(tmp_dir, dvc, tmp_path_factory, remote, mocker):
    mocked_clear = mocker.patch.object(remote.INDEX_CLS, "clear")
    dvc.status(cloud=True, clear_index=True)
    mocked_clear.assert_called_with()


def test_clear_on_gc(tmp_dir, dvc, tmp_path_factory, remote, mocker):
    (foo,) = tmp_dir.dvc_gen({"foo": "foo content"})
    dvc.push()
    dvc.remove(foo.relpath)

    # RemoteLOCAL.index.clear will be called twice in this case
    # once for local cache and once for the upstream remote
    mocked_clear = mocker.patch.object(remote.INDEX_CLS, "clear")
    dvc.gc(workspace=True, cloud=True)
    assert len(mocked_clear.mock_calls) == 2


def test_clear_on_download_err(tmp_dir, dvc, tmp_path_factory, remote, mocker):
    tmp_dir.dvc_gen({"foo": "foo content"})
    dvc.push()
    remove(dvc.cache.local.cache_dir)

    mocked_clear = mocker.patch.object(remote.INDEX_CLS, "clear")
    mocker.patch.object(RemoteLOCAL, "_download", side_effect=Exception)
    with pytest.raises(DownloadError):
        dvc.pull()
    mocked_clear.assert_called_once_with()
