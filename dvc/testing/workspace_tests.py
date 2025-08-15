import os
from typing import Union

import pytest
from funcy import first

from dvc.exceptions import URLMissingError
from dvc.repo import Repo
from dvc.repo.ls_url import ls_url, parse_external_url
from dvc.utils.fs import remove


class TestImport:
    def test_import(self, tmp_dir, dvc, workspace):
        workspace.gen("file", "file")
        assert not (tmp_dir / "file").exists()  # sanity check
        dvc.imp_url("remote://workspace/file")
        assert (tmp_dir / "file").read_text() == "file"
        assert dvc.status() == {}

    @pytest.fixture
    def stage_md5(self):
        pytest.skip()

    @pytest.fixture
    def dir_md5(self):
        pytest.skip()

    def test_import_dir(self, tmp_dir, dvc, workspace, stage_md5, dir_md5):
        from dvc.cachemgr import CacheManager

        workspace.gen({"dir": {"file": "file", "subdir": {"subfile": "subfile"}}})

        # remove external cache to make sure that we don't need it
        # to import dirs
        with dvc.config.edit() as conf:
            del conf["cache"]
        dvc.cache = CacheManager(dvc)

        assert not (tmp_dir / "dir").exists()  # sanity check
        dvc.imp_url("remote://workspace/dir")
        assert set(os.listdir(tmp_dir / "dir")) == {"file", "subdir"}
        assert (tmp_dir / "dir" / "file").read_text() == "file"
        assert list(os.listdir(tmp_dir / "dir" / "subdir")) == ["subfile"]
        assert (tmp_dir / "dir" / "subdir" / "subfile").read_text() == "subfile"

        assert dvc.status() == {}

        if stage_md5 is not None and dir_md5 is not None:
            assert (tmp_dir / "dir.dvc").read_text() == (
                f"md5: {stage_md5}\n"
                "frozen: true\n"
                "deps:\n"
                f"- md5: {dir_md5}\n"
                "  size: 11\n"
                "  nfiles: 2\n"
                "  hash: md5\n"
                "  path: remote://workspace/dir\n"
                "outs:\n"
                "- md5: b6dcab6ccd17ca0a8bf4a215a37d14cc.dir\n"
                "  size: 11\n"
                "  nfiles: 2\n"
                "  hash: md5\n"
                "  path: dir\n"
            )

    @pytest.fixture
    def is_object_storage(self):
        pytest.skip()

    def test_import_empty_dir(self, tmp_dir, dvc, workspace, is_object_storage):
        # prefix based storage services (e.g s3) doesn't have the real concept
        # of directories. So instead we create an empty file that ends with a
        # trailing slash in order to actually support this operation
        if is_object_storage:
            contents: Union[str, dict[str, str]] = ""
        else:
            contents = {}

        workspace.gen({"empty_dir/": contents})

        dvc.imp_url("remote://workspace/empty_dir/")

        empty_dir = tmp_dir / "empty_dir"
        assert empty_dir.is_dir()
        assert tuple(empty_dir.iterdir()) == ()


class TestImportURLVersionAware:
    def test_import_file(self, tmp_dir, dvc, remote_version_aware):
        remote_version_aware.gen("file", "file")
        dvc.imp_url("remote://upstream/file", version_aware=True)
        stage = first(dvc.index.stages)
        assert not stage.outs[0].can_push
        assert (tmp_dir / "file").read_text() == "file"
        assert dvc.status() == {}

        orig_version_id = stage.deps[0].meta.version_id
        orig_def_path = stage.deps[0].def_path

        dvc.cache.local.clear()
        remove(tmp_dir / "file")
        dvc.pull()
        assert (tmp_dir / "file").read_text() == "file"

        (remote_version_aware / "file").write_text("modified")
        assert dvc.status().get("file.dvc") == [
            {"changed deps": {"remote://upstream/file": "update available"}},
            {"changed outs": {"file": "not in cache"}},
        ]
        dvc.update(str(tmp_dir / "file.dvc"))
        assert (tmp_dir / "file").read_text() == "modified"
        assert dvc.status() == {}

        stage = first(dvc.index.stages)
        assert orig_version_id != stage.deps[0].meta.version_id
        assert orig_def_path == stage.deps[0].def_path

        dvc.cache.local.clear()
        remove(tmp_dir / "file")
        dvc.pull()
        assert (tmp_dir / "file").read_text() == "modified"

    def test_import_dir(self, tmp_dir, dvc, remote_version_aware):
        remote_version_aware.gen({"data_dir": {"subdir": {"file": "file"}}})
        dvc.imp_url("remote://upstream/data_dir", version_aware=True)
        stage = first(dvc.index.stages)
        assert not stage.outs[0].can_push
        assert (tmp_dir / "data_dir" / "subdir" / "file").read_text() == "file"
        assert dvc.status() == {}

        dvc.cache.local.clear()
        remove(tmp_dir / "data_dir")
        dvc.pull()
        assert (tmp_dir / "data_dir" / "subdir" / "file").read_text() == "file"

        (remote_version_aware / "data_dir" / "subdir" / "file").write_text("modified")
        (remote_version_aware / "data_dir" / "new_file").write_text("new")
        assert dvc.status().get("data_dir.dvc") == [
            {"changed deps": {"remote://upstream/data_dir": "modified"}},
            {"changed outs": {"data_dir": "not in cache"}},
        ]
        dvc.update(str(tmp_dir / "data_dir.dvc"))
        assert (tmp_dir / "data_dir" / "subdir" / "file").read_text() == "modified"
        assert (tmp_dir / "data_dir" / "new_file").read_text() == "new"
        assert dvc.status() == {}

        dvc.cache.local.clear()
        remove(tmp_dir / "data_dir")
        dvc.pull()
        assert (tmp_dir / "data_dir" / "subdir" / "file").read_text() == "modified"
        assert (tmp_dir / "data_dir" / "new_file").read_text() == "new"

    def test_import_no_download(self, tmp_dir, dvc, remote_version_aware, scm):
        remote_version_aware.gen({"data_dir": {"subdir": {"file": "file"}}})
        dvc.imp_url("remote://upstream/data_dir", version_aware=True, no_download=True)
        scm.add(["data_dir.dvc", ".gitignore"])
        scm.commit("v1")
        scm.tag("v1")

        stage = first(dvc.index.stages)
        assert not stage.outs[0].can_push

        (remote_version_aware / "data_dir" / "foo").write_text("foo")
        dvc.update(no_download=True)
        assert dvc.pull() == {
            "modified": [],
            "added": ["data_dir" + os.sep],
            "deleted": [],
            "stats": {"fetched": 2, "modified": 0, "added": 2, "deleted": 0},
        }
        assert (tmp_dir / "data_dir").read_text() == {
            "foo": "foo",
            "subdir": {"file": "file"},
        }
        scm.add(["data_dir.dvc", ".gitignore"])
        scm.commit("update")

        scm.checkout("v1")
        dvc.cache.local.clear()
        remove(tmp_dir / "data_dir")
        assert dvc.pull() == {
            "modified": [],
            "added": ["data_dir" + os.sep],
            "deleted": [],
            "stats": {"fetched": 1, "modified": 0, "added": 1, "deleted": 0},
        }
        assert (tmp_dir / "data_dir").read_text() == {"subdir": {"file": "file"}}

        dvc.commit(force=True)
        assert dvc.status() == {}


def match_files(fs, entries, expected):
    entries_content = {(fs.normpath(d["path"]), d["isdir"]) for d in entries}
    expected_content = {(fs.normpath(d["path"]), d["isdir"]) for d in expected}
    assert entries_content == expected_content


class TestLsUrl:
    @pytest.mark.parametrize("fname", ["foo", "foo.dvc", "dir/foo"])
    def test_file(self, cloud, fname):
        cloud.gen({fname: "foo contents"})
        fs, fs_path = parse_external_url(cloud.url, cloud.config)
        result = ls_url(str(cloud / fname), fs_config=cloud.config)
        match_files(fs, result, [{"path": fs.join(fs_path, fname), "isdir": False}])

    def test_dir(self, cloud):
        cloud.gen({"dir/foo": "foo contents", "dir/subdir/bar": "bar contents"})
        if not (cloud / "dir").is_dir():
            pytest.skip("Cannot create directories on this cloud")
        fs, _ = parse_external_url(cloud.url, cloud.config)
        result = ls_url(str(cloud / "dir"), fs_config=cloud.config)
        match_files(
            fs,
            result,
            [
                {"path": "foo", "isdir": False},
                {"path": "subdir", "isdir": True},
            ],
        )

    def test_recursive(self, cloud):
        cloud.gen({"dir/foo": "foo contents", "dir/subdir/bar": "bar contents"})
        if not (cloud / "dir").is_dir():
            pytest.skip("Cannot create directories on this cloud")
        fs, _ = parse_external_url(cloud.url, cloud.config)
        result = ls_url(str(cloud / "dir"), fs_config=cloud.config, recursive=True)
        match_files(
            fs,
            result,
            [
                {"path": "foo", "isdir": False},
                {"path": "subdir/bar", "isdir": False},
            ],
        )

        result = ls_url(
            str(cloud / "dir"), fs_config=cloud.config, recursive=True, maxdepth=0
        )
        match_files(
            fs,
            result,
            [{"path": (cloud / "dir").fs_path, "isdir": False}],
        )

        result = ls_url(
            str(cloud / "dir"), fs_config=cloud.config, recursive=True, maxdepth=1
        )
        match_files(
            fs,
            result,
            [
                {"path": "foo", "isdir": False},
                {"path": "subdir", "isdir": True},
            ],
        )

        result = ls_url(
            str(cloud / "dir"), fs_config=cloud.config, recursive=True, maxdepth=2
        )
        match_files(
            fs,
            result,
            [
                {"path": "foo", "isdir": False},
                {"path": "subdir/bar", "isdir": False},
            ],
        )

    def test_nonexistent(self, cloud):
        with pytest.raises(URLMissingError):
            ls_url(str(cloud / "dir"), fs_config=cloud.config)


class TestGetUrl:
    def test_get_file(self, cloud, tmp_dir):
        cloud.gen({"foo": "foo contents"})

        Repo.get_url(str(cloud / "foo"), "foo_imported", fs_config=cloud.config)

        assert (tmp_dir / "foo_imported").is_file()
        assert (tmp_dir / "foo_imported").read_text() == "foo contents"

    def test_get_dir(self, cloud, tmp_dir):
        cloud.gen({"foo": {"foo": "foo contents"}})
        if not (cloud / "foo").is_dir():
            pytest.skip("Cannot create directories on this cloud")

        Repo.get_url(str(cloud / "foo"), "foo_imported", fs_config=cloud.config)

        assert (tmp_dir / "foo_imported").is_dir()
        assert (tmp_dir / "foo_imported" / "foo").is_file()
        assert (tmp_dir / "foo_imported" / "foo").read_text() == "foo contents"

    @pytest.mark.parametrize("dname", [".", "dir", "dir/subdir"])
    def test_get_url_to_dir(self, cloud, tmp_dir, dname):
        cloud.gen({"src": {"foo": "foo contents"}})
        if not (cloud / "src").is_dir():
            pytest.skip("Cannot create directories on this cloud")
        tmp_dir.gen({"dir": {"subdir": {}}})

        Repo.get_url(str(cloud / "src" / "foo"), dname, fs_config=cloud.config)

        assert (tmp_dir / dname).is_dir()
        assert (tmp_dir / dname / "foo").read_text() == "foo contents"

    def test_get_url_nonexistent(self, cloud):
        with pytest.raises(URLMissingError):
            Repo.get_url(str(cloud / "nonexistent"), fs_config=cloud.config)


class TestToRemote:
    def test_add_to_remote(self, tmp_dir, dvc, remote, workspace):
        workspace.gen("foo", "foo")

        url = "remote://workspace/foo"
        [stage] = dvc.add(url, to_remote=True)

        assert not (tmp_dir / "foo").exists()
        assert (tmp_dir / "foo.dvc").exists()

        assert len(stage.deps) == 0
        assert len(stage.outs) == 1

        hash_info = stage.outs[0].hash_info
        meta = stage.outs[0].meta
        assert hash_info.name == "md5"
        assert hash_info.value == "acbd18db4cc2f85cedef654fccc4a4d8"
        assert (
            remote / "files" / "md5" / "ac" / "bd18db4cc2f85cedef654fccc4a4d8"
        ).read_text() == "foo"
        assert meta.size == len("foo")

    def test_import_url_to_remote_file(self, tmp_dir, dvc, workspace, remote):
        workspace.gen("foo", "foo")

        url = "remote://workspace/foo"
        stage = dvc.imp_url(url, to_remote=True)

        assert stage.deps[0].hash_info.value is not None
        assert not (tmp_dir / "foo").exists()
        assert (tmp_dir / "foo.dvc").exists()

        assert len(stage.deps) == 1
        assert stage.deps[0].def_path == url
        assert len(stage.outs) == 1

        hash_info = stage.outs[0].hash_info
        assert hash_info.name == "md5"
        assert hash_info.value == "acbd18db4cc2f85cedef654fccc4a4d8"
        assert (
            remote / "files" / "md5" / "ac" / "bd18db4cc2f85cedef654fccc4a4d8"
        ).read_text() == "foo"
        assert stage.outs[0].meta.size == len("foo")

    def test_import_url_to_remote_dir(self, tmp_dir, dvc, workspace, remote):
        import json

        workspace.gen(
            {
                "data": {
                    "foo": "foo",
                    "bar": "bar",
                    "sub_dir": {"baz": "sub_dir/baz"},
                }
            }
        )

        url = "remote://workspace/data"
        stage = dvc.imp_url(url, to_remote=True)

        assert not (tmp_dir / "data").exists()
        assert (tmp_dir / "data.dvc").exists()

        assert len(stage.deps) == 1
        assert stage.deps[0].def_path == url
        assert len(stage.outs) == 1

        hash_info = stage.outs[0].hash_info
        assert hash_info.name == "md5"
        assert hash_info.value == "55d05978954d1b2cd7b06aedda9b9e43.dir"
        file_parts = json.loads(
            (
                remote / "files" / "md5" / "55" / "d05978954d1b2cd7b06aedda9b9e43.dir"
            ).read_text()
        )

        assert len(file_parts) == 3
        assert {file_part["relpath"] for file_part in file_parts} == {
            "foo",
            "bar",
            "sub_dir/baz",
        }

        for file_part in file_parts:
            md5 = file_part["md5"]
            assert (
                remote / "files" / "md5" / md5[:2] / md5[2:]
            ).read_text() == file_part["relpath"]
