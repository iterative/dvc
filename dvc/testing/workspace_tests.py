import os

import pytest

from dvc.exceptions import URLMissingError
from dvc.repo import Repo
from dvc.repo.ls_url import ls_url, parse_external_url


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
        from dvc.odbmgr import ODBManager

        workspace.gen(
            {"dir": {"file": "file", "subdir": {"subfile": "subfile"}}}
        )

        # remove external cache to make sure that we don't need it
        # to import dirs
        with dvc.config.edit() as conf:
            del conf["cache"]
        dvc.odb = ODBManager(dvc)

        assert not (tmp_dir / "dir").exists()  # sanity check
        dvc.imp_url("remote://workspace/dir")
        assert set(os.listdir(tmp_dir / "dir")) == {"file", "subdir"}
        assert (tmp_dir / "dir" / "file").read_text() == "file"
        assert list(os.listdir(tmp_dir / "dir" / "subdir")) == ["subfile"]
        assert (
            tmp_dir / "dir" / "subdir" / "subfile"
        ).read_text() == "subfile"

        assert dvc.status() == {}

        if stage_md5 is not None and dir_md5 is not None:
            assert (tmp_dir / "dir.dvc").read_text() == (
                f"md5: {stage_md5}\n"
                "frozen: true\n"
                "deps:\n"
                f"- md5: {dir_md5}\n"
                "  size: 11\n"
                "  nfiles: 2\n"
                "  path: remote://workspace/dir\n"
                "outs:\n"
                "- md5: b6dcab6ccd17ca0a8bf4a215a37d14cc.dir\n"
                "  size: 11\n"
                "  nfiles: 2\n"
                "  path: dir\n"
            )

    @pytest.fixture
    def is_object_storage(self):
        pytest.skip()

    def test_import_empty_dir(
        self, tmp_dir, dvc, workspace, is_object_storage
    ):
        # prefix based storage services (e.g s3) doesn't have the real concept
        # of directories. So instead we create an empty file that ends with a
        # trailing slash in order to actually support this operation
        if is_object_storage:
            contents = ""
        else:
            contents = {}

        workspace.gen({"empty_dir/": contents})

        dvc.imp_url("remote://workspace/empty_dir/")

        empty_dir = tmp_dir / "empty_dir"
        assert empty_dir.is_dir()
        assert tuple(empty_dir.iterdir()) == ()


class TestAdd:
    @pytest.fixture
    def hash_name(self):
        pytest.skip()

    @pytest.fixture
    def hash_value(self):
        pytest.skip()

    @pytest.fixture
    def dir_hash_value(self):
        pytest.skip()

    def test_add(self, tmp_dir, dvc, workspace, hash_name, hash_value):
        from dvc.stage.exceptions import StageExternalOutputsError

        workspace.gen("file", "file")

        with pytest.raises(StageExternalOutputsError):
            dvc.add(workspace.url)

        dvc.add("remote://workspace/file")
        assert (tmp_dir / "file.dvc").read_text() == (
            "outs:\n"
            f"- {hash_name}: {hash_value}\n"
            "  size: 4\n"
            "  path: remote://workspace/file\n"
        )
        assert (workspace / "file").read_text() == "file"
        assert (
            workspace / "cache" / hash_value[:2] / hash_value[2:]
        ).read_text() == "file"

        assert dvc.status() == {}

    def test_add_dir(self, tmp_dir, dvc, workspace, hash_name, dir_hash_value):
        workspace.gen(
            {"dir": {"file": "file", "subdir": {"subfile": "subfile"}}}
        )

        dvc.add("remote://workspace/dir")
        assert (tmp_dir / "dir.dvc").read_text() == (
            "outs:\n"
            f"- {hash_name}: {dir_hash_value}\n"
            "  size: 11\n"
            "  nfiles: 2\n"
            "  path: remote://workspace/dir\n"
        )
        assert (
            workspace / "cache" / dir_hash_value[:2] / dir_hash_value[2:]
        ).is_file()


def match_files(fs, entries, expected):
    entries_content = {
        (fs.path.normpath(d["path"]), d["isdir"]) for d in entries
    }
    expected_content = {
        (fs.path.normpath(d["path"]), d["isdir"]) for d in expected
    }
    assert entries_content == expected_content


class TestLsUrl:
    @pytest.mark.parametrize("fname", ["foo", "foo.dvc", "dir/foo"])
    def test_file(self, cloud, fname):
        cloud.gen({fname: "foo contents"})
        fs, fs_path = parse_external_url(cloud.url, cloud.config)
        result = ls_url(str(cloud / fname), config=cloud.config)
        match_files(
            fs,
            result,
            [{"path": fs.path.join(fs_path, fname), "isdir": False}],
        )

    def test_dir(self, cloud):
        cloud.gen(
            {"dir/foo": "foo contents", "dir/subdir/bar": "bar contents"}
        )
        if not (cloud / "dir").is_dir():
            pytest.skip("Cannot create directories on this cloud")
        fs, _ = parse_external_url(cloud.url, cloud.config)
        result = ls_url(str(cloud / "dir"), config=cloud.config)
        match_files(
            fs,
            result,
            [
                {"path": "foo", "isdir": False},
                {"path": "subdir", "isdir": True},
            ],
        )

    def test_recursive(self, cloud):
        cloud.gen(
            {"dir/foo": "foo contents", "dir/subdir/bar": "bar contents"}
        )
        if not (cloud / "dir").is_dir():
            pytest.skip("Cannot create directories on this cloud")
        fs, _ = parse_external_url(cloud.url, cloud.config)
        result = ls_url(
            str(cloud / "dir"), config=cloud.config, recursive=True
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
            ls_url(str(cloud / "dir"), config=cloud.config)


class TestGetUrl:
    def test_get_file(self, cloud, tmp_dir):
        cloud.gen({"foo": "foo contents"})

        Repo.get_url(str(cloud / "foo"), "foo_imported", config=cloud.config)

        assert (tmp_dir / "foo_imported").is_file()
        assert (tmp_dir / "foo_imported").read_text() == "foo contents"

    def test_get_dir(self, cloud, tmp_dir):
        cloud.gen({"foo": {"foo": "foo contents"}})
        if not (cloud / "foo").is_dir():
            pytest.skip("Cannot create directories on this cloud")

        Repo.get_url(str(cloud / "foo"), "foo_imported", config=cloud.config)

        assert (tmp_dir / "foo_imported").is_dir()
        assert (tmp_dir / "foo_imported" / "foo").is_file()
        assert (tmp_dir / "foo_imported" / "foo").read_text() == "foo contents"

    @pytest.mark.parametrize("dname", [".", "dir", "dir/subdir"])
    def test_get_url_to_dir(self, cloud, tmp_dir, dname):
        cloud.gen({"src": {"foo": "foo contents"}})
        if not (cloud / "src").is_dir():
            pytest.skip("Cannot create directories on this cloud")
        tmp_dir.gen({"dir": {"subdir": {}}})

        Repo.get_url(str(cloud / "src" / "foo"), dname, config=cloud.config)

        assert (tmp_dir / dname).is_dir()
        assert (tmp_dir / dname / "foo").read_text() == "foo contents"

    def test_get_url_nonexistent(self, cloud):
        with pytest.raises(URLMissingError):
            Repo.get_url(str(cloud / "nonexistent"), config=cloud.config)
