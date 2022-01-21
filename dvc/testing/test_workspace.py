import os

import pytest


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
        from dvc.data.db import ODBManager

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
