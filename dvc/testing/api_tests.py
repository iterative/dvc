import pytest

from dvc import api
from dvc.api import DVCFileSystem
from dvc.utils.fs import remove


class TestAPI:
    def test_get_url(self, tmp_dir, dvc, remote):
        tmp_dir.dvc_gen("foo", "foo")

        expected_url = (
            remote / "files" / "md5" / "ac/bd18db4cc2f85cedef654fccc4a4d8"
        ).url
        assert api.get_url("foo") == expected_url

    def test_open(self, tmp_dir, dvc, remote):
        tmp_dir.dvc_gen(
            {
                "foo": "foo-text",
                "dir": {"bar": "bar-text"},
            }
        )
        dvc.push()

        # Remove cache to force download
        remove(dvc.cache.local.path)

        with api.open("foo") as fobj:
            assert fobj.read() == "foo-text"

        with api.open("dir/bar") as fobj:
            assert fobj.read() == "bar-text"

    @pytest.mark.parametrize("clear_cache", [True, False], ids=["cache", "no_cache"])
    @pytest.mark.parametrize(
        "fs_kwargs",
        [
            {},
            {"url": "{path}"},
            {"url": "{path}", "rev": "master"},
            {"url": "file://{posixpath}"},
            {"url": "file://{posixpath}", "rev": "master"},
        ],
        ids=["current", "local", "local_rev", "git", "git_rev"],
    )
    def test_filesystem(
        self,
        M,
        tmp_dir,
        make_tmp_dir,
        scm,
        dvc,
        remote,
        fs_kwargs,
        clear_cache,
    ):
        fs_kwargs = fs_kwargs.copy()

        tmp_dir.scm_gen({"scripts": {"script1": "script1"}}, commit="scripts")
        tmp_dir.dvc_gen({"data": {"foo": "foo", "bar": "bar"}}, commit="data")
        dvc.push()

        if clear_cache:
            remove(dvc.cache.repo.path)

        if url := fs_kwargs.get("url"):
            fs_kwargs["url"] = url.format(path=tmp_dir, posixpath=tmp_dir.as_posix())

        fs = DVCFileSystem(**fs_kwargs)

        assert fs.ls("/", detail=False) == M.unordered(
            "/.gitignore", "/scripts", "/data"
        )
        assert fs.ls("scripts", detail=False) == ["scripts/script1"]
        assert fs.ls("data", detail=False) == M.unordered("data/foo", "data/bar")

        data_info = M.dict(
            name="/data",
            type="directory",
            dvc_info=M.dict(isdvc=True, isout=True),
        )
        scripts_info = M.dict(name="/scripts", type="directory", isexec=False)

        assert sorted(fs.ls("/"), key=lambda i: i["name"]) == [
            M.dict(name="/.gitignore", type="file", isexec=False),
            data_info,
            scripts_info,
        ]

        with pytest.raises(FileNotFoundError):
            fs.info("/not-existing-path")

        assert fs.info("/") == M.dict(name="/", isexec=False, type="directory")
        assert fs.info("/data") == data_info
        assert fs.info("/scripts") == scripts_info
        assert fs.info("/data/foo") == M.dict(name="/data/foo", type="file")
        assert fs.info("/scripts/script1") == M.dict(
            name="/scripts/script1", type="file"
        )

        assert not fs.isdvc("/")
        assert fs.isdvc("/data")
        assert fs.isdvc("/data/foo")
        assert not fs.isdvc("/scripts")
        assert not fs.isdvc("/scripts/script1")

        with pytest.raises((IsADirectoryError, PermissionError)):
            fs.open("data")
        with pytest.raises((IsADirectoryError, PermissionError)):
            fs.open("scripts")
        with fs.open("/data/foo") as fobj:
            assert fobj.read() == b"foo"
        with fs.open("/scripts/script1") as fobj:
            assert fobj.read() == b"script1"

        tmp = make_tmp_dir("temp-download")
        fs.get_file("data/foo", (tmp / "foo").fs_path)
        assert (tmp / "foo").read_text() == "foo"

        fs.get_file("scripts/script1", (tmp / "script1").fs_path)
        assert (tmp / "script1").read_text() == "script1"

        fs.get("/", (tmp / "all").fs_path, recursive=True)
        assert (tmp / "all").read_text() == {
            ".gitignore": "/data\n",
            "data": {"bar": "bar", "foo": "foo"},
            "scripts": {"script1": "script1"},
        }
