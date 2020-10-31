import os
from uuid import uuid4

import pytest

from dvc.cache import Cache
from dvc.dependency.base import DependencyDoesNotExistError
from dvc.main import main
from dvc.stage import Stage
from dvc.utils.fs import makedirs
from tests.basic_env import TestDvc


class TestCmdImport(TestDvc):
    def test(self):
        ret = main(["import-url", self.FOO, "import"])
        self.assertEqual(ret, 0)
        self.assertTrue(os.path.exists("import.dvc"))

        ret = main(["import-url", "non-existing-file", "import"])
        self.assertNotEqual(ret, 0)

    def test_unsupported(self):
        ret = main(["import-url", "unsupported://path", "import_unsupported"])
        self.assertNotEqual(ret, 0)


class TestDefaultOutput(TestDvc):
    def test(self):
        tmpdir = self.mkdtemp()
        filename = str(uuid4())
        tmpfile = os.path.join(tmpdir, filename)

        with open(tmpfile, "w") as fd:
            fd.write("content")

        ret = main(["import-url", tmpfile])
        self.assertEqual(ret, 0)
        self.assertTrue(os.path.exists(filename))
        with open(filename) as fd:
            self.assertEqual(fd.read(), "content")


def test_should_remove_outs_before_import(tmp_dir, dvc, mocker, erepo_dir):
    erepo_dir.gen({"foo": "foo"})

    remove_outs_call_counter = mocker.spy(Stage, "remove_outs")
    ret = main(["import-url", os.fspath(erepo_dir / "foo")])

    assert ret == 0
    assert remove_outs_call_counter.mock.call_count == 1


class TestImportFilename(TestDvc):
    def setUp(self):
        super().setUp()
        tmp_dir = self.mkdtemp()
        self.external_source = os.path.join(tmp_dir, "file")
        with open(self.external_source, "w") as fobj:
            fobj.write("content")

    def test(self):
        ret = main(["import-url", "--file", "bar.dvc", self.external_source])
        self.assertEqual(0, ret)
        self.assertTrue(os.path.exists("bar.dvc"))

        os.remove("bar.dvc")
        os.mkdir("sub")

        path = os.path.join("sub", "bar.dvc")
        ret = main(["import-url", "--file", path, self.external_source])
        self.assertEqual(0, ret)
        self.assertTrue(os.path.exists(path))


@pytest.mark.parametrize("dname", [".", "dir", "dir/subdir"])
def test_import_url_to_dir(dname, tmp_dir, dvc):
    tmp_dir.gen({"data_dir": {"file": "file content"}})
    src = os.path.join("data_dir", "file")

    makedirs(dname, exist_ok=True)

    stage = dvc.imp_url(src, dname)

    dst = tmp_dir / dname / "file"

    assert stage.outs[0].path_info == dst
    assert os.path.isdir(dname)
    assert dst.read_text() == "file content"


def test_import_stage_accompanies_target(tmp_dir, dvc, erepo_dir):
    with erepo_dir.chdir():
        erepo_dir.dvc_gen("file1", "file1 content", commit="commit file")

    tmp_dir.gen({"dir": {}})
    erepo = {"url": os.fspath(erepo_dir)}
    dvc.imp_url("file1", out=os.path.join("dir", "imported_file"), erepo=erepo)

    assert (tmp_dir / "dir" / "imported_file").exists()
    assert (tmp_dir / "dir" / "imported_file.dvc").exists()


def test_import_url_nonexistent(dvc, erepo_dir):
    with pytest.raises(DependencyDoesNotExistError):
        dvc.imp_url(os.fspath(erepo_dir / "non-existent"))


def test_import_url_with_no_exec(tmp_dir, dvc, erepo_dir):
    tmp_dir.gen({"data_dir": {"file": "file content"}})
    src = os.path.join("data_dir", "file")

    dvc.imp_url(src, ".", no_exec=True)
    dst = tmp_dir / "file"
    assert not dst.exists()


@pytest.mark.parametrize(
    "workspace",
    [
        pytest.lazy_fixture("local_cloud"),
        pytest.lazy_fixture("s3"),
        pytest.lazy_fixture("gs"),
        pytest.lazy_fixture("hdfs"),
        pytest.lazy_fixture("webhdfs"),
        pytest.param(
            pytest.lazy_fixture("ssh"),
            marks=pytest.mark.skipif(
                os.name == "nt", reason="disabled on windows"
            ),
        ),
        pytest.lazy_fixture("http"),
    ],
    indirect=True,
)
def test_import_url(tmp_dir, dvc, workspace):
    workspace.gen("file", "file")
    assert not (tmp_dir / "file").exists()  # sanity check
    dvc.imp_url("remote://workspace/file")
    assert (tmp_dir / "file").read_text() == "file"

    assert dvc.status() == {}


@pytest.mark.parametrize(
    "workspace, stage_md5, dir_md5",
    [
        (
            pytest.lazy_fixture("local_cloud"),
            "dc24e1271084ee317ac3c2656fb8812b",
            "b6dcab6ccd17ca0a8bf4a215a37d14cc.dir",
        ),
        (
            pytest.lazy_fixture("s3"),
            "2aa17f8daa26996b3f7a4cf8888ac9ac",
            "ec602a6ba97b2dd07bd6d2cd89674a60.dir",
        ),
        (
            pytest.lazy_fixture("gs"),
            "dc24e1271084ee317ac3c2656fb8812b",
            "b6dcab6ccd17ca0a8bf4a215a37d14cc.dir",
        ),
        (
            pytest.lazy_fixture("hdfs"),
            "ec0943f83357f702033c98e70b853c8c",
            "e6dcd267966dc628d732874f94ef4280.dir",
        ),
        pytest.param(
            pytest.lazy_fixture("ssh"),
            "dc24e1271084ee317ac3c2656fb8812b",
            "b6dcab6ccd17ca0a8bf4a215a37d14cc.dir",
            marks=pytest.mark.skipif(
                os.name == "nt", reason="disabled on windows"
            ),
        ),
    ],
    indirect=["workspace"],
)
def test_import_url_dir(tmp_dir, dvc, workspace, stage_md5, dir_md5):
    workspace.gen({"dir": {"file": "file", "subdir": {"subfile": "subfile"}}})

    # remove external cache to make sure that we don't need it to import dirs
    with dvc.config.edit() as conf:
        del conf["cache"]
    dvc.cache = Cache(dvc)

    assert not (tmp_dir / "dir").exists()  # sanity check
    dvc.imp_url("remote://workspace/dir")
    assert set(os.listdir(tmp_dir / "dir")) == {"file", "subdir"}
    assert (tmp_dir / "dir" / "file").read_text() == "file"
    assert list(os.listdir(tmp_dir / "dir" / "subdir")) == ["subfile"]
    assert (tmp_dir / "dir" / "subdir" / "subfile").read_text() == "subfile"

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

    assert dvc.status() == {}
