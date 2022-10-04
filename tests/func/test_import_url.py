import json
import os
import textwrap
from uuid import uuid4

import pytest

from dvc.annotations import Annotation
from dvc.cli import main
from dvc.dependency.base import Dependency, DependencyDoesNotExistError
from dvc.dvcfile import Dvcfile
from dvc.exceptions import InvalidArgumentError
from dvc.stage import Stage
from dvc.testing.workspace_tests import TestImport as _TestImport
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

        with open(tmpfile, "w", encoding="utf-8") as fd:
            fd.write("content")

        ret = main(["import-url", tmpfile])
        self.assertEqual(ret, 0)
        self.assertTrue(os.path.exists(filename))
        with open(filename, encoding="utf-8") as fd:
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
        with open(self.external_source, "w", encoding="utf-8") as fobj:
            fobj.write("content")

    def test(self):
        ret = main(["import-url", "--file", "bar.dvc", self.external_source])
        self.assertEqual(0, ret)
        self.assertTrue(os.path.exists("bar.dvc"))

        os.remove("bar.dvc")
        os.mkdir("sub")

        path = os.path.join("sub", "bar.dvc")
        ret = main(["import-url", "--file", path, self.external_source, "out"])
        self.assertEqual(0, ret)
        self.assertTrue(os.path.exists(path))


@pytest.mark.parametrize("dname", [".", "dir", "dir/subdir"])
def test_import_url_to_dir(dname, tmp_dir, dvc):
    tmp_dir.gen({"data_dir": {"file": "file content"}})
    src = os.path.join("data_dir", "file")

    makedirs(dname, exist_ok=True)

    stage = dvc.imp_url(src, dname)

    dst = tmp_dir / dname / "file"

    assert stage.outs[0].fs_path == os.fspath(dst)
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


class TestImport(_TestImport):
    @pytest.fixture
    def stage_md5(self):
        return "dc24e1271084ee317ac3c2656fb8812b"

    @pytest.fixture
    def dir_md5(self):
        return "b6dcab6ccd17ca0a8bf4a215a37d14cc.dir"

    @pytest.fixture
    def is_object_storage(self):
        return False


def test_import_url_preserve_fields(tmp_dir, dvc):
    text = textwrap.dedent(
        """\
        # top comment
        desc: top desc
        deps:
        - path: foo # dep comment
        outs:
        - path: bar # out comment
          desc: out desc
          type: mytype
          labels:
          - label1
          - label2
          meta:
            key: value
        meta: some metadata
    """
    )
    tmp_dir.gen("bar.dvc", text)

    tmp_dir.gen("foo", "foo")
    dvc.imp_url("foo", out="bar")
    assert (tmp_dir / "bar.dvc").read_text() == textwrap.dedent(
        """\
        # top comment
        desc: top desc
        deps:
        - path: foo # dep comment
          md5: acbd18db4cc2f85cedef654fccc4a4d8
          size: 3
        outs:
        - path: bar # out comment
          desc: out desc
          type: mytype
          labels:
          - label1
          - label2
          meta:
            key: value
          md5: acbd18db4cc2f85cedef654fccc4a4d8
          size: 3
        meta: some metadata
        md5: be7ade0aa89cc8d56e320867a9de9740
        frozen: true
    """
    )


def test_import_url_to_remote_single_file(
    tmp_dir, dvc, workspace, local_remote
):
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
    with open(
        local_remote.oid_to_path(hash_info.value), encoding="utf-8"
    ) as fobj:
        assert fobj.read() == "foo"
    assert stage.outs[0].meta.size == len("foo")


def test_import_url_to_remote_directory(tmp_dir, dvc, workspace, local_remote):
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
    with open(
        local_remote.oid_to_path(hash_info.value), encoding="utf-8"
    ) as stream:
        file_parts = json.load(stream)

    assert len(file_parts) == 3
    assert {file_part["relpath"] for file_part in file_parts} == {
        "foo",
        "bar",
        "sub_dir/baz",
    }

    for file_part in file_parts:
        with open(
            local_remote.oid_to_path(file_part["md5"]), encoding="utf-8"
        ) as fobj:
            assert fobj.read() == file_part["relpath"]


def test_import_url_to_remote_absolute(
    tmp_dir, make_tmp_dir, dvc, local_remote
):
    tmp_abs_dir = make_tmp_dir("abs")
    tmp_foo = tmp_abs_dir / "foo"
    tmp_foo.write_text("foo")

    stage = dvc.imp_url(str(tmp_foo), to_remote=True)

    foo = tmp_dir / "foo"
    assert stage.deps[0].fspath == str(tmp_foo)
    assert stage.outs[0].fspath == os.fspath(foo)
    assert foo.with_suffix(".dvc").exists()


def test_import_url_to_remote_invalid_combinations(dvc):
    with pytest.raises(InvalidArgumentError, match="--no-exec"):
        dvc.imp_url("s3://bucket/foo", no_exec=True, to_remote=True)


def test_import_url_to_remote_status(tmp_dir, dvc, local_cloud, local_remote):
    local_cloud.gen("foo", "foo")

    stage = dvc.imp_url(str(local_cloud / "foo"), to_remote=True)
    assert stage.md5 is not None

    status = dvc.status()
    assert status["foo.dvc"] == [{"changed outs": {"foo": "not in cache"}}]

    dvc.pull()

    status = dvc.status()
    assert len(status) == 0


def test_import_url_no_download(tmp_dir, scm, dvc, local_workspace):
    local_workspace.gen("file", "file content")
    dst = tmp_dir / "file"
    stage = dvc.imp_url(
        "remote://workspace/file", os.fspath(dst), no_download=True
    )

    assert stage.deps[0].hash_info.value == "d10b4c3ff123b26dc068d43a8bef2d23"

    assert not dst.exists()
    assert scm.is_ignored(dst)

    out = stage.outs[0]
    assert not out.hash_info
    assert out.meta.size is None

    status = dvc.status()
    assert status["file.dvc"] == [
        {"changed outs": {"file": "deleted"}},
    ]


def test_partial_import_pull(tmp_dir, scm, dvc, local_workspace):
    local_workspace.gen("file", "file content")
    dst = tmp_dir / "file"
    dvc.imp_url("remote://workspace/file", os.fspath(dst), no_download=True)

    dvc.pull(["file.dvc"])

    assert dst.exists()

    stage = Dvcfile(dvc, "file.dvc").stage

    assert stage.outs[0].hash_info.value == "d10b4c3ff123b26dc068d43a8bef2d23"
    assert stage.outs[0].meta.size == 12


def test_imp_url_with_annotations(M, tmp_dir, dvc, local_workspace):
    local_workspace.gen("foo", "foo")
    annot = {
        "desc": "foo desc",
        "labels": ["l1", "l2"],
        "type": "t1",
        "meta": {"key": "value"},
    }
    stage = dvc.imp_url(
        "remote://workspace/foo",
        os.fspath(tmp_dir / "foo"),
        no_exec=True,
        **annot,
    )
    assert stage.outs[0].annot == Annotation(**annot)
    assert (tmp_dir / "foo.dvc").parse() == M.dict(outs=[M.dict(**annot)])

    # try to selectively update/overwrite some annotations
    annot = {**annot, "type": "t2"}
    stage = dvc.imp_url(
        "remote://workspace/foo",
        os.fspath(tmp_dir / "foo"),
        no_exec=True,
        type="t2",
    )
    assert stage.outs[0].annot == Annotation(**annot)
    assert (tmp_dir / "foo.dvc").parse() == M.dict(outs=[M.dict(**annot)])


def test_import_url_fs_config(tmp_dir, dvc, workspace, mocker):
    import dvc.fs as dvc_fs

    workspace.gen("foo", "foo")

    url = "remote://workspace/foo"
    get_fs_config = mocker.spy(dvc_fs, "get_fs_config")
    dep_init = mocker.spy(Dependency, "__init__")
    dvc.imp_url(url, fs_config={"jobs": 42})

    dep_init_kwargs = dep_init.call_args[1]
    assert dep_init_kwargs.get("fs_config") == {"jobs": 42}

    assert get_fs_config.call_args_list[0][1] == {"url": "foo"}
    assert get_fs_config.call_args_list[1][1] == {"url": url, "jobs": 42}
    assert get_fs_config.call_args_list[2][1] == {"name": "workspace"}
