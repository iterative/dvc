import os
from uuid import uuid4

import pytest

from dvc.stage import Stage
from dvc.main import main
from dvc.utils.fs import makedirs
from dvc.compat import fspath
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
    ret = main(["import-url", fspath(erepo_dir / "foo")])

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
        ret = main(["import-url", "-f", "bar.dvc", self.external_source])
        self.assertEqual(0, ret)
        self.assertTrue(os.path.exists("bar.dvc"))

        os.remove("bar.dvc")

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
