from __future__ import unicode_literals

import filecmp
import os
from uuid import uuid4

import pytest
from mock import patch

import dvc
from dvc.main import main
from dvc.utils import makedirs
from dvc.utils.compat import str
from tests.basic_env import TestDvc
from tests.utils import spy


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


class TestShouldRemoveOutsBeforeImport(TestDvc):
    def setUp(self):
        super(TestShouldRemoveOutsBeforeImport, self).setUp()
        tmp_dir = self.mkdtemp()
        self.external_source = os.path.join(tmp_dir, "file")
        with open(self.external_source, "w") as fobj:
            fobj.write("content")

    def test(self):
        remove_outs_call_counter = spy(dvc.stage.Stage.remove_outs)
        with patch.object(
            dvc.stage.Stage, "remove_outs", remove_outs_call_counter
        ):
            ret = main(["import-url", self.external_source])
            self.assertEqual(0, ret)

        self.assertEqual(1, remove_outs_call_counter.mock.call_count)


class TestImportFilename(TestDvc):
    def setUp(self):
        super(TestImportFilename, self).setUp()
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
def test_import_url_to_dir(dname, repo_dir, dvc_repo):
    src = repo_dir.DATA

    makedirs(dname, exist_ok=True)

    stage = dvc_repo.imp_url(src, dname)

    dst = os.path.join(dname, os.path.basename(src))

    assert stage.outs[0].fspath == os.path.abspath(dst)
    assert os.path.isdir(dname)
    assert filecmp.cmp(repo_dir.DATA, dst, shallow=False)
