import os
import shutil
import filecmp
import logging

from dvc.main import main
from dvc.utils import from_yaml_string

from tests.basic_env import TestDvc


class TestTag(TestDvc):
    def test(self):
        fname = "file"
        shutil.copyfile(self.FOO, fname)

        stages = self.dvc.add(fname)
        self.assertEqual(len(stages), 1)
        stage = stages[0]
        self.assertTrue(stage is not None)

        ret = main(["tag", "add", "v1", stage.path])
        self.assertEqual(ret, 0)

        os.unlink(fname)
        shutil.copyfile(self.BAR, fname)

        ret = main(["repro", stage.path])
        self.assertEqual(ret, 0)

        ret = main(["tag", "add", "v2", stage.path])
        self.assertEqual(ret, 0)

        ret = main(["tag", "list", stage.path])
        self.assertEqual(ret, 0)

        ret = main(["checkout", stage.path + "@v1"])
        self.assertEqual(ret, 0)

        self.assertTrue(filecmp.cmp(fname, self.FOO, shallow=False))

        ret = main(["checkout", stage.path + "@v2"])
        self.assertEqual(ret, 0)

        self.assertTrue(filecmp.cmp(fname, self.BAR, shallow=False))


class TestTagAll(TestDvc):
    def test(self):
        ret = main(["add", self.FOO, self.BAR])
        self.assertEqual(ret, 0)

        self._caplog.clear()

        ret = main(["tag", "list"])
        self.assertEqual(ret, 0)

        self.assertEqual("", self._caplog.text)

        ret = main(["tag", "add", "v1"])
        self.assertEqual(ret, 0)

        self._caplog.clear()

        ret = main(["tag", "list"])
        self.assertEqual(ret, 0)

        expected = {
            "foo.dvc": {
                "foo": {"v1": {"md5": "acbd18db4cc2f85cedef654fccc4a4d8"}}
            },
            "bar.dvc": {
                "bar": {"v1": {"md5": "8978c98bb5a48c2fb5f2c4c905768afa"}}
            },
        }

        stdout = "\n".join(record.message for record in self._caplog.records)
        received = from_yaml_string(stdout)

        assert expected == received

        ret = main(["tag", "remove", "v1"])
        self.assertEqual(ret, 0)

        self._caplog.clear()

        ret = main(["tag", "list"])
        self.assertEqual(ret, 0)

        self.assertEqual("", self._caplog.text)


class TestTagAddNoChecksumInfo(TestDvc):
    def test(self):
        ret = main(["run", "-o", self.FOO, "--no-exec"])
        self.assertEqual(ret, 0)

        self._caplog.clear()

        with self._caplog.at_level(logging.WARNING, logger="dvc"):
            ret = main(["tag", "add", "v1", "foo.dvc"])
            self.assertEqual(ret, 0)

        assert "missing checksum info for 'foo'" in self._caplog.text


class TestTagRemoveNoTag(TestDvc):
    def test(self):
        ret = main(["add", self.FOO])
        self.assertEqual(ret, 0)

        self._caplog.clear()

        with self._caplog.at_level(logging.WARNING, logger="dvc"):
            ret = main(["tag", "remove", "v1", "foo.dvc"])
            self.assertEqual(ret, 0)

        assert "tag 'v1' not found for 'foo'" in self._caplog.text
