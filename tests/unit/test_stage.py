import os

from dvc.stage import Stage

import mock
import pytest
from unittest import TestCase


class TestStageChecksum(TestCase):
    def test(self):
        stage = Stage(None, "path")
        outs = [{"path": "a", "md5": "123456789"}]
        deps = [{"path": "b", "md5": "987654321"}]
        d = {"md5": "123456", "cmd": "mycmd", "outs": outs, "deps": deps}

        with mock.patch.object(stage, "dumpd", return_value=d):
            self.assertEqual(
                stage._compute_md5(), "e9521a22111493406ea64a88cda63e0b"
            )

    def test_wdir_default_ignored(self):
        stage = Stage(None, "path")
        outs = [{"path": "a", "md5": "123456789"}]
        deps = [{"path": "b", "md5": "987654321"}]
        d = {
            "md5": "123456",
            "cmd": "mycmd",
            "outs": outs,
            "deps": deps,
            "wdir": ".",
        }

        with mock.patch.object(stage, "dumpd", return_value=d):
            self.assertEqual(
                stage._compute_md5(), "e9521a22111493406ea64a88cda63e0b"
            )

    def test_wdir_non_default_is_not_ignored(self):
        stage = Stage(None, "path")
        outs = [{"path": "a", "md5": "123456789"}]
        deps = [{"path": "b", "md5": "987654321"}]
        d = {
            "md5": "123456",
            "cmd": "mycmd",
            "outs": outs,
            "deps": deps,
            "wdir": "..",
        }

        with mock.patch.object(stage, "dumpd", return_value=d):
            self.assertEqual(
                stage._compute_md5(), "2ceba15e87f6848aa756502c1e6d24e9"
            )


class TestPathConversion(TestCase):
    def test(self):
        import posixpath
        import ntpath

        stage = Stage(None, "path")

        stage.wdir = posixpath.join("..", "..")
        self.assertEqual(stage.dumpd()["wdir"], "../..")

        stage.wdir = ntpath.join("..", "..")
        self.assertEqual(stage.dumpd()["wdir"], "../..")


@pytest.mark.parametrize("add", [True, False])
def test_expand_to_path_on_add_local(add):
    out = mock.Mock()
    out.is_in_repo = False
    path = "path"
    fname = Stage._expand_to_path_on_add_local(add, path, out, os.path)
    assert fname == path
