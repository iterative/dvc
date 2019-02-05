import mock
from unittest import TestCase
from tests.test_repro import TestRepro

from dvc.stage import Stage, StageFileDoesNotExistError


class TestStageChecksum(TestCase):
    def test(self):
        stage = Stage(None)
        outs = [{"path": "a", "md5": "123456789"}]
        deps = [{"path": "b", "md5": "987654321"}]
        d = {"md5": "123456", "cmd": "mycmd", "outs": outs, "deps": deps}

        with mock.patch.object(stage, "dumpd", return_value=d):
            self.assertEqual(
                stage._compute_md5(), "e9521a22111493406ea64a88cda63e0b"
            )


class TestStageFileDoesNotExistError(TestRepro):
    def test(self):

        msg = StageFileDoesNotExistError(self.FOO, from_checkout=False).args[0]
        self.assertEqual(
            msg,
            "'{}' does not exist. Do you mean '{}.dvc'?".format(
                self.FOO, self.FOO
            ),
        )

        msg = StageFileDoesNotExistError(self.FOO, from_checkout=True).args[0]
        self.assertEqual(
            msg,
            "'{}' does not exist. Do you mean '{}.dvc'? Or perhaps git checkout '{}'?".format(
                self.FOO, self.FOO, self.FOO
            ),
        )

        msg = StageFileDoesNotExistError("branch", from_checkout=False).args[0]
        self.assertEqual(msg, "'branch' does not exist.")

        msg = StageFileDoesNotExistError("branch", from_checkout=True).args[0]
        self.assertEqual(
            msg, "'branch' does not exist. Do you mean git checkout 'branch'?"
        )
