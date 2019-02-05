import mock
from unittest import TestCase
from dvc.stage import Stage


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
