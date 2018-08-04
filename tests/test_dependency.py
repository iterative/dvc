from dvc.dependency import _get
from dvc.exceptions import DvcException

from tests.basic_env import TestDvc


class TestUnsupported(TestDvc):
    def test(self):
        with self.assertRaises(DvcException):
            _get(None, 'unsupported://a/b', None)
