from dvc.stage import Stage
from dvc.dependency import _get, DEP_MAP

from tests.test_output import TestOutScheme


class TestDepScheme(TestOutScheme):
    MAP = DEP_MAP

    TESTS = TestOutScheme.TESTS.copy()
    TESTS['http://example.com/path/to/file'] = 'http'
    TESTS['https://example.com/path/to/file'] = 'https'

    def _get(self, path):
        return _get(Stage(self.dvc), path, None)
