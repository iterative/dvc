from dvc.dependency.http import HTTPDependency
from tests.unit.dependency.test_local import TestLocalDependency


class TestHTTPDependency(TestLocalDependency):
    def _get_cls(self):
        return HTTPDependency
