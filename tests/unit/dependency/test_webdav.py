from dvc.dependency.webdav import WebDAVDependency
from tests.unit.dependency.test_local import TestLocalDependency


class TestWebDAVDependency(TestLocalDependency):
    def _get_cls(self):
        return WebDAVDependency
