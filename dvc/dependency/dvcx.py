from typing import TYPE_CHECKING, Any, Dict, Optional, Tuple
from urllib.parse import urlparse

from dvc.utils.objects import cached_property

from .db import AbstractDependency

if TYPE_CHECKING:
    from dvc.stage import Stage


class DvcxDependency(AbstractDependency):
    def __init__(self, stage: "Stage", p, info, *args, **kwargs):
        super().__init__(stage, info, *args, **kwargs)
        parts = self.parse(p)
        assert parts
        self.dataset, self.version = parts

    def __repr__(self):
        return f"{self.__class__.__name__}({self.uri!r})"

    def __str__(self):
        return self.uri

    @property
    def uri(self):
        from dql.dataset import create_dataset_uri

        return create_dataset_uri(self.dataset, self.version)

    @classmethod
    def is_dataset(cls, p: str):
        return urlparse(p).scheme == "ds"

    @classmethod
    def parse(cls, uri: str) -> Optional[Tuple[str, Optional[int]]]:
        if not cls.is_dataset(uri):
            return None

        from dql.dataset import parse_dataset_uri

        return parse_dataset_uri(uri)

    @cached_property
    def catalog(self):
        from dvc.utils.packaging import check_required_version

        check_required_version(pkg="dvcx")

        from dql.catalog import get_catalog

        return get_catalog()

    @cached_property
    def latest_version(self) -> int:
        dataset = self.catalog.get_dataset(self.dataset)
        latest = dataset.latest_version
        assert latest, "no version info found"
        return latest

    def workspace_status(self):
        if not self.version or self.latest_version > self.version:
            return {self.uri: f"update available to v{self.latest_version}"}
        return {}

    def status(self):
        return self.workspace_status()

    def save(self):
        self.version = self.version or self.latest_version

    def update(self, rev=None, version: Optional[int] = None):  # noqa: ARG002
        self.version = version or self.latest_version

    def download(self, to, jobs=None, version: Optional[int] = None):  # noqa: ARG002
        self.version = version or self.version or self.latest_version
        self.catalog.pull_dataset(self.uri, output=to.fs_path)

    def dumpd(self, **kwargs: Any) -> Dict[str, Any]:
        return {self.PARAM_PATH: self.uri}
