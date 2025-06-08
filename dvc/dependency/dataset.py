from typing import TYPE_CHECKING, Any, ClassVar
from urllib.parse import urlparse

from funcy import compact, merge

from dvc.exceptions import DvcException
from dvc_data.hashfile.hash_info import HashInfo

from .db import AbstractDependency

if TYPE_CHECKING:
    from dvc.stage import Stage


class DatasetDependency(AbstractDependency):
    PARAM_DATASET = "dataset"
    DATASET_SCHEMA: ClassVar[dict] = {PARAM_DATASET: dict}

    def __init__(self, stage: "Stage", p, info, *args, **kwargs):
        super().__init__(stage, info, *args, **kwargs)
        self.def_path = p
        self.name = urlparse(p).netloc
        dataset_info = info.get(self.PARAM_DATASET) or {}
        self.hash_info = HashInfo(self.PARAM_DATASET, dataset_info)  # type: ignore[arg-type]
        self.hash_name = self.PARAM_DATASET

    def __repr__(self):
        return f"{self.__class__.__name__}({self.def_path!r})"

    def __str__(self):
        return self.def_path

    @classmethod
    def is_dataset(cls, p: str):
        return urlparse(p).scheme == "ds"

    @property
    def protocol(self):
        return None

    def dumpd(self, **kwargs):
        return compact({self.PARAM_PATH: self.def_path, **self.hash_info.to_dict()})

    def fill_values(self, values=None):
        """Load params values dynamically."""
        self.hash_info = HashInfo(
            self.PARAM_DATASET, merge(self.hash_info.value, values or {})
        )

    def workspace_status(self):
        ds = self.repo.datasets[self.name]
        if not ds.lock:
            return {str(self): "not in sync"}

        info: dict[str, Any] = self.hash_info.value if self.hash_info else {}  # type: ignore[assignment]
        lock = self.repo.datasets._lock_from_info(info)
        if not lock:
            return {str(self): "new"}
        if lock != ds.lock:
            return {str(self): "modified"}
        return {}

    def status(self):
        return self.workspace_status()

    def get_hash(self):
        ds = self.repo.datasets[self.name]
        if not ds.lock:
            if ds._invalidated:
                raise DvcException(
                    "Dataset information is not in sync. "
                    f"Run 'dvc ds update {self.name}' to sync."
                )
            raise DvcException("Dataset information missing from dvc.lock file")
        return HashInfo(self.PARAM_DATASET, ds.lock.to_dict())  # type: ignore[arg-type]

    def save(self):
        self.hash_info = self.get_hash()

    def download(self, to, jobs=None):
        raise NotImplementedError

    def update(self, rev=None):
        raise NotImplementedError
