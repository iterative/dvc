import os
from collections.abc import Iterator, Mapping
from datetime import datetime
from functools import cached_property
from pathlib import Path
from typing import TYPE_CHECKING, Any, ClassVar, Literal, Optional, Union, cast
from urllib.parse import urlparse

from attrs import Attribute, AttrsInstance, asdict, evolve, field, fields, frozen
from attrs.converters import default_if_none

from dvc.dvcfile import Lockfile, ProjectFile
from dvc.exceptions import DvcException
from dvc.log import logger
from dvc.types import StrPath
from dvc_data.hashfile.meta import Meta

if TYPE_CHECKING:
    from datachain.dataset import DatasetRecord, DatasetVersion  # type: ignore[import]
    from typing_extensions import Self

    from dvc.repo import Repo


logger = logger.getChild(__name__)


def _get_dataset_record(name: str) -> "DatasetRecord":
    from dvc.exceptions import DvcException

    try:
        from datachain.catalog import get_catalog  # type: ignore[import]

    except ImportError as exc:
        raise DvcException("datachain is not installed") from exc

    catalog = get_catalog()
    return catalog.get_remote_dataset(name)


def _get_dataset_info(
    name: str, record: Optional["DatasetRecord"] = None, version: Optional[int] = None
) -> "DatasetVersion":
    record = record or _get_dataset_record(name)
    assert record
    v = record.latest_version if version is None else version
    assert v is not None
    return record.get_version(v)


def default_str(v) -> str:
    return default_if_none("")(v)


def to_datetime(d: Union[str, datetime]) -> datetime:
    return datetime.fromisoformat(d) if isinstance(d, str) else d


def ensure(cls):
    def inner(v):
        return cls.from_dict(v) if isinstance(v, dict) else v

    return inner


class SerDe:
    def to_dict(self: AttrsInstance) -> dict[str, Any]:
        def filter_defaults(attr: Attribute, v: Any):
            if attr.metadata.get("exclude_falsy", False) and not v:
                return False
            return attr.default != v

        def value_serializer(_inst, _field, v):
            return v.isoformat() if isinstance(v, datetime) else v

        return asdict(self, filter=filter_defaults, value_serializer=value_serializer)

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> "Self":
        _fields = fields(cast("type[AttrsInstance]", cls))
        kwargs = {f.name: d[f.name] for f in _fields if f.name in d}
        return cls(**kwargs)


@frozen(kw_only=True)
class DatasetSpec(SerDe):
    name: str
    url: str
    type: Literal["dvc", "dc", "url"]


@frozen(kw_only=True)
class DVCDatasetSpec(DatasetSpec):
    type: Literal["dvc"]
    path: str = field(default="", converter=default_str)
    rev: Optional[str] = None


@frozen(kw_only=True, order=True)
class FileInfo(SerDe):
    relpath: str
    meta: Meta = field(order=False, converter=ensure(Meta))  # type: ignore[misc]


@frozen(kw_only=True)
class DVCDatasetLock(DVCDatasetSpec):
    rev_lock: str


@frozen(kw_only=True)
class DatachainDatasetLock(DatasetSpec):
    version: int
    created_at: datetime = field(converter=to_datetime)


@frozen(kw_only=True)
class URLDatasetLock(DatasetSpec):
    meta: Meta = field(converter=ensure(Meta))  # type: ignore[misc]
    files: list[FileInfo] = field(
        factory=list,
        converter=lambda f: sorted(map(ensure(FileInfo), f)),
        metadata={"exclude_falsy": True},
    )


def to_spec(lock: "Lock") -> "Spec":
    cls = DVCDatasetSpec if lock.type == "dvc" else DatasetSpec
    return cls(**{f.name: getattr(lock, f.name) for f in fields(cls)})


@frozen(kw_only=True)
class DVCDataset:
    manifest_path: str
    spec: DVCDatasetSpec
    lock: Optional[DVCDatasetLock] = None
    _invalidated: bool = field(default=False, eq=False, repr=False)

    type: ClassVar[Literal["dvc"]] = "dvc"

    def update(self, repo, rev: Optional[str] = None, **kwargs) -> "Self":
        from dvc.dependency import RepoDependency

        spec = self.spec
        if rev:
            spec = evolve(self.spec, rev=rev)

        def_repo = {
            RepoDependency.PARAM_REV: spec.rev,
            RepoDependency.PARAM_URL: spec.url,
        }
        dep = RepoDependency(def_repo, None, spec.path, repo=repo)  # type: ignore[arg-type]
        dep.save()
        d = dep.dumpd()

        repo_info = d[RepoDependency.PARAM_REPO]
        assert isinstance(repo_info, dict)
        rev_lock = repo_info[RepoDependency.PARAM_REV_LOCK]
        lock = DVCDatasetLock(**spec.to_dict(), rev_lock=rev_lock)
        return evolve(self, spec=spec, lock=lock)


@frozen(kw_only=True)
class DatachainDataset:
    manifest_path: str
    spec: "DatasetSpec"
    lock: "Optional[DatachainDatasetLock]" = field(default=None)
    _invalidated: bool = field(default=False, eq=False, repr=False)

    type: ClassVar[Literal["dc"]] = "dc"

    @property
    def pinned(self) -> bool:
        return self.name_version[1] is not None

    @property
    def name_version(self) -> tuple[str, Optional[int]]:
        url = urlparse(self.spec.url)
        path = url.netloc + url.path
        parts = path.split("@v")
        assert parts

        name = parts[0]
        version = int(parts[1]) if len(parts) > 1 else None
        return name, version

    def update(
        self,
        repo,  # noqa: ARG002
        record: Optional["DatasetRecord"] = None,
        version: Optional[int] = None,
        **kwargs,
    ) -> "Self":
        name, _version = self.name_version
        version = version if version is not None else _version
        version_info = _get_dataset_info(name, record=record, version=version)
        lock = DatachainDatasetLock(
            **self.spec.to_dict(),
            version=version_info.version,
            created_at=version_info.created_at,
        )
        return evolve(self, lock=lock)


@frozen(kw_only=True)
class URLDataset:
    manifest_path: str
    spec: "DatasetSpec"
    lock: "Optional[URLDatasetLock]" = None
    _invalidated: bool = field(default=False, eq=False, repr=False)

    type: ClassVar[Literal["url"]] = "url"

    def update(self, repo, **kwargs):
        from dvc.dependency import Dependency

        dep = Dependency(
            None, self.spec.url, repo=repo, fs_config={"version_aware": True}
        )
        dep.save()
        d = dep.dumpd(datasets=True)
        files = [
            FileInfo(relpath=info["relpath"], meta=Meta.from_dict(info))
            for info in d.get("files", [])
        ]
        lock = URLDatasetLock(**self.spec.to_dict(), meta=dep.meta, files=files)
        return evolve(self, lock=lock)


Lock = Union[DVCDatasetLock, DatachainDatasetLock, URLDatasetLock]
Spec = Union[DatasetSpec, DVCDatasetSpec]
Dataset = Union[DVCDataset, DatachainDataset, URLDataset]


class DatasetNotFoundError(DvcException, KeyError):
    def __init__(self, name, *args):
        self.name = name
        super().__init__("dataset not found", *args)

    def __str__(self) -> str:
        return self.msg


class Datasets(Mapping[str, Dataset]):
    def __init__(self, repo: "Repo") -> None:
        self.repo: Repo = repo

    def __repr__(self):
        return repr(dict(self))

    def __rich_repr__(self):
        yield dict(self)

    def __getitem__(self, name: str) -> Dataset:
        try:
            return self._datasets[name]
        except KeyError as exc:
            raise DatasetNotFoundError(name) from exc

    def __setitem__(self, name: str, dataset: Dataset) -> None:
        self._datasets[name] = dataset

    def __contains__(self, name: object) -> bool:
        return name in self._datasets

    def __iter__(self) -> Iterator[str]:
        return iter(self._datasets)

    def __len__(self) -> int:
        return len(self._datasets)

    @cached_property
    def _spec(self) -> dict[str, tuple[str, dict[str, Any]]]:
        return {
            dataset["name"]: (path, dataset)
            for path, datasets in self.repo.index._datasets.items()
            for dataset in datasets
        }

    @cached_property
    def _lock(self) -> dict[str, Optional[dict[str, Any]]]:
        datasets_lock = self.repo.index._datasets_lock

        def find(path, name) -> Optional[dict[str, Any]]:
            # only look for `name` in the lock file next to the
            # corresponding `dvc.yaml` file
            lock = datasets_lock.get(path, [])
            return next((dataset for dataset in lock if dataset["name"] == name), None)

        return {ds["name"]: find(path, name) for name, (path, ds) in self._spec.items()}

    @cached_property
    def _datasets(self) -> dict[str, Dataset]:
        return {
            name: self._build_dataset(path, spec, self._lock[name])
            for name, (path, spec) in self._spec.items()
        }

    def _reset(self) -> None:
        self.__dict__.pop("_spec", None)
        self.__dict__.pop("_lock", None)
        self.__dict__.pop("_datasets", None)

    @staticmethod
    def _spec_from_info(spec: dict[str, Any]) -> Spec:
        typ = spec.get("type")
        if not typ:
            raise ValueError("type should be present in spec")
        if typ == "dvc":
            return DVCDatasetSpec.from_dict(spec)
        if typ in {"dc", "url"}:
            return DatasetSpec.from_dict(spec)
        raise ValueError(f"unknown dataset type: {spec.get('type', '')}")

    @staticmethod
    def _lock_from_info(lock: Optional[dict[str, Any]]) -> Optional[Lock]:
        kl = {"dvc": DVCDatasetLock, "dc": DatachainDatasetLock, "url": URLDatasetLock}
        if lock and (cls := kl.get(lock.get("type", ""))):  # type: ignore[assignment]
            return cls.from_dict(lock)  # type: ignore[attr-defined]
        return None

    @classmethod
    def _build_dataset(
        cls,
        manifest_path: str,
        spec_data: dict[str, Any],
        lock_data: Optional[dict[str, Any]] = None,
    ) -> Dataset:
        _invalidated = False
        spec = cls._spec_from_info(spec_data)
        lock = cls._lock_from_info(lock_data)
        # if dvc.lock and dvc.yaml file are not in sync, we invalidate the lock.
        if lock is not None and to_spec(lock) != spec:
            logger.debug(
                "invalidated lock data for %s in %s",
                spec.name,
                manifest_path,
            )
            _invalidated = True  # signal is used during `dvc repro`/`dvc status`.
            lock = None

        assert isinstance(spec, DatasetSpec)
        if spec.type == "dvc":
            assert lock is None or isinstance(lock, DVCDatasetLock)
            assert isinstance(spec, DVCDatasetSpec)
            return DVCDataset(
                manifest_path=manifest_path,
                spec=spec,
                lock=lock,
                invalidated=_invalidated,
            )
        if spec.type == "url":
            assert lock is None or isinstance(lock, URLDatasetLock)
            return URLDataset(
                manifest_path=manifest_path,
                spec=spec,
                lock=lock,
                invalidated=_invalidated,
            )
        if spec.type == "dc":
            assert lock is None or isinstance(lock, DatachainDatasetLock)
            return DatachainDataset(
                manifest_path=manifest_path,
                spec=spec,
                lock=lock,
                invalidated=_invalidated,
            )
        raise ValueError(f"unknown dataset type: {spec.type!r}")

    def add(
        self,
        name: str,
        url: str,
        type: str,  # noqa: A002
        manifest_path: StrPath = "dvc.yaml",
        **kwargs: Any,
    ) -> Dataset:
        assert type in {"dvc", "dc", "url"}
        kwargs.update({"name": name, "url": url, "type": type})
        dataset = self._build_dataset(os.path.abspath(manifest_path), kwargs)
        dataset = dataset.update(self.repo)

        self.dump(dataset)
        self[name] = dataset
        return dataset

    def update(self, name, **kwargs) -> tuple[Dataset, Dataset]:
        dataset = self[name]
        version = kwargs.get("version")

        if dataset.type == "url" and (version or kwargs.get("rev")):
            raise ValueError("cannot update version/revision for a url")
        if dataset.type == "dc" and version is not None:
            if not isinstance(version, int):
                raise TypeError(
                    "DataChain dataset version has to be an integer, "
                    f"got {type(version).__name__!r}"
                )
            if version < 1:
                raise ValueError(
                    f"DataChain dataset version should be >=1, got {version}"
                )

        new = dataset.update(self.repo, **kwargs)

        self.dump(new, old=dataset)
        self[name] = new
        return dataset, new

    def _dump_spec(self, manifest_path: StrPath, spec: Spec) -> None:
        spec_data = spec.to_dict()
        assert spec_data.keys() & {"type", "name", "url"}
        project_file = ProjectFile(self.repo, manifest_path)
        project_file.dump_dataset(spec_data)

    def _dump_lock(self, manifest_path: StrPath, lock: Lock) -> None:
        lock_data = lock.to_dict()
        assert lock_data.keys() & {"type", "name", "url"}
        lockfile = Lockfile(self.repo, Path(manifest_path).with_suffix(".lock"))
        lockfile.dump_dataset(lock_data)

    def dump(self, dataset: Dataset, old: Optional[Dataset] = None) -> None:
        if not old or old.spec != dataset.spec:
            self._dump_spec(dataset.manifest_path, dataset.spec)
        if dataset.lock and (not old or old.lock != dataset.lock):
            self._dump_lock(dataset.manifest_path, dataset.lock)
