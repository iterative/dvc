from collections.abc import Iterator, Mapping
from datetime import datetime
from functools import cached_property
from pathlib import Path
from typing import TYPE_CHECKING, Any, Optional, Union, cast
from urllib.parse import urlparse

from attrs import Attribute, AttrsInstance, asdict, evolve, field, fields, frozen
from attrs.converters import default_if_none

from dvc.dvcfile import Lockfile, ProjectFile
from dvc.exceptions import DvcException
from dvc.log import logger
from dvc_data.hashfile.meta import Meta

if TYPE_CHECKING:
    from dql.dataset import DatasetRecord  # type: ignore[import]
    from typing_extensions import Self

    from dvc.repo import Repo


logger = logger.getChild(__name__)


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
    def from_dict(cls: type["Self"], d: dict[str, Any]) -> "Self":
        _fields = fields(cast("type[AttrsInstance]", cls))
        kwargs = {f.name: d[f.name] for f in _fields if f.name in d}
        return cls(**kwargs)


@frozen(kw_only=True)
class DVCDataset(SerDe):
    name: str
    url: str
    type: str
    path: str = field(default="", converter=default_str)
    rev: Optional[str] = None

    def lock(self, repo, **kwargs):
        from dvc.dependency import RepoDependency

        def_repo = {
            RepoDependency.PARAM_REV: self.rev,
            RepoDependency.PARAM_URL: self.url,
        }
        dep = RepoDependency(def_repo, None, self.path, repo=repo)  # type: ignore[arg-type]
        dep.save()
        d = dep.dumpd()

        repo_info = d[RepoDependency.PARAM_REPO]
        assert isinstance(repo_info, dict)
        rev_lock = repo_info[RepoDependency.PARAM_REV_LOCK]
        return DVCDatasetLock(
            name=self.name,
            type=self.type,
            url=self.url,
            path=self.path,
            rev=self.rev,
            rev_lock=rev_lock,
        )


@frozen(kw_only=True)
class DVCDatasetLock(DVCDataset):
    rev_lock: str

    def to_spec(self) -> "DVCDataset":
        return DVCDataset.from_dict(self.to_dict())


@frozen(kw_only=True)
class DVCXDataset(SerDe):
    name: str
    url: str
    type: str

    @property
    def name_version(self) -> tuple[str, Optional[int]]:
        url = urlparse(self.url)
        parts = url.netloc.split("@v")
        assert parts

        name = parts[0]
        version = int(parts[1]) if len(parts) > 1 else None
        return name, version

    def lock(
        self,
        repo,  # noqa: ARG002
        record: Optional["DatasetRecord"] = None,
        version: Optional[int] = None,
        **kwargs,
    ):
        if not record:
            try:
                from dvcx.catalog import get_catalog  # type: ignore[import]

            except ImportError as exc:
                raise DvcException("dvcx is not installed") from exc

            name, version = self.name_version
            catalog = get_catalog()
            record = catalog.get_remote_dataset(name)
        assert record is not None
        return self._lock_from_dataset_record(record, version=version)

    def _lock_from_dataset_record(
        self, record: "DatasetRecord", version: Optional[int] = None
    ) -> "DVCXDatasetLock":
        ver = version or record.latest_version
        assert ver
        version_info = record.get_version(ver)
        return DVCXDatasetLock(
            name=self.name,
            url=self.url,
            type=self.type,
            version=version_info.version,
            created_at=version_info.created_at,
        )


@frozen(kw_only=True)
class DVCXDatasetLock(DVCXDataset):
    version: int
    created_at: datetime = field(converter=to_datetime)

    def to_spec(self) -> "DVCXDataset":
        return DVCXDataset.from_dict(self.to_dict())


@frozen(kw_only=True, order=True)
class FileInfo(SerDe):
    relpath: str
    meta: Meta = field(order=False, converter=ensure(Meta))  # type: ignore[misc]


@frozen(kw_only=True)
class URLDataset(SerDe):
    name: str
    url: str
    type: str

    def lock(self, repo, **kwargs):
        from dvc.dependency import Dependency

        dep = Dependency(None, self.url, repo=repo, fs_config={"version_aware": True})
        dep.save()
        d = dep.dumpd(datasets=True)
        files = [
            FileInfo(relpath=info["relpath"], meta=Meta.from_dict(info))
            for info in d.get("files", [])
        ]
        return URLDatasetLock(
            name=self.name, type=self.type, url=self.url, meta=dep.meta, files=files
        )


@frozen(kw_only=True)
class URLDatasetLock(URLDataset):
    meta: Meta = field(converter=ensure(Meta))  # type: ignore[misc]
    files: list[FileInfo] = field(
        factory=list,
        converter=lambda f: sorted(map(ensure(FileInfo), f)),
        metadata={"exclude_falsy": True},
    )

    def to_spec(self) -> "URLDataset":
        return URLDataset.from_dict(self.to_dict())


Spec = Union[DVCXDataset, DVCDataset, URLDataset]
Lock = Union[DVCXDatasetLock, DVCDatasetLock, URLDatasetLock]


@frozen(kw_only=True)
class Dataset:
    manifest_path: str
    spec: Spec
    lock: Optional[Lock] = None

    @property
    def name(self):
        return self.spec.name

    @property
    def url(self):
        return self.spec.url

    @property
    def type(self):
        return self.spec.type

    def update(self, repo: "Repo", **kwargs: Any) -> "Self":
        spec_kwargs = self.spec.to_dict()
        spec = type(self.spec).from_dict(spec_kwargs | kwargs)
        lock_kwargs = {k: kwargs[k] for k in kwargs.keys() - spec_kwargs.keys()}
        lock = spec.lock(repo, **lock_kwargs)
        return evolve(self, spec=spec, lock=lock)


class DatasetNotFoundError(DvcException, KeyError):
    def __init__(self, name, *args):
        self.name = name
        super().__init__("dataset not found", *args)

    def __str__(self) -> str:
        return self.msg


class Datasets(Mapping[str, Dataset]):
    def __init__(self, repo: "Repo") -> None:
        self.repo: "Repo" = repo

    def __repr__(self):
        return repr(dict(self))

    def __rich_repr__(self):
        yield dict(self)

    def __getitem__(self, name: str) -> Dataset:
        try:
            path, spec = self._spec[name]
        except KeyError as e:
            raise DatasetNotFoundError(name) from e

        lock = self._lock[name]
        spec_obj = self._spec_from_info(spec)
        lock_obj = self._lock_from_info(lock)
        return Dataset(manifest_path=path, spec=spec_obj, lock=lock_obj)

    def by_url(self, url: str) -> Optional[Dataset]:
        for name, (_, ds) in self._spec.items():
            if ds["url"] == url:
                return self.get(name)
        return None

    def __contains__(self, name: object) -> bool:
        return name in self._spec

    def __iter__(self) -> Iterator[str]:
        return iter(self._spec)

    def __len__(self) -> int:
        return len(self._spec)

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
            lock = datasets_lock.get(path, [])
            return next((dataset for dataset in lock if dataset["name"] == name), None)

        return {ds["name"]: find(path, name) for name, (path, ds) in self._spec.items()}

    def _reset(self):
        self.__dict__.pop("_spec", None)
        self.__dict__.pop("_lock", None)

    def _spec_from_info(self, info: dict[str, Any]) -> Spec:
        klasses: dict[str, type[Spec]] = {
            "dvc": DVCDataset,
            "dvcx": DVCXDataset,
            "url": URLDataset,
        }
        cls = klasses[info["type"]]
        return cls.from_dict(info)

    def _lock_from_info(self, info: Optional[dict[str, Any]] = None) -> Optional[Lock]:
        klasses: dict[str, type[Lock]] = {
            "dvc": DVCDatasetLock,
            "dvcx": DVCXDatasetLock,
            "url": URLDatasetLock,
        }

        info = info or {}
        if cls := klasses.get(info.get("type", None)):
            return cls.from_dict(info)
        return None

    def add(
        self, url: str, name: str, manifest_path: str = "dvc.yaml", **kwargs
    ) -> Dataset:
        kwargs.update({"url": url, "name": name})
        spec = self._spec_from_info(kwargs)
        dataset = Dataset(manifest_path=manifest_path, spec=spec)
        dataset = dataset.update(self.repo)

        self.dump(dataset)
        return dataset

    def update(self, name, **kwargs) -> tuple[Dataset, Dataset]:
        dataset = self[name]
        new = dataset.update(self.repo, **kwargs)

        self.dump(new, old=dataset)
        return dataset, new

    def _dump_spec(self, manifest_path: str, spec: Spec) -> None:
        spec_data = spec.to_dict()
        project_file = ProjectFile(self.repo, manifest_path)
        project_file.dump_dataset(spec_data)

    def _dump_lock(self, manifest_path: str, lock: Lock) -> None:
        lock_data = lock.to_dict()
        lockfile = Lockfile(self.repo, Path(manifest_path).with_suffix(".lock"))
        lockfile.dump_dataset(lock_data)

    def dump(self, dataset: Dataset, old: Optional[Dataset] = None) -> None:
        if not old or old.spec != dataset.spec:
            self._dump_spec(dataset.manifest_path, dataset.spec)
        if dataset.lock and (not old or old.lock != dataset.lock):
            self._dump_lock(dataset.manifest_path, dataset.lock)
