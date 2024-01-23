import json
from dataclasses import asdict, dataclass, field
from datetime import datetime
from typing import TYPE_CHECKING, Any, Dict, Iterator, List, Literal, Optional

from dvc.exceptions import DvcException
from dvc.repo.metrics.show import _gather_metrics
from dvc.repo.params.show import _gather_params
from dvc.utils import relpath

if TYPE_CHECKING:
    from dvc.repo import Repo
    from dvc.repo.metrics.show import FileResult


class DeserializeError(DvcException):
    pass


class _ISOEncoder(json.JSONEncoder):
    def default(self, o: object) -> Any:
        if isinstance(o, datetime):
            return o.isoformat()
        return super().default(o)


@dataclass(frozen=True)
class SerializableExp:
    """Serializable experiment data."""

    rev: str
    timestamp: Optional[datetime] = None
    params: Dict[str, "FileResult"] = field(default_factory=dict)
    metrics: Dict[str, "FileResult"] = field(default_factory=dict)
    deps: Dict[str, "ExpDep"] = field(default_factory=dict)
    outs: Dict[str, "ExpOut"] = field(default_factory=dict)
    meta: Dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_repo(
        cls,
        repo: "Repo",
        rev: Optional[str] = None,
        param_deps: bool = False,
        **kwargs,
    ) -> "SerializableExp":
        """Returns a SerializableExp from the current repo state.

        Params, metrics, deps, outs are filled via repo fs/index, all other fields
        should be passed via kwargs.
        """
        from dvc.dependency import ParamsDependency, RepoDependency

        rev = rev or repo.get_rev()
        assert rev

        params = _gather_params(repo, deps_only=param_deps, on_error="return")
        metrics = _gather_metrics(repo, on_error="return")
        return cls(
            rev=rev,
            params=params,
            metrics=metrics,
            deps={
                relpath(dep.fs_path, repo.root_dir): ExpDep(
                    hash=dep.hash_info.value if dep.hash_info else None,
                    size=dep.meta.size if dep.meta else None,
                    nfiles=dep.meta.nfiles if dep.meta else None,
                )
                for dep in repo.index.deps
                if not isinstance(dep, (ParamsDependency, RepoDependency))
            },
            outs={
                relpath(out.fs_path, repo.root_dir): ExpOut(
                    hash=out.hash_info.value if out.hash_info else None,
                    size=out.meta.size if out.meta else None,
                    nfiles=out.meta.nfiles if out.meta else None,
                    use_cache=out.use_cache,
                    is_data_source=out.stage.is_data_source,
                )
                for out in repo.index.outs
                if not (out.is_metric or out.is_plot)
            },
            **kwargs,
        )

    def dumpd(self) -> Dict[str, Any]:
        return asdict(self)

    def as_bytes(self) -> bytes:
        return _ISOEncoder().encode(self.dumpd()).encode("utf-8")

    @classmethod
    def from_bytes(cls, data: bytes):
        try:
            parsed = json.loads(data)
            if "timestamp" in parsed:
                parsed["timestamp"] = datetime.fromisoformat(parsed["timestamp"])
            if "deps" in parsed:
                parsed["deps"] = {k: ExpDep(**v) for k, v in parsed["deps"].items()}
            if "outs" in parsed:
                parsed["outs"] = {k: ExpOut(**v) for k, v in parsed["outs"].items()}
            return cls(**parsed)
        except (TypeError, json.JSONDecodeError) as exc:
            raise DeserializeError("failed to load SerializableExp") from exc

    @property
    def contains_error(self) -> bool:
        return any(value.get("error") for value in self.params.values()) or any(
            value.get("error") for value in self.metrics.values()
        )


@dataclass(frozen=True)
class ExpDep:
    hash: Optional[str]
    size: Optional[int]
    nfiles: Optional[int]


@dataclass(frozen=True)
class ExpOut:
    hash: Optional[str]
    size: Optional[int]
    nfiles: Optional[int]
    use_cache: bool
    is_data_source: bool


@dataclass(frozen=True)
class SerializableError:
    msg: str
    type: str = ""

    def dumpd(self) -> Dict[str, Any]:
        return asdict(self)

    def as_bytes(self) -> bytes:
        return json.dumps(self.dumpd()).encode("utf-8")

    @classmethod
    def from_bytes(cls, data: bytes):
        try:
            parsed = json.loads(data)
            return cls(**parsed)
        except (TypeError, json.JSONDecodeError) as exc:
            raise DeserializeError("failed to load SerializableError") from exc


@dataclass
class ExpState:
    """Git/DVC experiment state."""

    rev: str
    name: Optional[str] = None
    data: Optional[SerializableExp] = None
    error: Optional[SerializableError] = None
    experiments: Optional[List["ExpRange"]] = None

    def dumpd(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass
class ExpRange:
    revs: List["ExpState"]
    executor: Optional["ExpExecutor"] = None
    name: Optional[str] = None

    def __len__(self) -> int:
        return len(self.revs)

    def __iter__(self) -> Iterator["ExpState"]:
        return iter(self.revs)

    def __getitem__(self, index: int) -> "ExpState":
        return self.revs[index]

    def dumpd(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass
class LocalExpExecutor:
    root: Optional[str] = None
    log: Optional[str] = None
    pid: Optional[int] = None
    returncode: Optional[int] = None
    task_id: Optional[str] = None


@dataclass
class ExpExecutor:
    state: Literal["success", "queued", "running", "failed"]
    name: Optional[str] = None
    local: Optional[LocalExpExecutor] = None
