import json
from dataclasses import asdict, dataclass, field
from datetime import datetime
from typing import TYPE_CHECKING, Any, Callable, Dict, Optional

from dvc.exceptions import DvcException
from dvc.repo.metrics.show import _gather_metrics
from dvc.repo.params.show import _gather_params
from dvc.utils import onerror_collect, relpath

if TYPE_CHECKING:
    from dvc.repo import Repo


class DeserializeError(DvcException):
    pass


class _ISOEncoder(json.JSONEncoder):
    def default(self, o: object) -> Any:
        if isinstance(o, datetime):
            return o.isoformat()
        return super().default(o)


@dataclass(frozen=True)
class SerializableExp:
    """Serializable experiment state."""

    rev: str
    timestamp: Optional[datetime] = None
    params: Dict[str, Any] = field(default_factory=dict)
    metrics: Dict[str, Any] = field(default_factory=dict)
    deps: Dict[str, "_ExpDep"] = field(default_factory=dict)
    outs: Dict[str, "_ExpOut"] = field(default_factory=dict)
    status: Optional[str] = None
    executor: Optional[str] = None
    error: Optional["SerializableError"] = None

    @classmethod
    def from_repo(
        cls,
        repo: "Repo",
        rev: Optional[str] = None,
        onerror: Optional[Callable] = None,
        **kwargs,
    ) -> "SerializableExp":
        """Returns a SerializableExp from the current repo state.

        Params, metrics, deps, outs are filled via repo fs/index, all other fields
        should be passed via kwargs.
        """
        from dvc.dependency import ParamsDependency, RepoDependency

        if not onerror:
            onerror = onerror_collect

        rev = rev or repo.get_rev()
        assert rev
        status: Optional[str] = kwargs.get("status")
        # NOTE: _gather_params/_gather_metrics return defaultdict which is not
        # supported in dataclasses.asdict() on all python releases
        # see https://bugs.python.org/issue35540
        params = dict(_gather_params(repo, onerror=onerror))
        if status and status.lower() in ("queued", "failed"):
            metrics: Dict[str, Any] = {}
        else:
            metrics = dict(
                _gather_metrics(
                    repo,
                    targets=None,
                    rev=rev[:7],
                    recursive=False,
                    onerror=onerror_collect,
                )
            )
        return cls(
            rev=rev,
            params=params,
            metrics=metrics,
            deps={
                relpath(dep.fs_path, repo.root_dir): _ExpDep(
                    hash=dep.hash_info.value if dep.hash_info else None,
                    size=dep.meta.size if dep.meta else None,
                    nfiles=dep.meta.nfiles if dep.meta else None,
                )
                for dep in repo.index.deps
                if not isinstance(dep, (ParamsDependency, RepoDependency))
            },
            outs={
                relpath(out.fs_path, repo.root_dir): _ExpOut(
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
            return cls(**parsed)
        except (TypeError, json.JSONDecodeError) as exc:
            raise DeserializeError("failed to load SerializableExp") from exc

    @property
    def contains_error(self) -> bool:
        return (
            self.error is not None
            or self.params.get("error")
            or any(value.get("error") for value in self.params.values())
            or self.metrics.get("error")
            or any(value.get("error") for value in self.metrics.values())
        )


@dataclass(frozen=True)
class _ExpDep:
    hash: Optional[str]  # noqa: A003
    size: Optional[int]
    nfiles: Optional[int]


@dataclass(frozen=True)
class _ExpOut:
    hash: Optional[str]  # noqa: A003
    size: Optional[int]
    nfiles: Optional[int]
    use_cache: bool
    is_data_source: bool


@dataclass(frozen=True)
class SerializableError:
    msg: str
    type: str = ""  # noqa: A003

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
