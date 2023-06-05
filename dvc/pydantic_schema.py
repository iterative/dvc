import warnings
from typing import TYPE_CHECKING, Dict, List, NewType, Optional, TypeVar, Union

from pydantic import BaseModel, Field

if TYPE_CHECKING:
    from dvc.fs import FileSystem
    from dvc.types import StrOrBytesPath


_T = TypeVar("_T")
OneOrMore = Union[_T, List[_T]]
ItemWithConfig = Union[str, Dict[str, _T]]
PathLike = NewType("PathLike", str)
PathOrId = NewType("PathOrId", str)
TemplateNameOrPath = NewType("TemplateNameOrPath", str)
ParamPath = NewType("ParamPath", str)
PlotColumn = NewType("PlotColumn", str)


class Plot(BaseModel):
    x: Union[PlotColumn, Dict[PathLike, PlotColumn], None] = None
    y: Union[
        OneOrMore[PlotColumn],
        Dict[PathLike, OneOrMore[PlotColumn]],
        None,
    ] = None
    x_label: Optional[str] = None
    y_label: Optional[str] = None
    title: Optional[str] = None
    template: Optional[TemplateNameOrPath] = None


class OutputConfig(BaseModel):
    desc: Optional[str] = None
    type: Optional[str] = None  # noqa: A003
    labels: Optional[str] = None
    meta: object = None
    cache: bool = True
    persist: bool = False
    remote: Optional[str] = None
    push: bool = True


class MetricConfig(OutputConfig):
    pass


class PlotConfig(OutputConfig):
    template: Optional[TemplateNameOrPath] = None
    x: Optional[PlotColumn] = None
    y: Optional[PlotColumn] = None
    x_label: Optional[str] = None
    y_label: Optional[str] = None
    title: Optional[str] = None
    header: bool = False


class Stage(BaseModel):
    cmd: OneOrMore[str]
    wdir: Optional[PathLike] = None
    deps: List[PathLike] = Field(default_factory=list)
    params: List[Union[ParamPath, Dict[PathLike, List[ParamPath]]]] = Field(
        default_factory=list
    )
    frozen: bool = False
    meta: object = None
    desc: Optional[str] = None
    always_changed: bool = False
    outs: List[ItemWithConfig[OutputConfig]] = Field(default_factory=list)
    metrics: List[ItemWithConfig[MetricConfig]] = Field(default_factory=list)
    plots: List[Union[PathLike, Dict[PathLike, OneOrMore[PlotConfig]]]] = Field(
        default_factory=list
    )


class ForeachDo(BaseModel):
    foreach: Union[str, Dict, List]
    do: Stage


class Artifact(BaseModel):
    path: PathLike
    desc: Optional[str] = None
    type: Optional[str] = None  # noqa: A003
    labels: List[str] = Field(default_factory=list)
    meta: object = None


class Project(BaseModel):
    plots: List[Union[PathLike, Dict[PathOrId, Optional[Plot]]]] = Field(
        default_factory=list
    )
    stages: Dict[str, Union[Stage, ForeachDo]] = Field(default_factory=dict)
    vars: List[Union[PathLike, Dict[str, object]]] = Field(  # noqa; A003
        default_factory=list
    )
    params: List[PathLike] = Field(default_factory=list)
    metrics: List[PathLike] = Field(default_factory=list)
    artifacts: Dict[str, Artifact] = Field(default_factory=dict)

    @classmethod
    def load_from(cls, path: "StrOrBytesPath", fs: Optional["FileSystem"] = None):
        from dvc.utils.serialize import load_yaml

        d = load_yaml(path, fs=fs)
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", category=DeprecationWarning)
            return cls.parse_obj(d)
