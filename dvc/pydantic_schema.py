import warnings
from typing import TYPE_CHECKING, Dict, List, NewType, Optional, TypeVar, Union

import pydantic
from pydantic import ConfigDict, Field
from pydantic.functional_validators import BeforeValidator
from typing_extensions import Annotated

if TYPE_CHECKING:
    from dvc.fs import FileSystem
    from dvc.types import StrOrBytesPath


_T = TypeVar("_T")

_Key = TypeVar("_Key")
_Config = TypeVar("_Config")

OneOrMore = Union[_T, List[_T]]
Config = Union[_Key, Dict[_Key, _Config]]

PathLike = NewType("PathLike", str)
PathOrId = NewType("PathOrId", str)
TemplateNameOrPath = NewType("TemplateNameOrPath", str)
ParamPath = NewType("ParamPath", str)
PlotColumn = NewType("PlotColumn", str)


class BaseModel(pydantic.BaseModel):
    model_config = ConfigDict(extra="forbid")


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
    labels: List[str] = Field(default_factory=list)
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
    params: List[Union[ParamPath, Dict[PathLike, Optional[List[ParamPath]]]]] = Field(
        default_factory=list
    )
    frozen: bool = False
    meta: object = None
    desc: Optional[str] = None
    always_changed: bool = False
    outs: List[Config[PathLike, OutputConfig]] = Field(default_factory=list)
    metrics: List[Config[PathLike, MetricConfig]] = Field(default_factory=list)
    plots: List[Union[PathLike, Dict[PathLike, OneOrMore[PlotConfig]]]] = Field(
        default_factory=list
    )


class ForeachDo(BaseModel):
    foreach: Union[str, Dict, List]
    do: Stage


def foreach_or_stage_validator(v):
    if isinstance(v, dict) and "foreach" in v:
        return ForeachDo.model_validate(v)
    return Stage.model_validate(v)


ForeachDoOrStage = Annotated[
    Union[ForeachDo, Stage], BeforeValidator(foreach_or_stage_validator)
]


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
    stages: Dict[str, ForeachDoOrStage] = Field(default_factory=dict)
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
