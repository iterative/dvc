from typing import Any, Dict, List, Optional, Type, TypeVar, Union

from pydantic import Field, validator

from dvc.types import OptStr, SingleOrListOf

from .base import BaseModel


class OutProps(BaseModel):
    cache: bool = True
    persist: bool = False
    checkpoint: bool = False
    desc: Optional[str]


class MetricProps(OutProps):
    pass


FilePath = str


class PlotProps(OutProps):
    template: Optional[FilePath]
    x: OptStr
    y: OptStr
    x_label: OptStr
    y_label: OptStr
    title: OptStr
    header: bool = False


class LiveProps(PlotProps):
    summary: bool = True
    html: bool = True


# eg: "file.txt", "file.txt:foo,bar", "file.txt:foo"
VarImportSpec = str  # validate here?
# {"foo" (str) : "foobar" (Any) }
LocalVarKey = str
LocalVarValue = Any
VarsSpec = List[Union[VarImportSpec, Dict[LocalVarKey, LocalVarValue]]]

# key name of the param, usually from `params.yaml`
ParamKey = str
ParamsSpec = List[Union[ParamKey, Dict[FilePath, List[ParamKey]]]]

_Entry = TypeVar("_Entry", covariant=True)
_Flag = TypeVar("_Flag", covariant=True)
EntryWithOptFlags = Union[Dict[_Entry, _Flag], _Entry]


class WithDescription(BaseModel):
    desc: OptStr


class StageDefinition(WithDescription, BaseModel):
    """This is the raw one, which could be parametrized."""

    cmd: SingleOrListOf[str]  # required
    wdir: OptStr
    deps: List[FilePath] = Field(default_factory=list)
    params: ParamsSpec = Field(default_factory=list)
    vars: VarsSpec = Field(default_factory=list)
    frozen: bool = False
    meta: Any
    desc: OptStr
    always_changed: bool = False
    outs: List[EntryWithOptFlags[FilePath, OutProps]] = Field(
        default_factory=list
    )
    plots: List[
        EntryWithOptFlags[FilePath, SingleOrListOf[PlotProps]]
    ] = Field(default_factory=list)
    metrics: List[EntryWithOptFlags[FilePath, MetricProps]] = Field(
        default_factory=list
    )
    live: EntryWithOptFlags[FilePath, LiveProps] = Field(default_factory=list)

    # Note: we don't support parametrization in props and in
    # frozen/always_changed/meta yet.


# trying to differentiate here between normal str expectation
# and parametrized ones
ParametrizedString = str  # validate with constr()?

ListAny = List[Any]
DictStrAny = Dict[str, Any]


class ForeachDo(BaseModel):
    foreach: Union[ParametrizedString, ListAny, DictStrAny]
    do: StageDefinition


Definition = Union[ForeachDo, StageDefinition]
StageName = str


class Schema(BaseModel):
    vars: VarsSpec = Field(default_factory=list)
    stages: Dict[StageName, Definition] = Field(default_factory=dict)

    @validator("stages", each_item=True, pre=True)
    @classmethod
    def validate_stages(cls, v: Any):
        if not isinstance(v, dict):
            raise TypeError("must be a dict")

        if v.keys() & {"foreach", "do"}:
            return ForeachDo.parse_obj(v)

        return StageDefinition.parse_obj(v)

    class Config:
        title = "dvc.yaml schema"


def get_schema(extra: str = "forbid") -> Type[Schema]:
    assert extra
    return Schema


if __name__ == "__main__":
    from pathlib import Path

    from IPython import embed

    from dvc.utils.serialize import load_yaml

    path = Path(__file__).parents[3] / "example-get-started" / "dvc.yaml"
    d = load_yaml(path)
    s = get_schema().parse_obj(d)
    embed(colors="neutral")
