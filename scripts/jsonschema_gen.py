try:
    # pylint: disable=unused-import
    from typing import TypedDict
except ImportError:
    # pylint: disable=unused-import
    from typing_extensions import TypedDict  # noqa: F401

from typing import Any, Dict, Optional, Set, Union

from pydantic import BaseModel, Field

FilePath = str
ParamKey = str
StageName = str


class OutFlags(BaseModel):
    cache: Optional[bool] = Field(True, description="Cache output by DVC")
    persist: Optional[bool] = Field(
        False, description="Persist output between runs"
    )


class PlotFlags(OutFlags):
    x: str = Field(
        None, description="Default field name to use as x-axis data"
    )
    y: str = Field(
        None, description="Default field name to use as y-axis data"
    )
    x_label: str = Field(None, description="Default label for the x-axis")
    y_label: str = Field(None, description="Default label for the y-axis")
    title: str = Field(None, description="Default plot title")
    header: bool = Field(
        False, description="Whether the target CSV or TSV has a header or not"
    )
    template: str = Field(None, description="Default plot template")


class DepModel(BaseModel):
    __root__: FilePath = Field(..., description="A dependency for the stage")


class Dependencies(BaseModel):
    __root__: Set[DepModel]


class CustomParamFileKeys(BaseModel):
    __root__: Dict[FilePath, Set[ParamKey]]


class Param(BaseModel):
    __root__: Union[ParamKey, CustomParamFileKeys]


class Params(BaseModel):
    __root__: Set[Param]


class Out(BaseModel):
    __root__: Union[FilePath, Dict[FilePath, OutFlags]]


class Outs(BaseModel):
    __root__: Set[Out]


class Plot(BaseModel):
    __root__: Union[FilePath, Dict[FilePath, PlotFlags]]


class Plots(BaseModel):
    __root__: Set[Plot]


class Stage(BaseModel):
    cmd: str = Field(..., description="Command to run")
    wdir: Optional[str] = Field(None, description="Working directory")
    deps: Optional[Dependencies] = Field(
        None, description="Dependencies for the stage"
    )
    params: Optional[Params] = Field(None, description="Params for the stage")
    outs: Optional[Outs] = Field(None, description="Outputs of the stage")
    metrics: Optional[Outs] = Field(None, description="Metrics of the stage")
    plots: Optional[Plots] = Field(None, description="Plots of the stage")
    frozen: Optional[bool] = Field(
        False, description="Assume stage as unchanged"
    )
    always_changed: Optional[bool] = Field(
        False, description="Assume stage as always changed"
    )
    meta: Any = Field(None, description="Additional information/metadata")

    class Config:
        allow_mutation = False


Stages = Dict[StageName, Stage]


class DvcYamlModel(BaseModel):
    stages: Stages = Field(..., description="List of stages")

    class Config:
        title = "dvc.yaml"


if __name__ == "__main__":
    print(DvcYamlModel.schema_json(indent=2))
