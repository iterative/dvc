from typing import Literal, Optional, Union

from pydantic import BaseModel, Field
from typing_extensions import Annotated


class DVCDataset(BaseModel):
    type: Literal["dvc"] = "dvc"
    name: str
    url: str
    path: str
    rev: Optional[str] = None


class WebDataset(BaseModel):
    type: Literal["webdataset"] = "webdataset"
    name: str
    url: str


class DVCxDataset(BaseModel):
    type: Literal["dvcx"] = "dvcx"
    name: str
    version: Optional[int] = None


SupportedDatasets = Union[DVCDataset, WebDataset, DVCxDataset]
Dataset = Annotated[SupportedDatasets, Field(discriminator="type")]
