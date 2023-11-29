from typing import Literal, Optional, Union

from pydantic import BaseModel, Field
from typing_extensions import Annotated


class DVCDataset(BaseModel):
    type: Literal["dvc"] = "dvc"  # noqa: A003
    name: str
    url: str
    path: str
    rev: Optional[str] = None


class WebDataset(BaseModel):
    type: Literal["webdataset"] = "webdataset"  # noqa: A003
    name: str
    url: str


class DVCxDataset(BaseModel):
    type: Literal["dvcx"] = "dvcx"  # noqa: A003
    name: str
    version: Optional[int] = None


SupportedDatasets = Union[DVCDataset, WebDataset, DVCxDataset]
Dataset = Annotated[SupportedDatasets, Field(discriminator="type")]
