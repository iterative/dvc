from pydantic import BaseModel as PydanticBaseModel
from pydantic import Extra


class BaseModel(PydanticBaseModel):
    class Config:
        # TODO: figure out a way to make it configurable
        extra = Extra.forbid
