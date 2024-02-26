import base64
import json
import logging
import os
from typing import TYPE_CHECKING, Annotated, Any

from pydantic import BaseModel, Field, ValidationError

from dvc.render import ANNOTATIONS, FILENAME, REVISION, SRC

from . import Converter

if TYPE_CHECKING:
    from dvc.types import StrPath

logger = logging.getLogger(__name__)


class ImageConverter(Converter):
    @staticmethod
    def _write_image(
        path: "StrPath",
        revision: str,
        filename: str,
        image_data: bytes,
    ) -> "StrPath":
        img_path = os.path.join(
            path,
            f"{revision}_{filename}".replace(os.sep, "_").replace("/", "_"),
        )
        with open(img_path, "wb") as fd:
            fd.write(image_data)

        return img_path

    @staticmethod
    def _encode_image(image_data: bytes) -> str:
        base64_str = base64.b64encode(image_data).decode()
        return f"data:image;base64,{base64_str}"

    def convert(self) -> tuple[list[tuple[str, str, Any]], dict]:
        datas = [
            (filename, "", image_data)
            for filename, image_data in self.data.items()
            if not filename.endswith(".json")
        ]
        return datas, self.properties

    def flat_datapoints(self, revision: str) -> tuple[list[dict], dict]:
        """
        Convert the DVC Plots content to DVC Render datapoints.
        Return both generated datapoints and updated properties.
        """
        path = self.properties.get("out")
        datapoints = []
        datas, properties = self.convert()

        if "annotations" in properties:
            annotations = self._load_annotations(properties["annotations"])

        for filename, _, image_content in datas:
            if path:
                if not os.path.isdir(path):
                    os.makedirs(path, exist_ok=True)
                src = self._write_image(
                    os.path.abspath(path), revision, filename, image_content
                )
            else:
                src = self._encode_image(image_content)
            datapoint = {
                REVISION: revision,
                FILENAME: filename,
                SRC: src,
                ANNOTATIONS: annotations,
            }
            datapoints.append(datapoint)
        return datapoints, properties

    def _load_annotations(self, path: "StrPath") -> dict:
        with open(path, encoding="utf-8") as annotations_path:
            try:
                return _Annotations(**json.load(annotations_path)).model_dump()
            except json.JSONDecodeError as json_error:
                logger.warning(json_error)
                logger.warning("Annotations file %s is not a valid JSON file.", path)
                return {"annotations": {}}
            except ValidationError as pydantic_error:
                logger.warning(pydantic_error)
                logger.warning(
                    "Annotations file %s is not a valid annotations file.", path
                )
                return {"annotations": {}}


class _Coordinates(BaseModel):
    left: int
    top: int
    bottom: int
    right: int


class _BBoxe(BaseModel):
    box: _Coordinates
    score: Annotated[float, Field(ge=0, le=1)]


class _Annotations(BaseModel):
    annotations: dict[str, list[_BBoxe]]
