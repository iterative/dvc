import base64
import json
import os
from typing import TYPE_CHECKING, Any

from dvc.render import ANNOTATIONS, FILENAME, REVISION, SRC

from . import Converter

if TYPE_CHECKING:
    from dvc.types import StrPath


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
        datas = []
        for filename, image_data in self.data.items():
            datas.append((filename, "", image_data))
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
            with open(properties["annotations"], encoding="utf-8") as annotations_path:
                annotations = json.load(annotations_path)

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
