import base64
import os
from typing import TYPE_CHECKING, Dict, List, Optional, Tuple

from dvc.render import FILENAME_FIELD, REVISION_FIELD, SRC_FIELD

if TYPE_CHECKING:
    from dvc.types import StrPath


class ImageConverter:
    def __init__(self, plot_properties: Optional[Dict] = None):
        self.plot_properties = plot_properties or {}

    @staticmethod
    def _write_image(
        path: "StrPath",
        revision: str,
        filename: str,
        image_data: bytes,
    ) -> "StrPath":
        img_path = os.path.join(
            path, f"{revision}_{filename.replace(os.sep, '_')}"
        )
        with open(img_path, "wb") as fd:
            fd.write(image_data)

        return img_path

    @staticmethod
    def _encode_image(
        image_data: bytes,
    ) -> str:
        base64_str = base64.b64encode(image_data).decode()
        return f"data:image;base64,{base64_str}"

    def convert(
        self, data: bytes, revision, filename
    ) -> Tuple[List[Dict], Dict]:
        """
        Convert the DVC Plots content to DVC Render datapoints.
        Return both generated datapoints and updated properties.
        """
        path = self.plot_properties.get("out")
        if path:
            if not os.path.isdir(path):
                os.makedirs(path, exist_ok=True)
            src = self._write_image(
                os.path.abspath(path), revision, filename, data
            )
        else:
            src = self._encode_image(data)
        datapoint = {
            REVISION_FIELD: revision,
            FILENAME_FIELD: filename,
            SRC_FIELD: src,
        }
        return [datapoint], self.plot_properties
