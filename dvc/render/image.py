import json
import os
from typing import TYPE_CHECKING

from funcy import reraise

from dvc.exceptions import DvcException
from dvc.render.base import Renderer
from dvc.render.utils import get_files
from dvc.utils import relpath

if TYPE_CHECKING:
    from dvc.types import StrPath


class ImageRenderer(Renderer):
    TYPE = "image"
    DIV = """
        <div
            id="{id}"
            style="border:1px solid black;text-align:center;
            white-space: nowrap;overflow-y:hidden;">
            {partial}
        </div>"""

    SCRIPTS = ""

    @property
    def needs_output_path(self):
        return True

    def _write_image(
        self,
        path: "StrPath",
        revision: str,
        filename: str,
        image_data: bytes,
    ):
        img_path = os.path.join(
            path, f"{revision}_{filename.replace(os.sep, '_')}"
        )
        with open(img_path, "wb") as fd:
            fd.write(image_data)

        return img_path

    def _save_images(self, path: "StrPath"):

        for rev, rev_data in self.data.items():
            if "data" in rev_data:
                for file, file_data in rev_data.get("data", {}).items():
                    if "data" in file_data:
                        if not os.path.isdir(path):
                            os.makedirs(path, exist_ok=True)
                        yield rev, file, self._write_image(
                            os.path.abspath(path), rev, file, file_data["data"]
                        )

    def partial_html(self, **kwargs):
        path = kwargs.get("path", None)
        if not path:
            raise DvcException("Can't save here")
        static = os.path.join(path, "static")

        div_content = []
        for rev, _, img_path in self._save_images(static):
            div_content.append(
                """
        <div
            style="border:1px dotted black;margin:2px;display: inline-block;
            overflow:hidden;margin-left:8px;">
            <p>{title}</p>
            <img src="{src}">
        </div>""".format(
                    title=rev, src=(relpath(img_path, path))
                )
            )
        if div_content:
            div_content.insert(0, f"<p>{self.filename}</p>")
            return "\n".join(div_content)
        return ""

    def as_json(self, **kwargs):

        with reraise(
            KeyError,
            DvcException(
                f"{type(self).__name__} needs 'path' to store images."
            ),
        ):
            path = kwargs["path"]

        results = []

        for revision, _, img_path in self._save_images(path):
            results.append(
                {
                    self.TYPE_KEY: self.TYPE,
                    self.REVISIONS_KEY: [revision],
                    "url": img_path,
                }
            )

        return json.dumps(results)

    @staticmethod
    def matches(data):
        files = get_files(data)
        extensions = set(map(lambda f: os.path.splitext(f)[1], files))
        return extensions.issubset({".jpg", ".jpeg", ".gif", ".png"})
