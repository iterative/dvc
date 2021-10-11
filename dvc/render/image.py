import os
from typing import TYPE_CHECKING

from dvc.render import Renderer
from dvc.render.utils import get_files
from dvc.utils import relpath

if TYPE_CHECKING:
    from dvc.types import StrPath


class ImageRenderer(Renderer):
    DIV = """
        <div
            id="{id}"
            style="border: 1px solid;">
            {partial}
        </div>"""

    def _write_image(
        self,
        page_dir_path: "StrPath",
        revision: str,
        filename: str,
        image_data: bytes,
    ):
        static = os.path.join(page_dir_path, "static")
        os.makedirs(static, exist_ok=True)

        img_path = os.path.join(
            static, f"{revision}_{filename.replace(os.sep, '_')}"
        )
        with open(img_path, "wb") as fd:
            fd.write(image_data)
        return """
        <div>
            <p>{title}</p>
            <img src="{src}">
        </div>""".format(
            title=revision, src=(relpath(img_path, page_dir_path))
        )

    def _convert(self, path: "StrPath"):
        div_content = []
        for rev, rev_data in self.data.items():
            if "data" in rev_data:
                for file, file_data in rev_data.get("data", {}).items():
                    if "data" in file_data:
                        div_content.append(
                            self._write_image(
                                path, rev, file, file_data["data"]
                            )
                        )
        if div_content:
            div_content.insert(0, f"<p>{self.filename}</p>")
            return "\n".join(div_content)
        return ""

    @staticmethod
    def matches(data):
        files = get_files(data)
        extensions = set(map(lambda f: os.path.splitext(f)[1], files))
        return extensions.issubset({".jpg", ".jpeg", ".gif", ".png"})
