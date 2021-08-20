import logging
from typing import TYPE_CHECKING, Dict

if TYPE_CHECKING:
    from dvc.types import StrPath

logger = logging.getLogger(__name__)


class Renderer:
    def __init__(self, data: Dict):
        self.data = data

        from dvc.render.utils import get_files

        files = get_files(self.data)

        # we assume comparison of same file between revisions for now
        assert len(files) == 1
        self.filename = files.pop()

    def _convert(self, path: "StrPath"):
        raise NotImplementedError

    @property
    def DIV(self):
        raise NotImplementedError

    def generate_html(self, path: "StrPath"):
        """this method might edit content of path"""
        partial = self._convert(path)
        div_id = f"plot_{self.filename.replace('.', '_').replace('/', '_')}"
        return self.DIV.format(id=div_id, partial=partial)
