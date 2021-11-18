import abc
from typing import TYPE_CHECKING, Dict

from dvc.exceptions import DvcException

if TYPE_CHECKING:
    from dvc.types import StrPath


REVISION_FIELD = "rev"
INDEX_FIELD = "step"


class BadTemplateError(DvcException):
    pass


class Renderer(abc.ABC):
    REVISIONS_KEY = "revisions"
    TYPE_KEY = "type"

    def __init__(self, data: Dict, **kwargs):
        self.data = data

        from dvc.render.utils import get_files

        files = get_files(self.data)

        # we assume comparison of same file between revisions for now
        assert len(files) == 1
        self.filename = files.pop()

    def partial_html(self, **kwargs):
        """
        Us this method to generate partial HTML content,
        that will fill self.DIV
        """

        raise NotImplementedError

    @property
    @abc.abstractmethod
    def TYPE(self):
        raise NotImplementedError

    @property
    @abc.abstractmethod
    def DIV(self):
        raise NotImplementedError

    @property
    @abc.abstractmethod
    def SCRIPTS(self):
        raise NotImplementedError

    @abc.abstractmethod
    def as_json(self, **kwargs):
        raise NotImplementedError

    @staticmethod
    def _remove_special_chars(string: str):
        return string.translate(
            {ord(c): "_" for c in r"!@#$%^&*()[]{};,<>?\/:.|`~=_+"}
        )

    @property
    def needs_output_path(self):
        return False

    def generate_html(self, path: "StrPath"):
        """this method might edit content of path"""
        partial = self.partial_html(path=path)

        div_id = self._remove_special_chars(self.filename)
        div_id = f"plot_{div_id}"

        return self.DIV.format(id=div_id, partial=partial)
