import json
import os
from typing import TYPE_CHECKING, Iterable, Optional

try:
    import importlib_resources
except ImportError:
    import importlib.resources as importlib_resources  # type: ignore[no-redef]

from funcy import cached_property

from dvc.exceptions import DvcException

if TYPE_CHECKING:
    from dvc.types import StrPath


class TemplateNotFoundError(DvcException):
    def __init__(self, name):
        super().__init__(f"Template '{name}' not found.")


class BadTemplateError(DvcException):
    pass


class NoFieldInDataError(DvcException):
    def __init__(self, field_name):
        super().__init__(
            f"Field '{field_name}' does not exist in provided data."
        )


class Template:
    INDENT = 4
    SEPARATORS = (",", ": ")
    EXTENSION = ".json"
    ANCHOR = "<DVC_METRIC_{}>"

    def __init__(self, content, name):
        self.content = content
        self.name = name

    def render(self, data, props=None):
        props = props or {}

        if self._anchor_str("data") not in self.content:
            anchor = self.anchor("data")
            raise BadTemplateError(
                f"Template '{self.name}' is not using '{anchor}' anchor"
            )

        if props.get("x"):
            Template._check_field_exists(data, props.get("x"))
        if props.get("y"):
            Template._check_field_exists(data, props.get("y"))

        content = self._fill_anchor(self.content, "data", data)
        content = self._fill_metadata(content, props)

        return content

    @classmethod
    def anchor(cls, name):
        return cls.ANCHOR.format(name.upper())

    def has_anchor(self, name):
        return self._anchor_str(name) in self.content

    @classmethod
    def _fill_anchor(cls, content, name, value):
        value_str = json.dumps(
            value, indent=cls.INDENT, separators=cls.SEPARATORS, sort_keys=True
        )
        return content.replace(cls._anchor_str(name), value_str)

    @classmethod
    def _anchor_str(cls, name):
        return f'"{cls.anchor(name)}"'

    @classmethod
    def _fill_metadata(cls, content, props):
        props.setdefault("title", "")
        props.setdefault("x_label", props.get("x"))
        props.setdefault("y_label", props.get("y"))

        names = ["title", "x", "y", "x_label", "y_label"]
        for name in names:
            value = props.get(name)
            if value is not None:
                content = cls._fill_anchor(content, name, value)

        return content

    @staticmethod
    def _check_field_exists(data, field):
        if not any(field in row for row in data):
            raise NoFieldInDataError(field)


class PlotTemplates:
    TEMPLATES_DIR = "plots"
    PKG_TEMPLATES_DIR = "templates"

    @cached_property
    def templates_dir(self):
        return os.path.join(self.dvc_dir, self.TEMPLATES_DIR)

    @staticmethod
    def _find(templates, template_name):
        for template in templates:
            if template.endswith(template_name) or template.endswith(
                template_name + ".json"
            ):
                return template
        return None

    def _find_in_project(self, name: str) -> Optional["StrPath"]:
        full_path = os.path.abspath(name)
        if os.path.exists(full_path):
            return full_path

        if os.path.exists(self.templates_dir):
            templates = [
                os.path.join(root, file)
                for root, _, files in os.walk(self.templates_dir)
                for file in files
            ]
            found = self._find(templates, name)
            if found:
                return os.path.join(self.templates_dir, found)
        return None

    @staticmethod
    def _get_templates() -> Iterable[str]:
        if (
            importlib_resources.files(__package__)
            .joinpath(PlotTemplates.PKG_TEMPLATES_DIR)
            .is_dir()
        ):
            entries = (
                importlib_resources.files(__package__)
                .joinpath(PlotTemplates.PKG_TEMPLATES_DIR)
                .iterdir()
            )
            return [entry.name for entry in entries]
        return []

    @staticmethod
    def _load_from_pkg(name):
        templates = PlotTemplates._get_templates()
        found = PlotTemplates._find(templates, name)
        if found:
            return (
                (
                    importlib_resources.files(__package__)
                    / PlotTemplates.PKG_TEMPLATES_DIR
                    / found
                )
                .read_bytes()
                .decode("utf-8")
            )
        return None

    def load(self, name: str = None) -> Template:

        if name is not None:
            template_path = self._find_in_project(name)
            if template_path:
                with open(template_path, "r", encoding="utf-8") as fd:
                    content = fd.read()
                return Template(content, name)
        else:
            name = "linear"

        content = self._load_from_pkg(name)
        if content:
            return Template(content, name)

        raise TemplateNotFoundError(name)

    def __init__(self, dvc_dir):
        self.dvc_dir = dvc_dir

    def init(self):
        from dvc.utils.fs import makedirs

        makedirs(self.templates_dir, exist_ok=True)

        templates = self._get_templates()
        for template in templates:
            content = (
                importlib_resources.files(__package__)
                .joinpath(PlotTemplates.PKG_TEMPLATES_DIR)
                .joinpath(template)
                .read_text()
            )
            with open(
                os.path.join(self.templates_dir, template),
                "w",
                encoding="utf-8",
            ) as fd:
                fd.write(content)
