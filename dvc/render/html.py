import os
from pathlib import Path
from typing import TYPE_CHECKING, Dict, List, Optional

from dvc.exceptions import DvcException

if TYPE_CHECKING:
    from dvc.render import Renderer
    from dvc.types import StrPath

PAGE_HTML = """<!DOCTYPE html>
<html>
<head>
    {refresh_tag}
    <title>DVC Plot</title>
    <script src="https://cdn.jsdelivr.net/npm/vega@5.20.2"></script>
    <script src="https://cdn.jsdelivr.net/npm/vega-lite@5.1.0"></script>
    <script src="https://cdn.jsdelivr.net/npm/vega-embed@6.18.2"></script>
</head>
<body>
    {plot_divs}
</body>
</html>"""


class MissingPlaceholderError(DvcException):
    def __init__(self, placeholder):
        super().__init__(f"HTML template has to contain '{placeholder}'.")


class HTML:
    PLACEHOLDER = "plot_divs"
    PLACEHOLDER_FORMAT_STR = f"{{{PLACEHOLDER}}}"
    REFRESH_PLACEHOLDER = "refresh_tag"
    REFRESH_TAG = '<meta http-equiv="refresh" content="{}">'

    def __init__(
        self,
        template: Optional[str] = None,
        refresh_seconds: Optional[int] = None,
    ):
        template = template or PAGE_HTML
        if self.PLACEHOLDER_FORMAT_STR not in template:
            raise MissingPlaceholderError(self.PLACEHOLDER_FORMAT_STR)

        self.template = template
        self.elements: List[str] = []
        self.refresh_tag = ""
        if refresh_seconds is not None:
            self.refresh_tag = self.REFRESH_TAG.format(refresh_seconds)

    def with_metrics(self, metrics: Dict[str, Dict]) -> "HTML":
        import tabulate

        header: List[str] = []
        rows: List[List[str]] = []

        for _, rev_data in metrics.items():
            for _, data in rev_data.items():
                if not header:
                    header.extend(sorted(data.keys()))

                rows.append([data[key] for key in header])

        self.elements.append(tabulate.tabulate(rows, header, tablefmt="html"))
        return self

    def with_element(self, html: str) -> "HTML":
        self.elements.append(html)
        return self

    def embed(self) -> str:
        kwargs = {
            self.PLACEHOLDER: "\n".join(self.elements),
            self.REFRESH_PLACEHOLDER: self.refresh_tag,
        }
        return self.template.format(**kwargs)


def write(
    path: "StrPath",
    renderers: List["Renderer"],
    metrics: Optional[Dict[str, Dict]] = None,
    template_path: Optional["StrPath"] = None,
    refresh_seconds: Optional[int] = None,
):

    os.makedirs(path, exist_ok=True)

    page_html = None
    if template_path:
        with open(template_path) as fobj:
            page_html = fobj.read()

    document = HTML(page_html, refresh_seconds=refresh_seconds)
    if metrics:
        document.with_metrics(metrics)
        document.with_element("<br>")

    for renderer in renderers:
        document.with_element(renderer.generate_html(path))

    index = Path(os.path.join(path, "index.html"))

    with open(index, "w") as fd:
        fd.write(document.embed())
    return index
