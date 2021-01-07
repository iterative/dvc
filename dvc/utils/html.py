from typing import Dict, List, Optional

PAGE_HTML = """<!DOCTYPE html>
<html>
<head>
    <title>DVC Plot</title>
    <script src="https://cdn.jsdelivr.net/npm/vega@5.10.0"></script>
    <script src="https://cdn.jsdelivr.net/npm/vega-lite@4.8.1"></script>
    <script src="https://cdn.jsdelivr.net/npm/vega-embed@6.5.1"></script>
</head>
<body>
    {divs}
</body>
</html>"""

VEGA_DIV_HTML = """<div id = "{id}"></div>
<script type = "text/javascript">
    var spec = {vega_json};
    vegaEmbed('#{id}', spec);
</script>"""


class HTML:
    def __init__(self):
        self.elements = []

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

    def with_plots(self, plots: Dict[str, Dict]) -> "HTML":
        self.elements.extend(
            [
                VEGA_DIV_HTML.format(id=f"plot{i}", vega_json=plot)
                for i, plot in enumerate(plots.values())
            ]
        )
        return self

    def with_element(self, html: str) -> "HTML":
        self.elements.append(html)
        return self

    def embed(self) -> str:
        return PAGE_HTML.format(divs="\n".join(self.elements))


def write(
    path, plots: Dict[str, Dict], metrics: Optional[Dict[str, Dict]] = None
):
    document = HTML()
    if metrics:
        document.with_metrics(metrics)
        document.with_element("<br>")

    document.with_plots(plots)

    with open(path, "w") as fd:
        fd.write(document.embed())
