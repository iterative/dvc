from typing import Dict, Iterable, List

from tabulate import tabulate

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


def metrics_embedding(metrics: Dict[str, Dict]) -> str:
    header: List[str] = []
    rows: List[List[str]] = []

    for _, rev_data in metrics.items():
        for _, data in rev_data.items():
            if not header:
                header.extend(sorted(data.keys()))

            rows.append([data[key] for key in header])
    return tabulate(rows, header, tablefmt="html")


def plots_embeddings(plots: Dict[str, Dict]) -> List[str]:
    return [
        VEGA_DIV_HTML.format(id=f"plot{i}", vega_json=plot)
        for i, plot in enumerate(plots.values())
    ]


def embed(elements: Iterable[str]) -> str:
    return PAGE_HTML.format(divs="\n".join(elements))
