import json
from collections import defaultdict
from typing import TYPE_CHECKING, Any, Dict, Optional

from dvc.render.base import Renderer

if TYPE_CHECKING:
    from dvc.compare import TabularData


class ParallelCoordinatesRenderer(Renderer):
    TYPE = "plotly"

    DIV = """
    <div id = "{id}">
        <script type = "text/javascript">
            var plotly_data = {partial};
            Plotly.newPlot("{id}", plotly_data.data, plotly_data.layout);
        </script>
    </div>
    """

    SCRIPTS = """
    <script src="https://cdn.plot.ly/plotly-latest.min.js"></script>
    """

    # pylint: disable=W0231
    def __init__(
        self,
        tabular_data: "TabularData",
        color_by: Optional[str] = None,
        fill_value: str = "",
    ):
        self.tabular_data = tabular_data
        self.color_by = color_by
        self.filename = "experiments"
        self.fill_value = fill_value

    def partial_html(self, **kwargs):
        return self.as_json()

    def as_json(self, **kwargs) -> str:
        tabular_dict = defaultdict(list)
        for row in self.tabular_data.as_dict():
            for col_name, value in row.items():
                tabular_dict[col_name].append(str(value))

        trace: Dict[str, Any] = {"type": "parcoords", "dimensions": []}
        for label, values in tabular_dict.items():
            is_categorical = False
            try:
                float_values = [
                    float(x) if x != self.fill_value else None for x in values
                ]
            except ValueError:
                is_categorical = True

            if is_categorical:
                non_missing = [x for x in values if x != self.fill_value]
                unique_values = sorted(set(non_missing))
                unique_values.append(self.fill_value)

                dummy_values = [unique_values.index(x) for x in values]

                values = [
                    x if x != self.fill_value else "Missing" for x in values
                ]
                trace["dimensions"].append(
                    {
                        "label": label,
                        "values": dummy_values,
                        "tickvals": dummy_values,
                        "ticktext": values,
                    }
                )
            else:
                trace["dimensions"].append(
                    {"label": label, "values": float_values}
                )

            if label == self.color_by:
                trace["line"] = {
                    "color": dummy_values if is_categorical else float_values,
                    "showscale": True,
                    "colorbar": {"title": self.color_by},
                }
                if is_categorical:
                    trace["line"]["colorbar"]["tickmode"] = "array"
                    trace["line"]["colorbar"]["tickvals"] = dummy_values
                    trace["line"]["colorbar"]["ticktext"] = values

        return json.dumps({"data": [trace], "layout": {}})
