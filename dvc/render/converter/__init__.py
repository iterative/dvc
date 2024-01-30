from typing import Any, Optional


class Converter:
    def __init__(
        self,
        plot_id: str,
        data: Optional[dict[str, Any]] = None,
        properties: Optional[dict] = None,
    ):
        self.plot_id = plot_id
        self.properties = properties or {}
        self.data = data or {}

    def convert(self) -> tuple[list[tuple[str, str, Any]], dict]:
        raise NotImplementedError

    def flat_datapoints(self, revision: str) -> tuple[list[dict], dict]:
        raise NotImplementedError
