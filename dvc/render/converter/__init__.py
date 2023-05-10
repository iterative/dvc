from typing import Any, Dict, List, Optional, Tuple


class Converter:
    def __init__(
        self,
        plot_id: str,
        data: Optional[Dict[str, Any]] = None,
        properties: Optional[Dict] = None,
    ):
        self.plot_id = plot_id
        self.properties = properties or {}
        self.data = data or {}

    def convert(self) -> Tuple[List[Tuple[str, str, Any]], Dict]:
        raise NotImplementedError

    def flat_datapoints(self, revision: str) -> Tuple[List[Dict], Dict]:
        raise NotImplementedError
