from typing import Dict, Optional


class Converter:
    def __init__(self, plot_properties: Optional[Dict] = None):
        self.plot_properties = plot_properties or {}

    def convert(self, data, revision: str, filename: str, **kwargs):
        raise NotImplementedError
