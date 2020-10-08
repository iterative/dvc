from itertools import starmap

from funcy import join

from .context import Context
from .interpolate import resolve

STAGES = "stages"


class DataResolver:
    def __init__(self, d):
        self.context = Context()
        self.data = d

    def _resolve_entry(self, name, definition):
        stage_d = resolve(definition, self.context)
        return {name: stage_d}

    def resolve(self):
        stages = self.data.get(STAGES, {})
        data = join(starmap(self._resolve_entry, stages.items()))
        return {**self.data, STAGES: data}
