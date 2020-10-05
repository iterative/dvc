import logging
from itertools import starmap

from funcy import join

from dvc.parsing.context import Context
from dvc.parsing.interpolate import resolve

logger = logging.getLogger(__name__)

ARGS = "args"
IMPORTS = "imports"
CONSTANTS = "constants"
STAGES = "stages"
COLON = ":"
PARAMS = "params"


class DataLoader:
    def __init__(self, d):
        """
        Args:
            d: data loaded from dvc.yaml or .dvc files
        """
        self.d = d
        args = d.get(ARGS, {})

        imports = args.get(IMPORTS, [])
        constants = args.get(CONSTANTS, [])
        self.file_ctx = Context.load_variables(imports, constants)

    def _resolve_entry(self, name, definition):
        # TODO: we might need to create a context out of stage's definition
        #  (think of `build-matrix` or `foreach`), empty dict for now.
        contexts = {}
        iter_keys = definition.pop("iter_keys", None)
        if iter_keys is not None:
            for key in iter_keys:
                contexts[f"{name}-{key}"] = context = Context.clone(
                    self.file_ctx
                )
                # setting as a local variable/raising up the variables
                context["item"] = context.select(key)
                context["key"] = key
        else:
            contexts[name] = Context.clone(self.file_ctx)

        return {
            name: self._resolve_stage(context, name, definition)
            for name, context in contexts.items()
        }

    def _resolve_stage(self, context, name, definition):
        logger.debug("Context for stage %s: %s", name, context)

        stage_d = resolve(definition, context)
        logger.debug("Resolved data: %s", stage_d)
        logger.debug("Tracking params: %s", context.tracked)

        stage_d[PARAMS] = stage_d.get(PARAMS, [])
        stage_d[PARAMS].extend(context.tracked)

        return stage_d

    def resolve(self):
        stages = self.d.get(STAGES, {})
        data = join(starmap(self._resolve_entry, stages.items()))
        return {**self.d, STAGES: data}
