from copy import copy

from dvc.dependency.local import DependencyLOCAL


class DependencyPARAM(DependencyLOCAL):
    # SCHEMA:
    # - path: <parameter file>
    #   params:
    #   - <parameter name>: <parameter value>
    PARAM_PARAMS = "params"
    PARAM_SCHEMA = {DependencyLOCAL.PARAM_PATH: str, PARAM_PARAMS: {str: str}}

    def __init__(self, stage, path_and_param_name, *args, **kwargs):
        # TODO: Verify format (no more than one ":", and more I guess)
        # TODO: If no file is given, use a default
        path, param_name = path_and_param_name.split(':')
        self.param_name = param_name
        super().__init__(stage, path, *args, **kwargs)
        self.def_path = self.def_path + ':' + param_name  # TODO: Not sure about this

    @property
    def unique_identifier(self):
        return self.param_name

    def save(self):
        # TODO: Verify exists (parse file and get value)
        super().save()