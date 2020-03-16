import json
import re

from dvc.dependency.local import DependencyLOCAL


class DependencyPARAM(DependencyLOCAL):
    # SCHEMA:
    #   params:
    #   - <parameter name>: <parameter value>
    PARAM_PARAMS = "params"
    # TODO: Combine parameter deps across multiple param deps
    PARAM_SCHEMA = {PARAM_PARAMS: {str: str}}
    DELIMITER = ':'
    DEFAULT_PARAMS_FILE = 'PARAMS.json'
    PARAM_NAME_REGEX = re.compile(r'^\w+$')

    def __init__(self, stage, path_and_param_name, *args, **kwargs):
        path, _, param_name = path_and_param_name.rpartition(self.DELIMITER)
        path = path or self.DEFAULT_PARAMS_FILE
        if not self._is_valid_name(param_name):
            raise NotImplementedError()  # TODO: raise BadParamNameError() ?
        super().__init__(stage, path, *args, **kwargs)
        self.param_name = param_name
        self.param_value = self._parse()[param_name]

    def __str__(self):
        path = super().__str__()
        return path + ':' + self.param_name

    @property
    def unique_identifier(self):
        return self.param_name

    # def save(self):
    #     # TODO: Do we need to do anything different regarding `save()`?
    #     super().save()

    def dumpd(self):
        return {
            self.PARAM_PATH: self.def_path,
            self.PARAM_PARAMS: {self.param_name: self.param_value},
        }

    @classmethod
    def _is_valid_name(cls, param_name):
        return cls.PARAM_NAME_REGEX.match(param_name)

    @property
    def exists(self):
        file_exists = super().exists
        params = self._parse()
        param_exists = self.param_name in params
        return file_exists and param_exists

    def _parse(self):
        try:
            return self._params_cache
        except AttributeError:
            path = self.path_info.fspath
            with open(path, 'r') as fp:
                try:
                    self._params_cache = json.load(fp)
                except json.JSONDecodeError:
                    # TODO raise BadParamFileError()?
                    raise NotImplementedError()
            return self._params_cache
