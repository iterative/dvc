import json
import re

from dvc.dependency.local import DependencyLOCAL
from dvc.exceptions import DvcException


class BadParamNameError(DvcException):
    def __init__(self, param_name):
        msg = "Parameter name '{}' is not allowed".format(param_name)
        super().__init__(msg)


class BadParamFileError(DvcException):
    def __init__(self, path):
        msg = "Parameter file '{}' could not be read".format(path)
        super().__init__(msg)


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
            raise BadParamNameError(param_name)
        super().__init__(stage, path, *args, **kwargs)
        self.param_name = param_name
        self.param_value = None

    def __str__(self):
        path = super().__str__()
        return path + ':' + self.param_name

    @property
    def unique_identifier(self):
        return self.param_name

    def save(self):
        self.param_value = self._parse()[self.param_name]
        super().save()  # TODO: Not sure if this is needed

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
                    raise BadParamFileError(path)
            return self._params_cache
