import json
import re

from dvc.dependency.local import DependencyLOCAL
from dvc.exceptions import DvcException


class BadParamNameError(DvcException):
    def __init__(self, param_name):
        msg = "Parameter name '{}' is not valid".format(param_name)
        super().__init__(msg)


class BadParamFileError(DvcException):
    def __init__(self, path):
        msg = "Parameter file '{}' could not be read".format(path)
        super().__init__(msg)


class DependencyPARAMS(DependencyLOCAL):
    # SCHEMA:
    #   params:
    #   - <parameter name>: <parameter value>
    #   - <parameter name>: <parameter value>
    PARAM_PARAMS = "params"
    PARAM_SCHEMA = {PARAM_PARAMS: {str: str}}
    FILE_DELIMITER = ':'
    PARAM_DELIMITER = ','
    DEFAULT_PARAMS_FILE = 'PARAMS.json'

    REGEX_SUBNAME = r'\w+'
    REGEX_NAME = r'{sub}(\.{sub})*'.format(sub=REGEX_SUBNAME)
    REGEX_MULTI_PARAMS = r'^{param}(,{param})*$'.format(param=REGEX_NAME)
    REGEX_COMPILED = re.compile(REGEX_MULTI_PARAMS)

    def __init__(self, stage, input_str, *args, **kwargs):
        path, _, param_names = input_str.rpartition(self.FILE_DELIMITER)
        path = path or self.DEFAULT_PARAMS_FILE
        if not self._is_valid_name(param_names):
            raise BadParamNameError(param_names)
        super().__init__(stage, path, *args, **kwargs)
        self.param_names = sorted(param_names.split(self.PARAM_DELIMITER))
        self.param_values = {}

    def __str__(self):
        path = super().__str__()
        return path + ':' + self.PARAM_DELIMITER.join(self.param_names)

    def save(self):
        super().save()
        params_in_file = self._parse()
        self.param_values = {k: params_in_file[k] for k in self.param_names}

    def dumpd(self):
        return {
            self.PARAM_PATH: self.def_path,
            self.PARAM_PARAMS: self.param_values,
        }

    @classmethod
    def _is_valid_name(cls, param_name):
        return cls.REGEX_COMPILED.match(param_name)

    @property
    def exists(self):
        file_exists = super().exists
        params_in_file = self._parse()
        params_exists = all([p in params_in_file for p in self.param_names])
        return file_exists and params_exists

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
