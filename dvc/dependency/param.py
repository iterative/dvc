import json
import re
from itertools import groupby

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
    FILE_DELIMITER = ":"
    PARAM_DELIMITER = ","
    DEFAULT_PARAMS_FILE = "params.json"

    REGEX_SUBNAME = r"\w+"
    REGEX_NAME = r"{sub}(\.{sub})*".format(sub=REGEX_SUBNAME)
    REGEX_MULTI_PARAMS = r"^{param}(,{param})*$".format(param=REGEX_NAME)
    REGEX_COMPILED = re.compile(REGEX_MULTI_PARAMS)

    def __init__(self, stage, input_str, *args, **kwargs):
        path, param_names = self._parse_and_validate_input(input_str)
        super().__init__(stage, path, *args, **kwargs)
        self.param_names = sorted(param_names.split(self.PARAM_DELIMITER))
        self.param_values = {}

    def __str__(self):
        path = super().__str__()
        return self._reverse_parse_input(path, self.param_names)

    @classmethod
    def from_list(cls, stage, s_list):
        # Creates an object for each unique file that is referenced in the list
        ret = []
        pathname_tuples = [cls._parse_and_validate_input(s) for s in s_list]
        grouped_by_path = groupby(sorted(pathname_tuples), key=lambda x: x[0])
        for path, group in grouped_by_path:
            param_names = [g[1] for g in group]
            regrouped_input = cls._reverse_parse_input(path, param_names)
            ret.append(DependencyPARAMS(stage, regrouped_input))
        return ret

    @classmethod
    def _parse_and_validate_input(cls, input_str):
        path, _, param_names = input_str.rpartition(cls.FILE_DELIMITER)
        cls._validate_input(param_names)
        path = path or cls.DEFAULT_PARAMS_FILE
        return path, param_names

    @classmethod
    def _reverse_parse_input(cls, path, param_names):
        return "{path}{delimiter}{params}".format(
            path=path,
            delimiter=cls.FILE_DELIMITER,
            params=cls.PARAM_DELIMITER.join(param_names),
        )

    @classmethod
    def _validate_input(cls, param_names):
        if not cls.REGEX_COMPILED.match(param_names):
            raise BadParamNameError(param_names)

    def save(self):
        super().save()
        params_in_file = self._parse_file()
        self.param_values = {k: params_in_file[k] for k in self.param_names}

    def dumpd(self):
        return {
            self.PARAM_PATH: self.def_path,
            self.PARAM_PARAMS: self.param_values,
        }

    @property
    def exists(self):
        file_exists = super().exists
        params_in_file = self._parse_file()
        params_exists = all([p in params_in_file for p in self.param_names])
        return file_exists and params_exists

    def _parse_file(self):
        try:
            return self._params_cache
        except AttributeError:
            path = self.path_info.fspath
            with open(path, "r") as fp:
                try:
                    self._params_cache = json.load(fp)
                except json.JSONDecodeError:
                    raise BadParamFileError(path)
            return self._params_cache
