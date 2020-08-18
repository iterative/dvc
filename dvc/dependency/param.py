import os
from collections import defaultdict

import dpath.util
from voluptuous import Any

from dvc.dependency.local import LocalDependency
from dvc.exceptions import DvcException
from dvc.utils.serialize import PARSERS, ParseError


class MissingParamsError(DvcException):
    pass


class BadParamFileError(DvcException):
    pass


class ParamsDependency(LocalDependency):
    PARAM_PARAMS = "params"
    PARAM_SCHEMA = {PARAM_PARAMS: Any(dict, list, None)}
    DEFAULT_PARAMS_FILE = "params.yaml"

    def __init__(self, stage, path, params):
        info = {}
        self.params = []
        if params:
            if isinstance(params, list):
                self.params = params
            else:
                assert isinstance(params, dict)
                self.params = list(params.keys())
                info = params

        super().__init__(
            stage,
            path
            or os.path.join(stage.repo.root_dir, self.DEFAULT_PARAMS_FILE),
            info=info,
        )

    def fill_values(self, values=None):
        """Load params values dynamically."""
        if not values:
            return
        for param in self.params:
            if param in values:
                self.info[param] = values[param]

    def save(self):
        super().save()
        self.info = self.save_info()

    def status(self):
        status = super().status()

        if status[str(self)] == "deleted":
            return status

        status = defaultdict(dict)
        info = self.read_params()
        for param in self.params:
            if param not in info.keys():
                st = "deleted"
            elif param not in self.info:
                st = "new"
            elif info[param] != self.info[param]:
                st = "modified"
            else:
                assert info[param] == self.info[param]
                continue

            status[str(self)][param] = st

        return status

    def dumpd(self):
        return {
            self.PARAM_PATH: self.def_path,
            self.PARAM_PARAMS: self.info or self.params,
        }

    def read_params(self):
        if not self.exists:
            return {}

        suffix = self.path_info.suffix.lower()
        parser = PARSERS[suffix]
        with self.repo.tree.open(self.path_info, "r") as fobj:
            try:
                config = parser(fobj.read(), self.path_info)
            except ParseError as exc:
                raise BadParamFileError(
                    f"Unable to read parameters from '{self}'"
                ) from exc

        ret = {}
        for param in self.params:
            try:
                ret[param] = dpath.util.get(config, param, separator=".")
            except KeyError:
                pass
        return ret

    def save_info(self):
        info = self.read_params()

        missing_params = set(self.params) - set(info.keys())
        if missing_params:
            raise MissingParamsError(
                "Parameters '{}' are missing from '{}'.".format(
                    ", ".join(missing_params), self,
                )
            )

        return info
