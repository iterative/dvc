import logging
import os
import typing
from collections import defaultdict
from typing import Dict

import dpath.util
from voluptuous import Any

from dvc.exceptions import DvcException
from dvc.utils.serialize import ParseError, load_path
from dvc_data.hashfile.hash_info import HashInfo

from .base import Dependency

logger = logging.getLogger(__name__)


class MissingParamsError(DvcException):
    pass


class MissingParamsFile(DvcException):
    pass


class ParamsIsADirectoryError(DvcException):
    pass


class BadParamFileError(DvcException):
    pass


class ParamsDependency(Dependency):
    PARAM_PARAMS = "params"
    PARAM_SCHEMA = {PARAM_PARAMS: Any(dict, list, None)}
    DEFAULT_PARAMS_FILE = "params.yaml"

    def __init__(self, stage, path, params=None, repo=None):
        self.params = list(params) if params else []
        hash_info = HashInfo()
        if isinstance(params, dict):
            hash_info = HashInfo(self.PARAM_PARAMS, params)
        repo = repo or stage.repo
        path = path or os.path.join(repo.root_dir, self.DEFAULT_PARAMS_FILE)
        super().__init__(stage, path, repo=repo)
        self.hash_info = hash_info

    def dumpd(self, **kwargs):
        ret = super().dumpd()
        if not self.hash_info:
            ret[self.PARAM_PARAMS] = self.params or {}
        return ret

    def fill_values(self, values=None):
        """Load params values dynamically."""
        if values is None:
            return

        info = {}
        if not self.params:
            info.update(values)
        for param in self.params:
            if param in values:
                info[param] = values[param]
        self.hash_info = HashInfo(self.PARAM_PARAMS, info)

    def read_params(
        self, flatten: bool = True, **kwargs: typing.Any
    ) -> Dict[str, typing.Any]:
        try:
            config = self.read_file()
        except MissingParamsFile:
            config = {}

        if not self.params:
            return config

        ret = {}
        if flatten:
            for param in self.params:
                try:
                    ret[param] = dpath.util.get(config, param, separator=".")
                except KeyError:
                    continue
            return ret

        from dpath.util import merge

        for param in self.params:
            merge(
                ret,
                dpath.util.search(config, param, separator="."),
                separator=".",
            )
        return ret

    def workspace_status(self):
        if not self.exists:
            return {str(self): "deleted"}
        if self.hash_info.value is None:
            return {str(self): "new"}

        from funcy import ldistinct

        status = defaultdict(dict)
        info = self.hash_info.value if self.hash_info else {}
        actual = self.read_params()

        # NOTE: we want to preserve the order of params as specified in the
        # status. In case of tracking the whole file, the order is top-level
        # keys in the file and then the keys in the `info` from `dvc.lock`
        # (which are alphabetically sorted).
        params = self.params or ldistinct([*actual.keys(), *info.keys()])
        for param in params:
            if param not in actual:
                st = "deleted"
            elif param not in info:
                st = "new"
            elif actual[param] != info[param]:
                st = "modified"
            else:
                assert actual[param] == info[param]
                continue

            status[str(self)][param] = st

        return status

    def status(self):
        return self.workspace_status()

    def validate_filepath(self):
        if not self.exists:
            raise MissingParamsFile(f"Parameters file '{self}' does not exist")
        if self.isdir():
            raise ParamsIsADirectoryError(
                f"'{self}' is a directory, expected a parameters file"
            )

    def read_file(self):
        self.validate_filepath()
        try:
            return load_path(self.fs_path, self.repo.fs)
        except ParseError as exc:
            raise BadParamFileError(
                f"Unable to read parameters from '{self}'"
            ) from exc

    def get_hash(self):
        info = self.read_params()

        missing_params = set(self.params) - set(info.keys())
        if missing_params:
            raise MissingParamsError(
                "Parameters '{}' are missing from '{}'.".format(
                    ", ".join(missing_params), self
                )
            )

        return HashInfo(self.PARAM_PARAMS, info)

    def save(self):
        if not self.exists:
            raise self.DoesNotExistError(self)

        if not self.isfile and not self.isdir:
            raise self.IsNotFileOrDirError(self)

        self.ignore()
        self.hash_info = self.get_hash()
