import os
import typing
from collections import defaultdict
from typing import TYPE_CHECKING, Any, Optional

import dpath

from dvc.exceptions import DvcException
from dvc.log import logger
from dvc.utils.serialize import ParseError, load_path
from dvc_data.hashfile.hash_info import HashInfo

from .base import Dependency

if TYPE_CHECKING:
    from dvc.fs import FileSystem

logger = logger.getChild(__name__)


class MissingParamsError(DvcException):
    pass


class MissingParamsFile(DvcException):
    pass


class ParamsIsADirectoryError(DvcException):
    pass


class BadParamFileError(DvcException):
    pass


def read_param_file(
    fs: "FileSystem",
    path: str,
    key_paths: Optional[list[str]] = None,
    flatten: bool = False,
    **load_kwargs,
) -> Any:
    config = load_path(path, fs, **load_kwargs)
    if not key_paths:
        return config

    ret = {}
    if flatten:
        for key_path in key_paths:
            try:
                ret[key_path] = dpath.get(config, key_path, separator=".")
            except KeyError:
                continue
        return ret

    from copy import deepcopy

    from dpath import merge
    from funcy import distinct

    for key_path in distinct(key_paths):
        merge(
            ret,
            deepcopy(dpath.search(config, key_path, separator=".")),
            separator=".",
        )
    return ret


class ParamsDependency(Dependency):
    PARAM_PARAMS = "params"
    DEFAULT_PARAMS_FILE = "params.yaml"

    def __init__(self, stage, path, params=None, repo=None):
        self.params = list(params) if params else []
        hash_info = HashInfo()
        if isinstance(params, dict):
            hash_info = HashInfo(self.PARAM_PARAMS, params)  # type: ignore[arg-type]
        repo = repo or stage.repo
        path = path or os.path.join(repo.root_dir, self.DEFAULT_PARAMS_FILE)
        super().__init__(stage, path, repo=repo)
        self.hash_name = self.PARAM_PARAMS
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
        self.hash_info = HashInfo(self.PARAM_PARAMS, info)  # type: ignore[arg-type]

    def read_params(
        self, flatten: bool = True, **kwargs: typing.Any
    ) -> dict[str, typing.Any]:
        try:
            self.validate_filepath()
        except MissingParamsFile:
            return {}

        try:
            return read_param_file(
                self.repo.fs,
                self.fs_path,
                list(self.params) if self.params else None,
                flatten=flatten,
            )
        except ParseError as exc:
            raise BadParamFileError(f"Unable to read parameters from '{self}'") from exc

    def workspace_status(self):
        if not self.exists:
            return {str(self): "deleted"}
        if self.hash_info.value is None:
            return {str(self): "new"}

        from funcy import ldistinct

        status: dict[str, Any] = defaultdict(dict)
        info = self.hash_info.value if self.hash_info else {}
        assert isinstance(info, dict)
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
                if (
                    isinstance(actual[param], tuple)
                    and list(actual[param]) == info[param]
                ):
                    continue
                st = "modified"
            else:
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

    def get_hash(self):
        info = self.read_params()

        missing_params = set(self.params) - set(info.keys())
        if missing_params:
            raise MissingParamsError(
                "Parameters '{}' are missing from '{}'.".format(
                    ", ".join(missing_params), self
                )
            )

        return HashInfo(self.PARAM_PARAMS, info)  # type: ignore[arg-type]

    def save(self):
        if not self.exists:
            raise self.DoesNotExistError(self)

        if not self.isfile() and not self.isdir():
            raise self.IsNotFileOrDirError(self)

        self.ignore()
        self.hash_info = self.get_hash()
