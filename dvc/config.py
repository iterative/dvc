"""DVC config objects."""
import logging
import os
import re
from contextlib import contextmanager
from functools import partial
from typing import TYPE_CHECKING, Dict, Optional

from funcy import compact, memoize, re_find

from dvc.exceptions import DvcException, NotDvcRepoError

from .utils.objects import cached_property

if TYPE_CHECKING:
    from dvc.fs import FileSystem
    from dvc.types import DictStrAny, StrPath

logger = logging.getLogger(__name__)


class ConfigError(DvcException):
    """DVC config exception."""

    def __init__(self, msg):
        super().__init__(f"config file error: {msg}")


class RemoteConfigError(ConfigError):
    pass


class NoRemoteError(RemoteConfigError):
    pass


class RemoteNotFoundError(RemoteConfigError):
    pass


class MachineConfigError(ConfigError):
    pass


class NoMachineError(MachineConfigError):
    pass


class MachineNotFoundError(MachineConfigError):
    pass


@memoize
def get_compiled_schema():
    from voluptuous import Schema

    from .config_schema import SCHEMA

    return Schema(SCHEMA)


def to_bool(value):
    from .config_schema import Bool

    return Bool(value)


class Config(dict):
    """Class that manages configuration files for a DVC repo.

    Args:
        dvc_dir (str): optional path to `.dvc` directory, that is used to
            access repo-specific configs like .dvc/config and
            .dvc/config.local.
        validate (bool): optional flag to tell dvc if it should validate the
            config or just load it as is. 'True' by default.

    Raises:
        ConfigError: thrown if config has an invalid format.
    """

    SYSTEM_LEVELS = ("system", "global")
    REPO_LEVELS = ("repo", "local")
    # In the order they shadow each other
    LEVELS = SYSTEM_LEVELS + REPO_LEVELS

    CONFIG = "config"
    CONFIG_LOCAL = "config.local"

    def __init__(
        self,
        dvc_dir: Optional["StrPath"] = None,
        validate: bool = True,
        fs: Optional["FileSystem"] = None,
        config: Optional["DictStrAny"] = None,
        remote: Optional[str] = None,
        remote_config: Optional["DictStrAny"] = None,
    ):  # pylint: disable=super-init-not-called
        from dvc.fs import LocalFileSystem

        self.dvc_dir = dvc_dir
        self.wfs = LocalFileSystem()
        self.fs = fs or self.wfs

        if dvc_dir:
            self.dvc_dir = self.fs.path.abspath(self.fs.path.realpath(dvc_dir))

        self.load(
            validate=validate, config=config, remote=remote, remote_config=remote_config
        )

    @classmethod
    def from_cwd(cls, fs: Optional["FileSystem"] = None, **kwargs):
        from dvc.repo import Repo

        try:
            dvc_dir = Repo.find_dvc_dir(fs=fs)
        except NotDvcRepoError:
            dvc_dir = None

        return cls(dvc_dir=dvc_dir, fs=fs, **kwargs)

    @classmethod
    def get_dir(cls, level):
        from dvc.dirs import global_config_dir, system_config_dir

        assert level in ("global", "system")

        if level == "global":
            return global_config_dir()
        if level == "system":
            return system_config_dir()

    @cached_property
    def files(self) -> Dict[str, str]:
        files = {
            level: os.path.join(self.get_dir(level), self.CONFIG)
            for level in ("system", "global")
        }

        if self.dvc_dir is not None:
            files["repo"] = self.fs.path.join(self.dvc_dir, self.CONFIG)
            files["local"] = self.fs.path.join(self.dvc_dir, self.CONFIG_LOCAL)

        return files

    @staticmethod
    def init(dvc_dir):
        """Initializes dvc config.

        Args:
            dvc_dir (str): path to .dvc directory.

        Returns:
            dvc.config.Config: config object.
        """
        config_file = os.path.join(dvc_dir, Config.CONFIG)
        with open(config_file, "w+", encoding="utf-8"):
            return Config(dvc_dir)

    def load(
        self,
        validate: bool = True,
        config: Optional["DictStrAny"] = None,
        remote: Optional[str] = None,
        remote_config: Optional["DictStrAny"] = None,
    ):
        """Loads config from all the config files.

        Raises:
            ConfigError: thrown if config has an invalid format.
        """
        conf = self.load_config_to_level()

        if config is not None:
            merge(conf, config)

        if validate:
            conf = self.validate(conf)

        self.clear()

        if remote:
            conf["core"]["remote"] = remote

        if remote_config:
            remote = remote or conf["core"].get("remote")
            if not remote:
                raise ValueError("Missing remote name")

            merge(conf, {"remote": {remote: remote_config}})

        self.update(conf)

    def _get_fs(self, level):
        # NOTE: this might be a Gitfs, which doesn't see things outside of
        # the repo.
        return self.fs if level == "repo" else self.wfs

    @staticmethod
    def load_file(path, fs=None) -> dict:
        from configobj import ConfigObj, ConfigObjError

        from dvc.fs import localfs

        fs = fs or localfs

        with fs.open(path) as fobj:
            try:
                conf_obj = ConfigObj(fobj)
            except UnicodeDecodeError as exc:
                raise ConfigError(str(exc)) from exc
            except ConfigObjError as exc:
                raise ConfigError(str(exc)) from exc

        return _parse_named(_lower_keys(conf_obj.dict()))

    def _load_config(self, level):
        filename = self.files[level]
        fs = self._get_fs(level)

        try:
            return self.load_file(filename, fs=fs)
        except FileNotFoundError:
            return {}

    def _save_config(self, level, conf_dict):
        from configobj import ConfigObj

        filename = self.files[level]
        fs = self._get_fs(level)

        logger.debug("Writing '%s'.", filename)

        fs.makedirs(os.path.dirname(filename))

        config = ConfigObj(_pack_named(conf_dict))
        with fs.open(filename, "wb") as fobj:
            config.write(fobj)
        config.filename = filename

    def load_one(self, level):
        conf = self._load_config(level)
        conf = self._load_paths(conf, self.files[level])

        # Auto-verify sections
        for key in get_compiled_schema().schema:
            conf.setdefault(key, {})

        return conf

    @staticmethod
    def _load_paths(conf, filename):
        abs_conf_dir = os.path.abspath(os.path.dirname(filename))

        def resolve(path):
            from .config_schema import RelPath

            if os.path.isabs(path) or re.match(r"\w+://", path):
                return path

            # on windows convert slashes to backslashes
            # to have path compatible with abs_conf_dir
            if os.path.sep == "\\" and "/" in path:
                path = path.replace("/", "\\")

            return RelPath(os.path.join(abs_conf_dir, path))

        return Config._map_dirs(conf, resolve)

    @staticmethod
    def _to_relpath(conf_dir, path):
        from dvc.fs import localfs
        from dvc.utils import relpath

        from .config_schema import RelPath

        if re.match(r"\w+://", path):
            return path

        if isinstance(path, RelPath) or not os.path.isabs(path):
            path = relpath(path, conf_dir)
            return localfs.path.as_posix(path)

        return path

    @staticmethod
    def _save_paths(conf, filename):
        conf_dir = os.path.dirname(filename)
        rel = partial(Config._to_relpath, conf_dir)

        return Config._map_dirs(conf, rel)

    @staticmethod
    def _map_dirs(conf, func):
        from voluptuous import ALLOW_EXTRA, Schema

        dirs_schema = {
            "cache": {"dir": func},
            "remote": {
                str: {
                    "url": func,
                    "gdrive_user_credentials_file": func,
                    "gdrive_service_account_json_file_path": func,
                    "credentialpath": func,
                    "keyfile": func,
                    "cert_path": func,
                    "key_path": func,
                }
            },
            "machine": {
                str: {
                    "startup_script": func,
                    "setup_script": func,
                }
            },
        }
        return Schema(dirs_schema, extra=ALLOW_EXTRA)(conf)

    def load_config_to_level(self, level=None):
        merged_conf: Dict = {}
        for merge_level in self.LEVELS:
            if merge_level == level:
                break
            if merge_level in self.files:
                merge(merged_conf, self.load_one(merge_level))
        return merged_conf

    def read(self, level=None):
        # NOTE: we read from a merged config by default, same as git config
        if level is None:
            return self.load_config_to_level()
        return self.load_one(level)

    @contextmanager
    def edit(self, level=None, validate=True):
        # NOTE: we write to repo config by default, same as git config
        level = level or "repo"
        if self.dvc_dir is None and level in self.REPO_LEVELS:
            raise ConfigError("Not inside a DVC repo")

        conf = self.load_one(level)
        yield conf

        conf = self._save_paths(conf, self.files[level])

        merged_conf = self.load_config_to_level(level)
        merge(merged_conf, conf)

        if validate:
            self.validate(merged_conf)

        self._save_config(level, conf)
        self.load(validate=validate)

    @staticmethod
    def validate(data):
        from voluptuous import Invalid

        try:
            return get_compiled_schema()(data)
        except Invalid as exc:
            raise ConfigError(str(exc)) from None


def _parse_named(conf):
    result: Dict[str, Dict] = {"remote": {}, "machine": {}}

    for section, val in conf.items():
        match = re_find(r'^\s*(remote|machine)\s*"(.*)"\s*$', section)
        if match:
            key, name = match
            result[key][name] = val
        else:
            result[section] = val

    return result


def _pack_named(conf):
    # Drop empty sections
    result = compact(conf)

    # Transform remote.name -> 'remote "name"'
    for key in ("remote", "machine"):
        for name, val in conf[key].items():
            result[f'{key} "{name}"'] = val
        result.pop(key, None)

    return result


def merge(into, update):
    """Merges second dict into first recursively"""
    for key, val in update.items():
        if isinstance(into.get(key), dict) and isinstance(val, dict):
            merge(into[key], val)
        else:
            into[key] = val


def _lower_keys(data):
    return {
        k.lower(): _lower_keys(v) if isinstance(v, dict) else v for k, v in data.items()
    }
