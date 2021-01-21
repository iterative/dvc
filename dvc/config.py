"""DVC config objects."""
import logging
import os
import re
from contextlib import contextmanager
from functools import partial
from urllib.parse import urlparse

import configobj
from funcy import cached_property, compact, re_find, walk_values
from voluptuous import (
    ALLOW_EXTRA,
    All,
    Any,
    Coerce,
    Invalid,
    Lower,
    Optional,
    Range,
    Schema,
)

from dvc.exceptions import DvcException, NotDvcRepoError
from dvc.path_info import PathInfo
from dvc.utils import relpath

logger = logging.getLogger(__name__)


class ConfigError(DvcException):
    """DVC config exception."""

    def __init__(self, msg):
        super().__init__(f"config file error: {msg}")


class NoRemoteError(ConfigError):
    pass


def supported_cache_type(types):
    """Checks if link type config option consists only of valid values.

    Args:
        types (list/string): type(s) of links that dvc should try out.
    """
    if types is None:
        return None
    if isinstance(types, str):
        types = [typ.strip() for typ in types.split(",")]

    unsupported = set(types) - {"reflink", "hardlink", "symlink", "copy"}
    if unsupported:
        raise Invalid(
            "Unsupported cache type(s): {}".format(", ".join(unsupported))
        )

    return types


# Checks that value is either true or false and converts it to bool
to_bool = Bool = All(
    Lower,
    Any("true", "false"),
    lambda v: v == "true",
    msg="expected true or false",
)


def Choices(*choices):
    """Checks that value belongs to the specified set of values

    Args:
        *choices: pass allowed values as arguments, or pass a list or
            tuple as a single argument
    """
    return Any(*choices, msg="expected one of {}".format(", ".join(choices)))


def ByUrl(mapping):
    schemas = walk_values(Schema, mapping)

    def validate(data):
        if "url" not in data:
            raise Invalid("expected 'url'")

        parsed = urlparse(data["url"])
        # Windows absolute paths should really have scheme == "" (local)
        if os.name == "nt" and len(parsed.scheme) == 1 and parsed.netloc == "":
            return schemas[""](data)
        if parsed.scheme not in schemas:
            raise Invalid(f"Unsupported URL type {parsed.scheme}://")

        return schemas[parsed.scheme](data)

    return validate


class RelPath(str):
    pass


REMOTE_COMMON = {
    "url": str,
    "checksum_jobs": All(Coerce(int), Range(1)),
    "jobs": All(Coerce(int), Range(1)),
    Optional("no_traverse"): Bool,  # obsoleted
    "verify": Bool,
}
LOCAL_COMMON = {
    "type": supported_cache_type,
    Optional("protected", default=False): Bool,  # obsoleted
    "shared": All(Lower, Choices("group")),
    Optional("slow_link_warning", default=True): Bool,
}
HTTP_COMMON = {
    "auth": All(Lower, Choices("basic", "digest", "custom")),
    "custom_auth_header": str,
    "user": str,
    "password": str,
    "ask_password": Bool,
    "ssl_verify": Bool,
    "method": str,
}
WEBDAV_COMMON = {
    "user": str,
    "password": str,
    "ask_password": Bool,
    "token": str,
    "cert_path": str,
    "key_path": str,
    "timeout": Coerce(int),
}

SCHEMA = {
    "core": {
        "remote": Lower,
        "checksum_jobs": All(Coerce(int), Range(1)),
        Optional("interactive", default=False): Bool,
        Optional("analytics", default=True): Bool,
        Optional("hardlink_lock", default=False): Bool,
        Optional("no_scm", default=False): Bool,
        Optional("autostage", default=False): Bool,
        Optional("experiments", default=False): Bool,
        Optional("check_update", default=True): Bool,
    },
    "cache": {
        "local": str,
        "s3": str,
        "gs": str,
        "hdfs": str,
        "webhdfs": str,
        "ssh": str,
        "azure": str,
        # This is for default local cache
        "dir": str,
        **LOCAL_COMMON,
    },
    "remote": {
        str: ByUrl(
            {
                "": {**LOCAL_COMMON, **REMOTE_COMMON},
                "s3": {
                    "region": str,
                    "profile": str,
                    "credentialpath": str,
                    "endpointurl": str,
                    "access_key_id": str,
                    "secret_access_key": str,
                    "session_token": str,
                    Optional("listobjects", default=False): Bool,  # obsoleted
                    Optional("use_ssl", default=True): Bool,
                    "sse": str,
                    "sse_kms_key_id": str,
                    "acl": str,
                    "grant_read": str,
                    "grant_read_acp": str,
                    "grant_write_acp": str,
                    "grant_full_control": str,
                    **REMOTE_COMMON,
                },
                "gs": {
                    "projectname": str,
                    "credentialpath": str,
                    **REMOTE_COMMON,
                },
                "ssh": {
                    "type": supported_cache_type,
                    "port": Coerce(int),
                    "user": str,
                    "password": str,
                    "ask_password": Bool,
                    "keyfile": str,
                    "timeout": Coerce(int),
                    "gss_auth": Bool,
                    "allow_agent": Bool,
                    **REMOTE_COMMON,
                },
                "hdfs": {"user": str, **REMOTE_COMMON},
                "webhdfs": {
                    "hdfscli_config": str,
                    "webhdfs_token": str,
                    "user": str,
                    "webhdfs_alias": str,
                    **REMOTE_COMMON,
                },
                "azure": {"connection_string": str, **REMOTE_COMMON},
                "oss": {
                    "oss_key_id": str,
                    "oss_key_secret": str,
                    "oss_endpoint": str,
                    **REMOTE_COMMON,
                },
                "gdrive": {
                    "gdrive_use_service_account": Bool,
                    "gdrive_client_id": str,
                    "gdrive_client_secret": str,
                    "gdrive_user_credentials_file": str,
                    "gdrive_service_account_email": str,
                    "gdrive_service_account_user_email": str,
                    "gdrive_service_account_p12_file_path": str,
                    Optional("gdrive_trash_only", default=False): Bool,
                    **REMOTE_COMMON,
                },
                "http": {**HTTP_COMMON, **REMOTE_COMMON},
                "https": {**HTTP_COMMON, **REMOTE_COMMON},
                "webdav": {**WEBDAV_COMMON, **REMOTE_COMMON},
                "webdavs": {**WEBDAV_COMMON, **REMOTE_COMMON},
                "remote": {str: object},  # Any of the above options are valid
            }
        )
    },
    "state": {
        "row_limit": All(Coerce(int), Range(1)),
        "row_cleanup_quota": All(Coerce(int), Range(0, 100)),
    },
    # section for experimental features
    "feature": {Optional("parametrization", default=False): Bool},
}
COMPILED_SCHEMA = Schema(SCHEMA)


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

    APPNAME = "dvc"
    APPAUTHOR = "iterative"

    # In the order they shadow each other
    LEVELS = ("system", "global", "repo", "local")

    CONFIG = "config"
    CONFIG_LOCAL = "config.local"

    def __init__(
        self, dvc_dir=None, validate=True, tree=None,
    ):  # pylint: disable=super-init-not-called
        from dvc.tree.local import LocalTree

        self.dvc_dir = dvc_dir

        if not dvc_dir:
            try:
                from dvc.repo import Repo

                self.dvc_dir = os.path.join(Repo.find_dvc_dir())
            except NotDvcRepoError:
                self.dvc_dir = None
        else:
            self.dvc_dir = os.path.abspath(os.path.realpath(dvc_dir))

        self.wtree = LocalTree(None, {"url": self.dvc_dir})
        self.tree = tree or self.wtree

        self.load(validate=validate)

    @classmethod
    def get_dir(cls, level):
        from appdirs import site_config_dir, user_config_dir

        assert level in ("global", "system")

        if level == "global":
            return user_config_dir(cls.APPNAME, cls.APPAUTHOR)
        if level == "system":
            return site_config_dir(cls.APPNAME, cls.APPAUTHOR)

    @cached_property
    def files(self):
        files = {
            level: os.path.join(self.get_dir(level), self.CONFIG)
            for level in ("system", "global")
        }

        if self.dvc_dir is not None:
            files["repo"] = os.path.join(self.dvc_dir, self.CONFIG)
            files["local"] = os.path.join(self.dvc_dir, self.CONFIG_LOCAL)

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
        open(config_file, "w+").close()
        return Config(dvc_dir)

    def load(self, validate=True):
        """Loads config from all the config files.

        Raises:
            ConfigError: thrown if config has an invalid format.
        """
        conf = self.load_config_to_level()

        if validate:
            conf = self.validate(conf)

        self.clear()
        self.update(conf)

        # Add resolved default cache.dir
        if not self["cache"].get("dir") and self.dvc_dir:
            self["cache"]["dir"] = os.path.join(self.dvc_dir, "cache")

    def _get_tree(self, level):
        # NOTE: this might be a GitTree, which doesn't see things outside of
        # the repo.
        return self.tree if level == "repo" else self.wtree

    def _load_config(self, level):
        filename = self.files[level]
        tree = self._get_tree(level)

        if tree.exists(filename, use_dvcignore=False):
            with tree.open(filename) as fobj:
                conf_obj = configobj.ConfigObj(fobj)
        else:
            conf_obj = configobj.ConfigObj()
        return _parse_remotes(_lower_keys(conf_obj.dict()))

    def _save_config(self, level, conf_dict):
        filename = self.files[level]
        tree = self._get_tree(level)

        logger.debug(f"Writing '{filename}'.")

        tree.makedirs(os.path.dirname(filename))

        config = configobj.ConfigObj(_pack_remotes(conf_dict))
        with tree.open(filename, "wb") as fobj:
            config.write(fobj)
        config.filename = filename

    def load_one(self, level):
        conf = self._load_config(level)
        conf = self._load_paths(conf, self.files[level])

        # Auto-verify sections
        for key in COMPILED_SCHEMA.schema:
            conf.setdefault(key, {})

        return conf

    @staticmethod
    def _load_paths(conf, filename):
        abs_conf_dir = os.path.abspath(os.path.dirname(filename))

        def resolve(path):
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
        if re.match(r"\w+://", path):
            return path

        if isinstance(path, RelPath) or not os.path.isabs(path):
            path = relpath(path, conf_dir)

        return PathInfo(path).as_posix()

    @staticmethod
    def _save_paths(conf, filename):
        conf_dir = os.path.dirname(filename)
        rel = partial(Config._to_relpath, conf_dir)

        return Config._map_dirs(conf, rel)

    @staticmethod
    def _map_dirs(conf, func):
        dirs_schema = {
            "cache": {"dir": func},
            "remote": {
                str: {
                    "url": func,
                    "gdrive_user_credentials_file": func,
                    "gdrive_service_account_p12_file_path": func,
                    "credentialpath": func,
                    "keyfile": func,
                    "cert_path": func,
                    "key_path": func,
                }
            },
        }
        return Schema(dirs_schema, extra=ALLOW_EXTRA)(conf)

    def load_config_to_level(self, level=None):
        merged_conf = {}
        for merge_level in self.LEVELS:
            if merge_level == level:
                break
            if merge_level in self.files:
                merge(merged_conf, self.load_one(merge_level))
        return merged_conf

    @contextmanager
    def edit(self, level="repo"):
        if level in {"repo", "local"} and self.dvc_dir is None:
            raise ConfigError("Not inside a DVC repo")

        conf = self.load_one(level)
        yield conf

        conf = self._save_paths(conf, self.files[level])

        merged_conf = self.load_config_to_level(level)
        merge(merged_conf, conf)
        self.validate(merged_conf)

        self._save_config(level, conf)
        self.load()

    @staticmethod
    def validate(data):
        try:
            return COMPILED_SCHEMA(data)
        except Invalid as exc:
            raise ConfigError(str(exc)) from None


def _parse_remotes(conf):
    result = {"remote": {}}

    for section, val in conf.items():
        name = re_find(r'^\s*remote\s*"(.*)"\s*$', section)
        if name:
            result["remote"][name] = val
        else:
            result[section] = val

    return result


def _pack_remotes(conf):
    # Drop empty sections
    result = compact(conf)

    # Transform remote.name -> 'remote "name"'
    for name, val in conf["remote"].items():
        result[f'remote "{name}"'] = val
    result.pop("remote", None)

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
        k.lower(): _lower_keys(v) if isinstance(v, dict) else v
        for k, v in data.items()
    }
