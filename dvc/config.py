"""DVC config objects."""
from contextlib import contextmanager
import logging
import os
import re
from urllib.parse import urlparse

from funcy import cached_property, re_find, walk_values, compact
import configobj
from voluptuous import Schema, Optional, Invalid, ALLOW_EXTRA
from voluptuous import All, Any, Lower, Range, Coerce

from dvc.exceptions import DvcException, NotDvcRepoError
from dvc.utils import relpath

logger = logging.getLogger(__name__)


class ConfigError(DvcException):
    """DVC config exception."""

    def __init__(self, msg):
        super().__init__("config file error: {}".format(msg))


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
            raise Invalid("Unsupported URL type {}://".format(parsed.scheme))

        return schemas[parsed.scheme](data)

    return validate


class RelPath(str):
    pass


REMOTE_COMMON = {
    "url": str,
    "checksum_jobs": All(Coerce(int), Range(1)),
    "no_traverse": Bool,
    "verify": Bool,
}
LOCAL_COMMON = {
    "type": supported_cache_type,
    Optional("protected", default=False): Bool,
    "shared": All(Lower, Choices("group")),
    Optional("slow_link_warning", default=True): Bool,
}
HTTP_COMMON = {
    "auth": All(Lower, Choices("basic", "digest", "custom")),
    "custom_auth_header": str,
    "user": str,
    "password": str,
    "ask_password": Bool,
}
SCHEMA = {
    "core": {
        "remote": Lower,
        "checksum_jobs": All(Coerce(int), Range(1)),
        Optional("interactive", default=False): Bool,
        Optional("analytics", default=True): Bool,
        Optional("hardlink_lock", default=False): Bool,
        Optional("no_scm", default=False): Bool,
    },
    "cache": {
        "local": str,
        "s3": str,
        "gs": str,
        "hdfs": str,
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
                    Optional("listobjects", default=False): Bool,
                    Optional("use_ssl", default=True): Bool,
                    "sse": str,
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
                    **REMOTE_COMMON,
                },
                "hdfs": {"user": str, **REMOTE_COMMON},
                "azure": {"connection_string": str, **REMOTE_COMMON},
                "oss": {
                    "oss_key_id": str,
                    "oss_key_secret": str,
                    "oss_endpoint": str,
                    **REMOTE_COMMON,
                },
                "gdrive": {
                    "gdrive_client_id": str,
                    "gdrive_client_secret": str,
                    "gdrive_user_credentials_file": str,
                    **REMOTE_COMMON,
                },
                "http": {**HTTP_COMMON, **REMOTE_COMMON},
                "https": {**HTTP_COMMON, **REMOTE_COMMON},
                "remote": {str: object},  # Any of the above options are valid
            }
        )
    },
    "state": {
        "row_limit": All(Coerce(int), Range(1)),
        "row_cleanup_quota": All(Coerce(int), Range(0, 100)),
    },
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
        ConfigError: thrown when config has an invalid format.
    """

    APPNAME = "dvc"
    APPAUTHOR = "iterative"

    # In the order they shadow each other
    LEVELS = ("system", "global", "repo", "local")

    CONFIG = "config"
    CONFIG_LOCAL = "config.local"

    def __init__(self, dvc_dir=None, validate=True):
        self.dvc_dir = dvc_dir

        if not dvc_dir:
            try:
                from dvc.repo import Repo

                self.dvc_dir = os.path.join(Repo.find_dvc_dir())
            except NotDvcRepoError:
                self.dvc_dir = None
        else:
            self.dvc_dir = os.path.abspath(os.path.realpath(dvc_dir))

        self.load(validate=validate)

    @classmethod
    def get_dir(cls, level):
        from appdirs import user_config_dir, site_config_dir

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
            dvc.config.ConfigError: thrown if config has invalid format.
        """
        conf = {}
        for level in self.LEVELS:
            if level in self.files:
                _merge(conf, self.load_one(level))

        if validate:
            conf = self.validate(conf)

        self.clear()
        self.update(conf)

        # Add resolved default cache.dir
        if not self["cache"].get("dir") and self.dvc_dir:
            self["cache"]["dir"] = os.path.join(self.dvc_dir, "cache")

    def load_one(self, level):
        conf = _load_config(self.files[level])
        conf = self._load_paths(conf, self.files[level])

        # Autovivify sections
        for key in COMPILED_SCHEMA.schema:
            conf.setdefault(key, {})

        return conf

    @staticmethod
    def _load_paths(conf, filename):
        abs_conf_dir = os.path.abspath(os.path.dirname(filename))

        def resolve(path):
            if os.path.isabs(path) or re.match(r"\w+://", path):
                return path
            return RelPath(os.path.join(abs_conf_dir, path))

        return Config._map_dirs(conf, resolve)

    @staticmethod
    def _save_paths(conf, filename):
        conf_dir = os.path.dirname(filename)

        def rel(path):
            if re.match(r"\w+://", path):
                return path

            if isinstance(path, RelPath) or not os.path.isabs(path):
                return relpath(path, conf_dir)
            return path

        return Config._map_dirs(conf, rel)

    @staticmethod
    def _map_dirs(conf, func):
        dirs_schema = {"cache": {"dir": func}, "remote": {str: {"url": func}}}
        return Schema(dirs_schema, extra=ALLOW_EXTRA)(conf)

    @contextmanager
    def edit(self, level="repo"):
        if level in {"repo", "local"} and self.dvc_dir is None:
            raise ConfigError("Not inside a dvc repo")

        conf = self.load_one(level)
        yield conf

        conf = self._save_paths(conf, self.files[level])
        _save_config(self.files[level], conf)
        self.load()

    @staticmethod
    def validate(data):
        try:
            return COMPILED_SCHEMA(data)
        except Invalid as exc:
            raise ConfigError(str(exc)) from None


def _load_config(filename):
    conf_obj = configobj.ConfigObj(filename)
    return _parse_remotes(_lower_keys(conf_obj.dict()))


def _save_config(filename, conf_dict):
    logger.debug("Writing '{}'.".format(filename))
    os.makedirs(os.path.dirname(filename), exist_ok=True)

    config = configobj.ConfigObj(_pack_remotes(conf_dict))
    config.filename = filename
    config.write()


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
        result['remote "{}"'.format(name)] = val
    result.pop("remote", None)

    return result


def _merge(into, update):
    """Merges second dict into first recursively"""
    for key, val in update.items():
        if isinstance(into.get(key), dict) and isinstance(val, dict):
            _merge(into[key], val)
        else:
            into[key] = val


def _lower_keys(data):
    return {
        k.lower(): _lower_keys(v) if isinstance(v, dict) else v
        for k, v in data.items()
    }
