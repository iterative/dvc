"""DVC config objects."""

from __future__ import unicode_literals

from dvc.utils.compat import str, open

import os
import re
import copy
import errno
import configobj
import logging

from schema import Schema, Optional, And, Use, Regex, SchemaError
from dvc.exceptions import DvcException, NotDvcRepoError

logger = logging.getLogger(__name__)


class ConfigError(DvcException):
    """DVC config exception.

    Args:
        msg (str): error message.
        cause (Exception): optional exception that has caused this error.
    """

    def __init__(self, msg, cause=None):
        super(ConfigError, self).__init__(
            "config file error: {}".format(msg), cause=cause
        )


class NoRemoteError(ConfigError):
    def __init__(self, command, cause=None):
        msg = (
            "no remote specified. Setup default remote with\n"
            "    dvc config core.remote <name>\n"
            "or use:\n"
            "    dvc {} -r <name>\n".format(command)
        )

        super(NoRemoteError, self).__init__(msg, cause=cause)


def supported_cache_type(types):
    """Checks if link type config option has a valid value.

    Args:
        types (list/string): type(s) of links that dvc should try out.
    """
    if isinstance(types, str):
        types = [typ.strip() for typ in types.split(",")]
    for typ in types:
        if typ not in ["reflink", "hardlink", "symlink", "copy"]:
            return False
    return True


def is_bool(val):
    """Checks that value is a boolean.

    Args:
        val (str): string value verify.

    Returns:
        bool: True if value stands for boolean, False otherwise.
    """
    return val.lower() in ["true", "false"]


def to_bool(val):
    """Converts value to boolean.

    Args:
        val (str): string to convert to boolean.

    Returns:
        bool: True if value.lower() == 'true', False otherwise.
    """
    return val.lower() == "true"


def is_whole(val):
    """Checks that value is a whole integer.

    Args:
        val (str): number string to verify.

    Returns:
        bool: True if val is a whole number, False otherwise.
    """
    return int(val) >= 0


def is_percent(val):
    """Checks that value is a percent.

    Args:
        val (str): number string to verify.

    Returns:
        bool: True if 0<=value<=100, False otherwise.
    """
    return int(val) >= 0 and int(val) <= 100


class Choices(object):
    """Checks that value belongs to the specified set of values

    Args:
        *choices: pass allowed values as arguments, or pass a list or
            tuple as a single argument
    """

    def __init__(self, *choices):
        if len(choices) == 1 and isinstance(choices[0], (list, tuple)):
            choices = choices[0]
        self.choices = choices

    def __call__(self, value):
        return value in self.choices


class Config(object):  # pylint: disable=too-many-instance-attributes
    """Class that manages configuration files for a dvc repo.

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

    # NOTE: used internally in RemoteLOCAL to know config
    # location, that url should resolved relative to.
    PRIVATE_CWD = "_cwd"

    CONFIG = "config"
    CONFIG_LOCAL = "config.local"

    LEVEL_LOCAL = 0
    LEVEL_REPO = 1
    LEVEL_GLOBAL = 2
    LEVEL_SYSTEM = 3

    BOOL_SCHEMA = And(str, is_bool, Use(to_bool))

    SECTION_CORE = "core"
    SECTION_CORE_LOGLEVEL = "loglevel"
    SECTION_CORE_LOGLEVEL_SCHEMA = And(
        Use(str.lower), Choices("info", "debug", "warning", "error")
    )
    SECTION_CORE_REMOTE = "remote"
    SECTION_CORE_INTERACTIVE_SCHEMA = BOOL_SCHEMA
    SECTION_CORE_INTERACTIVE = "interactive"
    SECTION_CORE_ANALYTICS = "analytics"
    SECTION_CORE_ANALYTICS_SCHEMA = BOOL_SCHEMA
    SECTION_CORE_CHECKSUM_JOBS = "checksum_jobs"
    SECTION_CORE_CHECKSUM_JOBS_SCHEMA = And(Use(int), lambda x: x > 0)

    SECTION_CACHE = "cache"
    SECTION_CACHE_DIR = "dir"
    SECTION_CACHE_TYPE = "type"
    SECTION_CACHE_TYPE_SCHEMA = supported_cache_type
    SECTION_CACHE_PROTECTED = "protected"
    SECTION_CACHE_SHARED = "shared"
    SECTION_CACHE_SHARED_SCHEMA = And(Use(str.lower), Choices("group"))
    SECTION_CACHE_LOCAL = "local"
    SECTION_CACHE_S3 = "s3"
    SECTION_CACHE_GS = "gs"
    SECTION_CACHE_SSH = "ssh"
    SECTION_CACHE_HDFS = "hdfs"
    SECTION_CACHE_AZURE = "azure"
    SECTION_CACHE_SLOW_LINK_WARNING = "slow_link_warning"
    SECTION_CACHE_SCHEMA = {
        Optional(SECTION_CACHE_LOCAL): str,
        Optional(SECTION_CACHE_S3): str,
        Optional(SECTION_CACHE_GS): str,
        Optional(SECTION_CACHE_HDFS): str,
        Optional(SECTION_CACHE_SSH): str,
        Optional(SECTION_CACHE_AZURE): str,
        Optional(SECTION_CACHE_DIR): str,
        Optional(SECTION_CACHE_TYPE, default=None): SECTION_CACHE_TYPE_SCHEMA,
        Optional(SECTION_CACHE_PROTECTED, default=False): BOOL_SCHEMA,
        Optional(SECTION_CACHE_SHARED): SECTION_CACHE_SHARED_SCHEMA,
        Optional(PRIVATE_CWD): str,
        Optional(SECTION_CACHE_SLOW_LINK_WARNING, default=True): BOOL_SCHEMA,
    }

    SECTION_CORE_SCHEMA = {
        Optional(SECTION_CORE_LOGLEVEL): And(
            str, Use(str.lower), SECTION_CORE_LOGLEVEL_SCHEMA
        ),
        Optional(SECTION_CORE_REMOTE, default=""): And(str, Use(str.lower)),
        Optional(
            SECTION_CORE_INTERACTIVE, default=False
        ): SECTION_CORE_INTERACTIVE_SCHEMA,
        Optional(
            SECTION_CORE_ANALYTICS, default=True
        ): SECTION_CORE_ANALYTICS_SCHEMA,
        Optional(
            SECTION_CORE_CHECKSUM_JOBS, default=None
        ): SECTION_CORE_CHECKSUM_JOBS_SCHEMA,
    }

    # backward compatibility
    SECTION_AWS = "aws"
    SECTION_AWS_STORAGEPATH = "storagepath"
    SECTION_AWS_CREDENTIALPATH = "credentialpath"
    SECTION_AWS_ENDPOINT_URL = "endpointurl"
    SECTION_AWS_LIST_OBJECTS = "listobjects"
    SECTION_AWS_REGION = "region"
    SECTION_AWS_PROFILE = "profile"
    SECTION_AWS_USE_SSL = "use_ssl"
    SECTION_AWS_SSE = "sse"
    SECTION_AWS_ACL = "acl"
    SECTION_AWS_SCHEMA = {
        SECTION_AWS_STORAGEPATH: str,
        Optional(SECTION_AWS_REGION): str,
        Optional(SECTION_AWS_PROFILE): str,
        Optional(SECTION_AWS_CREDENTIALPATH): str,
        Optional(SECTION_AWS_ENDPOINT_URL): str,
        Optional(SECTION_AWS_LIST_OBJECTS, default=False): BOOL_SCHEMA,
        Optional(SECTION_AWS_USE_SSL, default=True): BOOL_SCHEMA,
        Optional(SECTION_AWS_SSE): str,
        Optional(SECTION_AWS_ACL): str,
    }

    # backward compatibility
    SECTION_GCP = "gcp"
    SECTION_GCP_STORAGEPATH = SECTION_AWS_STORAGEPATH
    SECTION_GCP_CREDENTIALPATH = SECTION_AWS_CREDENTIALPATH
    SECTION_GCP_PROJECTNAME = "projectname"
    SECTION_GCP_SCHEMA = {
        SECTION_GCP_STORAGEPATH: str,
        Optional(SECTION_GCP_PROJECTNAME): str,
    }

    # backward compatibility
    SECTION_LOCAL = "local"
    SECTION_LOCAL_STORAGEPATH = SECTION_AWS_STORAGEPATH
    SECTION_LOCAL_SCHEMA = {SECTION_LOCAL_STORAGEPATH: str}

    SECTION_AZURE_CONNECTION_STRING = "connection_string"
    # Alibabacloud oss options
    SECTION_OSS_ACCESS_KEY_ID = "oss_key_id"
    SECTION_OSS_ACCESS_KEY_SECRET = "oss_key_secret"
    SECTION_OSS_ENDPOINT = "oss_endpoint"

    SECTION_REMOTE_REGEX = r'^\s*remote\s*"(?P<name>.*)"\s*$'
    SECTION_REMOTE_FMT = 'remote "{}"'
    SECTION_REMOTE_URL = "url"
    SECTION_REMOTE_USER = "user"
    SECTION_REMOTE_PORT = "port"
    SECTION_REMOTE_KEY_FILE = "keyfile"
    SECTION_REMOTE_TIMEOUT = "timeout"
    SECTION_REMOTE_PASSWORD = "password"
    SECTION_REMOTE_ASK_PASSWORD = "ask_password"
    SECTION_REMOTE_GSS_AUTH = "gss_auth"
    SECTION_REMOTE_NO_TRAVERSE = "no_traverse"
    SECTION_REMOTE_SCHEMA = {
        SECTION_REMOTE_URL: str,
        Optional(SECTION_AWS_REGION): str,
        Optional(SECTION_AWS_PROFILE): str,
        Optional(SECTION_AWS_CREDENTIALPATH): str,
        Optional(SECTION_AWS_ENDPOINT_URL): str,
        Optional(SECTION_AWS_LIST_OBJECTS, default=False): BOOL_SCHEMA,
        Optional(SECTION_AWS_USE_SSL, default=True): BOOL_SCHEMA,
        Optional(SECTION_AWS_SSE): str,
        Optional(SECTION_AWS_ACL): str,
        Optional(SECTION_GCP_PROJECTNAME): str,
        Optional(SECTION_CACHE_TYPE): SECTION_CACHE_TYPE_SCHEMA,
        Optional(SECTION_CACHE_PROTECTED, default=False): BOOL_SCHEMA,
        Optional(SECTION_REMOTE_USER): str,
        Optional(SECTION_REMOTE_PORT): Use(int),
        Optional(SECTION_REMOTE_KEY_FILE): str,
        Optional(SECTION_REMOTE_TIMEOUT): Use(int),
        Optional(SECTION_REMOTE_PASSWORD): str,
        Optional(SECTION_REMOTE_ASK_PASSWORD): BOOL_SCHEMA,
        Optional(SECTION_REMOTE_GSS_AUTH): BOOL_SCHEMA,
        Optional(SECTION_AZURE_CONNECTION_STRING): str,
        Optional(SECTION_OSS_ACCESS_KEY_ID): str,
        Optional(SECTION_OSS_ACCESS_KEY_SECRET): str,
        Optional(SECTION_OSS_ENDPOINT): str,
        Optional(PRIVATE_CWD): str,
        Optional(SECTION_REMOTE_NO_TRAVERSE, default=True): BOOL_SCHEMA,
    }

    SECTION_STATE = "state"
    SECTION_STATE_ROW_LIMIT = "row_limit"
    SECTION_STATE_ROW_CLEANUP_QUOTA = "row_cleanup_quota"
    SECTION_STATE_SCHEMA = {
        Optional(SECTION_STATE_ROW_LIMIT): And(Use(int), is_whole),
        Optional(SECTION_STATE_ROW_CLEANUP_QUOTA): And(Use(int), is_percent),
    }

    SCHEMA = {
        Optional(SECTION_CORE, default={}): SECTION_CORE_SCHEMA,
        Optional(Regex(SECTION_REMOTE_REGEX)): SECTION_REMOTE_SCHEMA,
        Optional(SECTION_CACHE, default={}): SECTION_CACHE_SCHEMA,
        Optional(SECTION_STATE, default={}): SECTION_STATE_SCHEMA,
        # backward compatibility
        Optional(SECTION_AWS, default={}): SECTION_AWS_SCHEMA,
        Optional(SECTION_GCP, default={}): SECTION_GCP_SCHEMA,
        Optional(SECTION_LOCAL, default={}): SECTION_LOCAL_SCHEMA,
    }

    def __init__(self, dvc_dir=None, validate=True):
        self.dvc_dir = dvc_dir
        self.validate = validate

        if not dvc_dir:
            try:
                from dvc.repo import Repo

                self.dvc_dir = os.path.join(Repo.find_dvc_dir())
            except NotDvcRepoError:
                self.dvc_dir = None
        else:
            self.dvc_dir = os.path.abspath(os.path.realpath(dvc_dir))

        self.load()

    @staticmethod
    def get_global_config_dir():
        """Returns global config location. E.g. ~/.config/dvc/config.

        Returns:
            str: path to the global config directory.
        """
        from appdirs import user_config_dir

        return user_config_dir(
            appname=Config.APPNAME, appauthor=Config.APPAUTHOR
        )

    @staticmethod
    def get_system_config_dir():
        """Returns system config location. E.g. /etc/dvc.conf.

        Returns:
            str: path to the system config directory.
        """
        from appdirs import site_config_dir

        return site_config_dir(
            appname=Config.APPNAME, appauthor=Config.APPAUTHOR
        )

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

    def _resolve_cache_path(self, config):
        cache = config.get(self.SECTION_CACHE)
        if cache is None:
            return

        cache_dir = cache.get(self.SECTION_CACHE_DIR)
        if cache_dir is None:
            return

        cache[self.PRIVATE_CWD] = os.path.dirname(config.filename)

    def _resolve_paths(self, config):
        if config.filename is None:
            return config

        ret = copy.deepcopy(config)
        self._resolve_cache_path(ret)

        for section in ret.values():
            if self.SECTION_REMOTE_URL not in section.keys():
                continue

            section[self.PRIVATE_CWD] = os.path.dirname(ret.filename)

        return ret

    def _load_configs(self):
        system_config_file = os.path.join(
            self.get_system_config_dir(), self.CONFIG
        )

        global_config_file = os.path.join(
            self.get_global_config_dir(), self.CONFIG
        )

        self._system_config = configobj.ConfigObj(system_config_file)
        self._global_config = configobj.ConfigObj(global_config_file)
        self._repo_config = configobj.ConfigObj()
        self._local_config = configobj.ConfigObj()

        if not self.dvc_dir:
            return

        config_file = os.path.join(self.dvc_dir, self.CONFIG)
        config_local_file = os.path.join(self.dvc_dir, self.CONFIG_LOCAL)

        self._repo_config = configobj.ConfigObj(config_file)
        self._local_config = configobj.ConfigObj(config_local_file)

    @property
    def config_local_file(self):
        return self._local_config.filename

    @property
    def config_file(self):
        return self._repo_config.filename

    def load(self):
        """Loads config from all the config files.

        Raises:
            dvc.config.ConfigError: thrown if config has invalid format.
        """
        self._load_configs()

        self.config = configobj.ConfigObj()
        for c in [
            self._system_config,
            self._global_config,
            self._repo_config,
            self._local_config,
        ]:
            c = self._resolve_paths(c)
            c = self._lower(c)
            self.config.merge(c)

        if not self.validate:
            return

        d = self.config.dict()
        try:
            d = Schema(self.SCHEMA).validate(d)
        except SchemaError as exc:
            raise ConfigError("config format error", cause=exc)
        self.config = configobj.ConfigObj(d, write_empty_values=True)

    def save(self, config=None):
        """Saves config to config files.

        Raises:
            dvc.config.ConfigError: thrown if failed to write config file.
        """
        if config is not None:
            clist = [config]
        else:
            clist = [
                self._system_config,
                self._global_config,
                self._repo_config,
                self._local_config,
            ]

        for conf in clist:
            self._save(conf)

        self.load()

    @staticmethod
    def _save(config):
        if config.filename is None:
            return

        logger.debug("Writing '{}'.".format(config.filename))
        dname = os.path.dirname(os.path.abspath(config.filename))
        try:
            os.makedirs(dname)
        except OSError as exc:
            if exc.errno != errno.EEXIST:
                raise
        config.write()

    def unset(self, section, opt=None, level=None, force=False):
        """Unsets specified option and/or section in the config.

        Args:
            section (str): section name.
            opt (str): optional option name.
            level (int): config level to use.
            force (bool): don't error-out even if section doesn't exist. False
                by default.

        Raises:
            dvc.config.ConfigError: thrown if section doesn't exist and
                `force != True`.
        """
        config = self.get_configobj(level)

        if section not in config.keys():
            if force:
                return
            raise ConfigError("section '{}' doesn't exist".format(section))

        if opt:
            if opt not in config[section].keys():
                if force:
                    return
                raise ConfigError(
                    "option '{}.{}' doesn't exist".format(section, opt)
                )
            del config[section][opt]

            if not config[section]:
                del config[section]
        else:
            del config[section]

        self.save(config)

    def set(self, section, opt, value, level=None, force=True):
        """Sets specified option in the config.

        Args:
            section (str): section name.
            opt (str): option name.
            value: value to set option to.
            level (int): config level to use.
            force (bool): set option even if section already exists. True by
                default.

        Raises:
            dvc.config.ConfigError: thrown if section already exists and
                `force != True`.

        """
        config = self.get_configobj(level)

        if section not in config.keys():
            config[section] = {}
        elif not force:
            raise ConfigError(
                "Section '{}' already exists. Use `-f|--force` to overwrite "
                "section with new value.".format(section)
            )

        config[section][opt] = value
        self.save(config)

    def get(self, section, opt=None, level=None):
        """Return option value from the config.

        Args:
            section (str): section name.
            opt (str): option name.
            level (int): config level to use.

        Returns:
            value (str, int): option value.
        """
        config = self.get_configobj(level)

        if section not in config.keys():
            raise ConfigError("section '{}' doesn't exist".format(section))

        if opt not in config[section].keys():
            raise ConfigError(
                "option '{}.{}' doesn't exist".format(section, opt)
            )

        return config[section][opt]

    @staticmethod
    def _lower(config):
        new_config = configobj.ConfigObj()
        for s_key, s_value in config.items():
            new_s = {}
            for key, value in s_value.items():
                new_s[key.lower()] = str(value)
            new_config[s_key.lower()] = new_s
        return new_config

    def get_configobj(self, level):
        configs = {
            self.LEVEL_LOCAL: self._local_config,
            self.LEVEL_REPO: self._repo_config,
            self.LEVEL_GLOBAL: self._global_config,
            self.LEVEL_SYSTEM: self._system_config,
        }

        return configs.get(level, self._repo_config)

    def list_options(self, section_regex, option, level=None):
        ret = {}
        config = self.get_configobj(level)
        for section in config.keys():
            r = re.match(section_regex, section)
            if r:
                name = r.group("name")
                value = config[section].get(option, "")
                ret[name] = value
        return ret
