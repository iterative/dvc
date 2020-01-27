"""DVC config objects."""

import copy
import errno
import logging
import os
import re

import configobj
from voluptuous import Schema, Required, Optional, Invalid
from voluptuous import All, Any, Lower, Range, Coerce, Match

from dvc.exceptions import DvcException
from dvc.exceptions import NotDvcRepoError

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
Bool = All(
    Lower,
    Any("true", "false"),
    lambda v: v == "true",
    msg="expected true or false",
)
to_bool = Schema(Bool)


def Choices(*choices):
    """Checks that value belongs to the specified set of values

    Args:
        *choices: pass allowed values as arguments, or pass a list or
            tuple as a single argument
    """
    return Any(*choices, msg="expected one of {}".format(", ".join(choices)))


class Config(object):  # pylint: disable=too-many-instance-attributes
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

    # NOTE: used internally in RemoteLOCAL to know config
    # location, that url should resolved relative to.
    PRIVATE_CWD = "_cwd"

    CONFIG = "config"
    CONFIG_LOCAL = "config.local"

    CREDENTIALPATH = "credentialpath"

    LEVEL_LOCAL = 0
    LEVEL_REPO = 1
    LEVEL_GLOBAL = 2
    LEVEL_SYSTEM = 3

    SECTION_CORE = "core"
    SECTION_CORE_LOGLEVEL = "loglevel"
    SECTION_CORE_LOGLEVEL_SCHEMA = All(
        Lower, Choices("info", "debug", "warning", "error")
    )
    SECTION_CORE_REMOTE = "remote"
    SECTION_CORE_INTERACTIVE = "interactive"
    SECTION_CORE_ANALYTICS = "analytics"
    SECTION_CORE_CHECKSUM_JOBS = "checksum_jobs"
    SECTION_CORE_HARDLINK_LOCK = "hardlink_lock"

    SECTION_CACHE = "cache"
    SECTION_CACHE_DIR = "dir"
    SECTION_CACHE_TYPE = "type"
    SECTION_CACHE_PROTECTED = "protected"
    SECTION_CACHE_SHARED = "shared"
    SECTION_CACHE_SHARED_SCHEMA = All(Lower, Choices("group"))
    SECTION_CACHE_LOCAL = "local"
    SECTION_CACHE_S3 = "s3"
    SECTION_CACHE_GS = "gs"
    SECTION_CACHE_SSH = "ssh"
    SECTION_CACHE_HDFS = "hdfs"
    SECTION_CACHE_AZURE = "azure"
    SECTION_CACHE_SLOW_LINK_WARNING = "slow_link_warning"
    SECTION_CACHE_SCHEMA = {
        SECTION_CACHE_LOCAL: str,
        SECTION_CACHE_S3: str,
        SECTION_CACHE_GS: str,
        SECTION_CACHE_HDFS: str,
        SECTION_CACHE_SSH: str,
        SECTION_CACHE_AZURE: str,
        SECTION_CACHE_DIR: str,
        SECTION_CACHE_TYPE: supported_cache_type,
        Optional(SECTION_CACHE_PROTECTED, default=False): Bool,
        SECTION_CACHE_SHARED: SECTION_CACHE_SHARED_SCHEMA,
        PRIVATE_CWD: str,
        Optional(SECTION_CACHE_SLOW_LINK_WARNING, default=True): Bool,
    }

    SECTION_CORE_SCHEMA = {
        SECTION_CORE_LOGLEVEL: SECTION_CORE_LOGLEVEL_SCHEMA,
        SECTION_CORE_REMOTE: Lower,
        Optional(SECTION_CORE_INTERACTIVE, default=False): Bool,
        Optional(SECTION_CORE_ANALYTICS, default=True): Bool,
        SECTION_CORE_CHECKSUM_JOBS: All(Coerce(int), Range(1)),
        Optional(SECTION_CORE_HARDLINK_LOCK, default=False): Bool,
    }

    # aws specific options
    SECTION_AWS_CREDENTIALPATH = CREDENTIALPATH
    SECTION_AWS_ENDPOINT_URL = "endpointurl"
    SECTION_AWS_LIST_OBJECTS = "listobjects"
    SECTION_AWS_REGION = "region"
    SECTION_AWS_PROFILE = "profile"
    SECTION_AWS_USE_SSL = "use_ssl"
    SECTION_AWS_SSE = "sse"
    SECTION_AWS_ACL = "acl"
    SECTION_AWS_GRANT_READ = "grant_read"
    SECTION_AWS_GRANT_READ_ACP = "grant_read_acp"
    SECTION_AWS_GRANT_WRITE_ACP = "grant_write_acp"
    SECTION_AWS_GRANT_FULL_CONTROL = "grant_full_control"

    # gcp specific options
    SECTION_GCP_CREDENTIALPATH = CREDENTIALPATH
    SECTION_GCP_PROJECTNAME = "projectname"

    # azure specific option
    SECTION_AZURE_CONNECTION_STRING = "connection_string"

    # Alibabacloud oss options
    SECTION_OSS_ACCESS_KEY_ID = "oss_key_id"
    SECTION_OSS_ACCESS_KEY_SECRET = "oss_key_secret"
    SECTION_OSS_ENDPOINT = "oss_endpoint"

    # GDrive options
    SECTION_GDRIVE_CLIENT_ID = "gdrive_client_id"
    SECTION_GDRIVE_CLIENT_SECRET = "gdrive_client_secret"
    SECTION_GDRIVE_USER_CREDENTIALS_FILE = "gdrive_user_credentials_file"

    SECTION_REMOTE_CHECKSUM_JOBS = "checksum_jobs"
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
    SECTION_REMOTE_VERIFY = "verify"
    SECTION_REMOTE_SCHEMA = {
        Required(SECTION_REMOTE_URL): str,
        SECTION_AWS_REGION: str,
        SECTION_AWS_PROFILE: str,
        SECTION_AWS_CREDENTIALPATH: str,
        SECTION_AWS_ENDPOINT_URL: str,
        Optional(SECTION_AWS_LIST_OBJECTS, default=False): Bool,
        Optional(SECTION_AWS_USE_SSL, default=True): Bool,
        SECTION_AWS_SSE: str,
        SECTION_AWS_ACL: str,
        SECTION_AWS_GRANT_READ: str,
        SECTION_AWS_GRANT_READ_ACP: str,
        SECTION_AWS_GRANT_WRITE_ACP: str,
        SECTION_AWS_GRANT_FULL_CONTROL: str,
        SECTION_GCP_PROJECTNAME: str,
        SECTION_CACHE_TYPE: supported_cache_type,
        Optional(SECTION_CACHE_PROTECTED, default=False): Bool,
        SECTION_REMOTE_CHECKSUM_JOBS: All(Coerce(int), Range(1)),
        SECTION_REMOTE_USER: str,
        SECTION_REMOTE_PORT: Coerce(int),
        SECTION_REMOTE_KEY_FILE: str,
        SECTION_REMOTE_TIMEOUT: Coerce(int),
        SECTION_REMOTE_PASSWORD: str,
        SECTION_REMOTE_ASK_PASSWORD: Bool,
        SECTION_REMOTE_GSS_AUTH: Bool,
        SECTION_AZURE_CONNECTION_STRING: str,
        SECTION_OSS_ACCESS_KEY_ID: str,
        SECTION_OSS_ACCESS_KEY_SECRET: str,
        SECTION_OSS_ENDPOINT: str,
        SECTION_GDRIVE_CLIENT_ID: str,
        SECTION_GDRIVE_CLIENT_SECRET: str,
        SECTION_GDRIVE_USER_CREDENTIALS_FILE: str,
        PRIVATE_CWD: str,
        SECTION_REMOTE_NO_TRAVERSE: Bool,
        SECTION_REMOTE_VERIFY: Bool,
    }

    SECTION_STATE = "state"
    SECTION_STATE_ROW_LIMIT = "row_limit"
    SECTION_STATE_ROW_CLEANUP_QUOTA = "row_cleanup_quota"
    SECTION_STATE_SCHEMA = {
        SECTION_STATE_ROW_LIMIT: All(Coerce(int), Range(1)),
        SECTION_STATE_ROW_CLEANUP_QUOTA: All(Coerce(int), Range(0, 100)),
    }

    SCHEMA = {
        Optional(SECTION_CORE, default={}): SECTION_CORE_SCHEMA,
        Match(SECTION_REMOTE_REGEX): SECTION_REMOTE_SCHEMA,
        Optional(SECTION_CACHE, default={}): SECTION_CACHE_SCHEMA,
        Optional(SECTION_STATE, default={}): SECTION_STATE_SCHEMA,
    }
    COMPILED_SCHEMA = Schema(SCHEMA)

    def __init__(self, dvc_dir=None, validate=True):
        self.dvc_dir = dvc_dir
        self.should_validate = validate

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

        if not self.should_validate:
            return

        d = self.validate(self.config)
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

    def validate(self, config):
        try:
            return self.COMPILED_SCHEMA(config.dict())
        except Invalid as exc:
            raise ConfigError(str(exc)) from exc

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

        result = copy.deepcopy(self.config)
        result.merge(config)
        self.validate(result)

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
