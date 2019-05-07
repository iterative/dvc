"""DVC config objects."""

from __future__ import unicode_literals

from dvc.utils.compat import str, open, urlparse

import os
import errno
import configobj
import logging

from schema import Schema, Optional, And, Use, Regex
from dvc.exceptions import DvcException

logger = logging.getLogger(__name__)


class ConfigError(DvcException):
    """DVC config exception.

    Args:
        msg (str): error message.
        ex (Exception): optional exception that has caused this error.
    """

    def __init__(self, msg, ex=None):
        super(ConfigError, self).__init__(
            "config file error: {}".format(msg), ex
        )


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


def supported_loglevel(level):
    """Checks if log level config option has a valid value.

    Args:
        level (str): log level name.
    """
    return level in ["info", "debug", "warning", "error"]


def supported_cloud(cloud):
    """Checks if obsoleted cloud option has a valid value.

    Args:
        cloud (str): cloud type name.
    """
    return cloud in ["aws", "gcp", "local", ""]


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

    BOOL_SCHEMA = And(str, is_bool, Use(to_bool))

    SECTION_CORE = "core"
    SECTION_CORE_LOGLEVEL = "loglevel"
    SECTION_CORE_LOGLEVEL_SCHEMA = And(Use(str.lower), supported_loglevel)
    SECTION_CORE_REMOTE = "remote"
    SECTION_CORE_INTERACTIVE_SCHEMA = BOOL_SCHEMA
    SECTION_CORE_INTERACTIVE = "interactive"
    SECTION_CORE_ANALYTICS = "analytics"
    SECTION_CORE_ANALYTICS_SCHEMA = BOOL_SCHEMA

    SECTION_CACHE = "cache"
    SECTION_CACHE_DIR = "dir"
    SECTION_CACHE_TYPE = "type"
    SECTION_CACHE_TYPE_SCHEMA = supported_cache_type
    SECTION_CACHE_PROTECTED = "protected"
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
        Optional(PRIVATE_CWD): str,
        Optional(SECTION_CACHE_SLOW_LINK_WARNING, default=True): BOOL_SCHEMA,
    }

    # backward compatibility
    SECTION_CORE_CLOUD = "cloud"
    SECTION_CORE_CLOUD_SCHEMA = And(Use(str.lower), supported_cloud)
    SECTION_CORE_STORAGEPATH = "storagepath"

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
        # backward compatibility
        Optional(SECTION_CORE_CLOUD, default=""): SECTION_CORE_CLOUD_SCHEMA,
        Optional(SECTION_CORE_STORAGEPATH, default=""): str,
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
    SECTION_AWS_SCHEMA = {
        SECTION_AWS_STORAGEPATH: str,
        Optional(SECTION_AWS_REGION): str,
        Optional(SECTION_AWS_PROFILE): str,
        Optional(SECTION_AWS_CREDENTIALPATH): str,
        Optional(SECTION_AWS_ENDPOINT_URL): str,
        Optional(SECTION_AWS_LIST_OBJECTS, default=False): BOOL_SCHEMA,
        Optional(SECTION_AWS_USE_SSL, default=True): BOOL_SCHEMA,
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
    SECTION_REMOTE_SCHEMA = {
        SECTION_REMOTE_URL: str,
        Optional(SECTION_AWS_REGION): str,
        Optional(SECTION_AWS_PROFILE): str,
        Optional(SECTION_AWS_CREDENTIALPATH): str,
        Optional(SECTION_AWS_ENDPOINT_URL): str,
        Optional(SECTION_AWS_LIST_OBJECTS, default=False): BOOL_SCHEMA,
        Optional(SECTION_AWS_USE_SSL, default=True): BOOL_SCHEMA,
        Optional(SECTION_GCP_PROJECTNAME): str,
        Optional(SECTION_CACHE_TYPE): SECTION_CACHE_TYPE_SCHEMA,
        Optional(SECTION_CACHE_PROTECTED, default=False): BOOL_SCHEMA,
        Optional(SECTION_REMOTE_USER): str,
        Optional(SECTION_REMOTE_PORT): Use(int),
        Optional(SECTION_REMOTE_KEY_FILE): str,
        Optional(SECTION_REMOTE_TIMEOUT): Use(int),
        Optional(SECTION_REMOTE_PASSWORD): str,
        Optional(SECTION_REMOTE_ASK_PASSWORD): BOOL_SCHEMA,
        Optional(SECTION_AZURE_CONNECTION_STRING): str,
        Optional(SECTION_OSS_ACCESS_KEY_ID): str,
        Optional(SECTION_OSS_ACCESS_KEY_SECRET): str,
        Optional(SECTION_OSS_ENDPOINT): str,
        Optional(PRIVATE_CWD): str,
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
        self.system_config_file = os.path.join(
            self.get_system_config_dir(), self.CONFIG
        )
        self.global_config_file = os.path.join(
            self.get_global_config_dir(), self.CONFIG
        )

        if dvc_dir is not None:
            self.dvc_dir = os.path.abspath(os.path.realpath(dvc_dir))
            self.config_file = os.path.join(dvc_dir, self.CONFIG)
            self.config_local_file = os.path.join(dvc_dir, self.CONFIG_LOCAL)
        else:
            self.dvc_dir = None
            self.config_file = None
            self.config_local_file = None

        self._system_config = None
        self._global_config = None
        self._repo_config = None
        self._local_config = None

        self.config = None

        self.load(validate=validate)

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

    def _load(self):
        self._system_config = configobj.ConfigObj(self.system_config_file)
        self._global_config = configobj.ConfigObj(self.global_config_file)

        if self.config_file is not None:
            self._repo_config = configobj.ConfigObj(self.config_file)
        else:
            self._repo_config = configobj.ConfigObj()

        if self.config_local_file is not None:
            self._local_config = configobj.ConfigObj(self.config_local_file)
        else:
            self._local_config = configobj.ConfigObj()

        self.config = None

    def _load_config(self, path):
        config = configobj.ConfigObj(path)
        config = self._lower(config)
        self._resolve_paths(config, path)
        return config

    @staticmethod
    def _resolve_path(path, config_file):
        assert os.path.isabs(config_file)
        config_dir = os.path.dirname(config_file)
        return os.path.abspath(os.path.join(config_dir, path))

    def _resolve_cache_path(self, config, fname):
        cache = config.get(self.SECTION_CACHE)
        if cache is None:
            return

        cache_dir = cache.get(self.SECTION_CACHE_DIR)
        if cache_dir is None:
            return

        cache[self.PRIVATE_CWD] = os.path.dirname(fname)

    def _resolve_paths(self, config, fname):
        if fname is None:
            return

        self._resolve_cache_path(config, fname)
        for section in config.values():
            if self.SECTION_REMOTE_URL not in section.keys():
                continue

            section[self.PRIVATE_CWD] = os.path.dirname(fname)

    def load(self, validate=True):
        """Loads config from all the config files.

        Args:
            validate (bool): optional flag to tell dvc if it should validate
                the config or just load it as is. 'True' by default.


        Raises:
            dvc.config.ConfigError: thrown if config has invalid format.
        """
        self._load()
        try:
            self.config = self._load_config(self.system_config_file)
            user = self._load_config(self.global_config_file)
            config = self._load_config(self.config_file)
            local = self._load_config(self.config_local_file)

            # NOTE: schema doesn't support ConfigObj.Section validation, so we
            # need to convert our config to dict before passing it to
            for conf in [user, config, local]:
                self.config = self._merge(self.config, conf)

            if validate:
                self.config = Schema(self.SCHEMA).validate(self.config)

            # NOTE: now converting back to ConfigObj
            self.config = configobj.ConfigObj(
                self.config, write_empty_values=True
            )
            self.config.filename = self.config_file
            self._resolve_paths(self.config, self.config_file)
        except Exception as ex:
            raise ConfigError(ex)

    @staticmethod
    def _get_key(conf, name, add=False):
        for k in conf.keys():
            if k.lower() == name.lower():
                return k

        if add:
            conf[name] = {}
            return name

        return None

    def save(self, config=None):
        """Saves config to config files.

        Args:
            config (configobj.ConfigObj): optional config object to save.

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
            if conf.filename is None:
                continue

            try:
                logger.debug("Writing '{}'.".format(conf.filename))
                dname = os.path.dirname(os.path.abspath(conf.filename))
                try:
                    os.makedirs(dname)
                except OSError as exc:
                    if exc.errno != errno.EEXIST:
                        raise
                conf.write()
            except Exception as exc:
                msg = "failed to write config '{}'".format(conf.filename)
                raise ConfigError(msg, exc)

    def get_remote_settings(self, name):
        import posixpath

        """
        Args:
            name (str): The name of the remote that we want to retrieve

        Returns:
            dict: The content beneath the given remote name.

        Example:
            >>> config = {'remote "server"': {'url': 'ssh://localhost/'}}
            >>> get_remote_settings("server")
            {'url': 'ssh://localhost/'}
        """
        settings = self.config[self.SECTION_REMOTE_FMT.format(name)]
        parsed = urlparse(settings["url"])

        # Support for cross referenced remotes.
        # This will merge the settings, giving priority to the outer reference.
        # For example, having:
        #
        #       dvc remote add server ssh://localhost
        #       dvc remote modify server user root
        #       dvc remote modify server ask_password true
        #
        #       dvc remote add images remote://server/tmp/pictures
        #       dvc remote modify images user alice
        #       dvc remote modify images ask_password false
        #       dvc remote modify images password asdf1234
        #
        # Results on a config dictionary like:
        #
        #       {
        #           "url": "ssh://localhost/tmp/pictures",
        #           "user": "alice",
        #           "password": "asdf1234",
        #           "ask_password": False,
        #       }
        #
        if parsed.scheme == "remote":
            reference = self.get_remote_settings(parsed.netloc)
            url = posixpath.join(reference["url"], parsed.path.lstrip("/"))
            merged = reference.copy()
            merged.update(settings)
            merged["url"] = url
            return merged

        return settings

    @staticmethod
    def unset(config, section, opt=None):
        """Unsets specified option and/or section in the config.

        Args:
            config (configobj.ConfigObj): config to work on.
            section (str): section name.
            opt (str): optional option name.
        """
        if section not in config.keys():
            raise ConfigError("section '{}' doesn't exist".format(section))

        if opt is None:
            del config[section]
            return

        if opt not in config[section].keys():
            raise ConfigError(
                "option '{}.{}' doesn't exist".format(section, opt)
            )
        del config[section][opt]

        if not config[section]:
            del config[section]

    @staticmethod
    def set(config, section, opt, value):
        """Sets specified option in the config.

        Args:
            config (configobj.ConfigObj): config to work on.
            section (str): section name.
            opt (str): option name.
            value: value to set option to.
        """
        if section not in config.keys():
            config[section] = {}

        config[section][opt] = value

    @staticmethod
    def show(config, section, opt):
        """Prints option value from the config.

        Args:
            config (configobj.ConfigObj): config to work on.
            section (str): section name.
            opt (str): option name.
        """
        if section not in config.keys():
            raise ConfigError("section '{}' doesn't exist".format(section))

        if opt not in config[section].keys():
            raise ConfigError(
                "option '{}.{}' doesn't exist".format(section, opt)
            )

        logger.info(config[section][opt])

    @staticmethod
    def _merge(first, second):
        res = {}
        sections = list(first.keys()) + list(second.keys())
        for section in sections:
            first_copy = first.get(section, {}).copy()
            second_copy = second.get(section, {}).copy()
            first_copy.update(second_copy)
            res[section] = first_copy
        return res

    @staticmethod
    def _lower(config):
        new_config = {}
        for s_key, s_value in config.items():
            new_s = {}
            for key, value in s_value.items():
                new_s[key.lower()] = str(value)
            new_config[s_key.lower()] = new_s
        return new_config
