import logging
import os
import posixpath

from dvc.config import Config
from dvc.config import ConfigError
from dvc.utils import relpath
from dvc.utils.compat import urlparse


logger = logging.getLogger(__name__)


class RemoteConfig(object):
    def __init__(self, config):
        self.config = config

    def get_settings(self, name):
        """
        Args:
            name (str): The name of the remote that we want to retrieve

        Returns:
            dict: The content beneath the given remote name.

        Example:
            >>> config = {'remote "server"': {'url': 'ssh://localhost/'}}
            >>> get_settings("server")
            {'url': 'ssh://localhost/'}
        """
        settings = self.config.config.get(
            Config.SECTION_REMOTE_FMT.format(name.lower())
        )

        if settings is None:
            raise ConfigError(
                "unable to find remote section '{}'".format(name)
            )

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
            reference = self.get_settings(parsed.netloc)
            url = posixpath.join(reference["url"], parsed.path.lstrip("/"))
            merged = reference.copy()
            merged.update(settings)
            merged["url"] = url
            return merged

        return settings

    @staticmethod
    def resolve_path(path, config_file):
        """Resolve path relative to config file location.

        Args:
            path: Path to be resolved.
            config_file: Path to config file, which `path` is specified
                relative to.

        Returns:
            Path relative to the `config_file` location. If `path` is an
            absolute path then it will be returned without change.

        """
        if os.path.isabs(path):
            return path
        return relpath(path, os.path.dirname(config_file))

    def add(self, name, url, default=False, force=False, level=None):
        from dvc.remote import _get, RemoteLOCAL

        configobj = self.config.get_configobj(level)
        remote = _get({Config.SECTION_REMOTE_URL: url})
        if remote == RemoteLOCAL and not url.startswith("remote://"):
            url = self.resolve_path(url, configobj.filename)

        self.config.set(
            Config.SECTION_REMOTE_FMT.format(name),
            Config.SECTION_REMOTE_URL,
            url,
            force=force,
            level=level,
        )
        if default:
            self.config.set(
                Config.SECTION_CORE,
                Config.SECTION_CORE_REMOTE,
                name,
                level=level,
            )

    def remove(self, name, level=None):
        self.config.unset(Config.SECTION_REMOTE_FMT.format(name), level=level)

        if level is None:
            level = Config.LEVEL_REPO

        for lev in [
            Config.LEVEL_LOCAL,
            Config.LEVEL_REPO,
            Config.LEVEL_GLOBAL,
            Config.LEVEL_SYSTEM,
        ]:
            self.config.unset(
                Config.SECTION_CORE,
                Config.SECTION_CORE_REMOTE,
                level=lev,
                force=True,
            )
            if lev == level:
                break

    def modify(self, name, option, value, level=None):
        self.config.set(
            Config.SECTION_REMOTE_FMT.format(name), option, value, level=level
        )

    def list(self, level=None):
        return self.config.list_options(
            Config.SECTION_REMOTE_REGEX, Config.SECTION_REMOTE_URL, level=level
        )

    def set_default(self, name, unset=False, level=None):
        if unset:
            self.config.unset(Config.SECTION_CORE, Config.SECTION_CORE_REMOTE)
            return
        self.config.set(
            Config.SECTION_CORE, Config.SECTION_CORE_REMOTE, name, level=level
        )

    def get_default(self, level=None):
        return self.config.get(
            Config.SECTION_CORE, Config.SECTION_CORE_REMOTE, level=level
        )
