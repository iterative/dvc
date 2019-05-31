import os
import logging
import posixpath

from dvc.utils.compat import urlparse
from dvc.config import Config, ConfigError


logger = logging.getLogger(__name__)


def get_remote_settings(config, name):
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
    settings = config.config.get(
        config.SECTION_REMOTE_FMT.format(name.lower())
    )

    if settings is None:
        raise ConfigError("unable to find remote section '{}'".format(name))

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
        reference = get_remote_settings(config, parsed.netloc)
        url = posixpath.join(reference["url"], parsed.path.lstrip("/"))
        merged = reference.copy()
        merged.update(settings)
        merged["url"] = url
        return merged

    return settings


def _resolve_remote_path(path, config_file):
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
    return os.path.relpath(path, os.path.dirname(config_file))


def remote_add(config, name, url, default=False, force=False, level=None):
    from dvc.remote import _get, RemoteLOCAL

    configobj = config.get_configobj(level)
    remote = _get({Config.SECTION_REMOTE_URL: url})
    if remote == RemoteLOCAL and not url.startswith("remote://"):
        url = _resolve_remote_path(url, configobj.filename)

    config.set(
        config.SECTION_REMOTE_FMT.format(name),
        config.SECTION_REMOTE_URL,
        url,
        force=force,
        level=level,
    )
    if default:
        logger.info("Setting '{}' as a default remote.".format(name))
        config.set(
            config.SECTION_CORE, config.SECTION_CORE_REMOTE, name, level=level
        )


def remote_remove(config, name, level=None):
    config.unset(config.SECTION_REMOTE_FMT.format(name), level=level)

    if level is None:
        level = config.LEVEL_REPO

    for lev in [
        config.LEVEL_LOCAL,
        config.LEVEL_REPO,
        config.LEVEL_GLOBAL,
        config.LEVEL_SYSTEM,
    ]:
        config.unset(
            config.SECTION_CORE,
            config.SECTION_CORE_REMOTE,
            level=lev,
            force=True,
        )
        if lev == level:
            break


def remote_modify(config, name, option, value, level=None):
    config.set(
        config.SECTION_REMOTE_FMT.format(name), option, value, level=level
    )


def remote_list(config, level=None):
    return config.list_options(
        config.SECTION_REMOTE_REGEX, config.SECTION_REMOTE_URL, level=level
    )


def remote_default(config, name, unset=False, level=None):
    if unset:
        config.unset(config.SECTION_CORE, config.SECTION_CORE_REMOTE)
        return
    config.set(
        config.SECTION_CORE, config.SECTION_CORE_REMOTE, name, level=level
    )


def set_cache_dir(config, dname, level=None):
    configobj = config.get_configobj(level)
    path = _resolve_remote_path(dname, configobj.filename)
    config.set(
        config.SECTION_CACHE, config.SECTION_CACHE_DIR, path, level=level
    )
