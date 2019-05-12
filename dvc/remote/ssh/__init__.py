from __future__ import unicode_literals

import os
import getpass
import logging

from dvc.path import Schemes
from dvc.path.ssh import PathSSH

try:
    import paramiko
except ImportError:
    paramiko = None

import dvc.prompt as prompt
from dvc.remote.ssh.connection import SSHConnection
from dvc.config import Config
from dvc.utils.compat import urlparse
from dvc.remote.base import RemoteBASE


logger = logging.getLogger(__name__)


class RemoteSSH(RemoteBASE):
    scheme = Schemes.SSH

    # NOTE: we support both URL-like (ssh://[user@]host.xz[:port]/path) and
    # SCP-like (ssh://[user@]host.xz:/absolute/path) urls.
    REGEX = r"^ssh://.*$"

    REQUIRES = {"paramiko": paramiko}

    JOBS = 4
    PARAM_CHECKSUM = "md5"
    DEFAULT_PORT = 22
    TIMEOUT = 1800

    def __init__(self, repo, config):
        super(RemoteSSH, self).__init__(repo, config)
        self.url = config.get(Config.SECTION_REMOTE_URL, "ssh://")

        parsed = urlparse(self.url)
        self.host = parsed.hostname

        user_ssh_config = self._load_user_ssh_config(self.host)

        self.host = user_ssh_config.get("hostname", self.host)
        self.user = (
            config.get(Config.SECTION_REMOTE_USER)
            or parsed.username
            or user_ssh_config.get("user")
            or getpass.getuser()
        )
        self.prefix = parsed.path or "/"
        self.port = (
            config.get(Config.SECTION_REMOTE_PORT)
            or parsed.port
            or self._try_get_ssh_config_port(user_ssh_config)
            or self.DEFAULT_PORT
        )
        self.keyfile = config.get(
            Config.SECTION_REMOTE_KEY_FILE
        ) or self._try_get_ssh_config_keyfile(user_ssh_config)
        self.timeout = config.get(Config.SECTION_REMOTE_TIMEOUT, self.TIMEOUT)
        self.password = config.get(Config.SECTION_REMOTE_PASSWORD, None)
        self.ask_password = config.get(
            Config.SECTION_REMOTE_ASK_PASSWORD, False
        )

        self.path_info = PathSSH(
            host=self.host, user=self.user, port=self.port
        )

    @staticmethod
    def ssh_config_filename():
        return os.path.expanduser(os.path.join("~", ".ssh", "config"))

    @staticmethod
    def _load_user_ssh_config(hostname):
        user_config_file = RemoteSSH.ssh_config_filename()
        user_ssh_config = dict()
        if hostname and os.path.exists(user_config_file):
            with open(user_config_file) as f:
                ssh_config = paramiko.SSHConfig()
                ssh_config.parse(f)
                user_ssh_config = ssh_config.lookup(hostname)
        return user_ssh_config

    @staticmethod
    def _try_get_ssh_config_port(user_ssh_config):
        try:
            return int(user_ssh_config.get("port"))
        except (ValueError, TypeError):
            return None

    @staticmethod
    def _try_get_ssh_config_keyfile(user_ssh_config):
        identity_file = user_ssh_config.get("identityfile")
        if identity_file and len(identity_file) > 0:
            return identity_file[0]
        return None

    def ssh(self, path_info):
        host, user, port = path_info.host, path_info.user, path_info.port

        logger.debug(
            "Establishing ssh connection with '{host}' "
            "through port '{port}' as user '{user}'".format(
                host=host, user=user, port=port
            )
        )

        if self.ask_password and not self.password:
            self.password = prompt.password(
                "Enter a private key passphrase or a password for "
                "host '{host}' port '{port}' user '{user}'".format(
                    host=host, port=port, user=user
                )
            )

        return SSHConnection(
            host,
            username=user,
            port=port,
            key_filename=self.keyfile,
            timeout=self.timeout,
            password=self.password,
        )

    def exists(self, path_info):
        assert not isinstance(path_info, list)
        assert path_info.scheme == self.scheme

        with self.ssh(path_info) as ssh:
            return ssh.exists(path_info.path)

    def get_file_checksum(self, path_info):
        if path_info.scheme != self.scheme:
            raise NotImplementedError

        with self.ssh(path_info) as ssh:
            return ssh.md5(path_info.path)

    def isdir(self, path_info):
        with self.ssh(path_info) as ssh:
            return ssh.isdir(path_info.path)

    def copy(self, from_info, to_info):
        if from_info.scheme != self.scheme or to_info.scheme != self.scheme:
            raise NotImplementedError

        with self.ssh(from_info) as ssh:
            ssh.cp(from_info.path, to_info.path)

    def remove(self, path_info):
        if path_info.scheme != self.scheme:
            raise NotImplementedError

        with self.ssh(path_info) as ssh:
            ssh.remove(path_info.path)

    def move(self, from_info, to_info):
        if from_info.scheme != self.scheme or to_info.scheme != self.scheme:
            raise NotImplementedError

        with self.ssh(from_info) as ssh:
            ssh.move(from_info.path, to_info.path)

    def download(
        self,
        from_infos,
        to_infos,
        no_progress_bar=False,
        names=None,
        resume=False,
    ):
        names = self._verify_path_args(from_infos, to_infos, names)
        ssh = self.ssh(from_infos[0])

        for to_info, from_info, name in zip(to_infos, from_infos, names):
            if from_info.scheme != self.scheme:
                raise NotImplementedError

            if to_info.scheme == self.scheme:
                ssh.cp(from_info.path, to_info.path)
                continue

            if to_info.scheme != "local":
                raise NotImplementedError

            logger.debug(
                "Downloading '{host}/{path}' to '{dest}'".format(
                    host=from_info.host, path=from_info.path, dest=to_info.path
                )
            )

            try:
                ssh.download(
                    from_info.path,
                    to_info.path,
                    progress_title=name,
                    no_progress_bar=no_progress_bar,
                )
            except Exception:
                logger.exception(
                    "failed to download '{host}/{path}' to '{dest}'".format(
                        host=from_info.host,
                        path=from_info.path,
                        dest=to_info.path,
                    )
                )
                continue

        ssh.close()

    def upload(self, from_infos, to_infos, names=None, no_progress_bar=False):
        names = self._verify_path_args(to_infos, from_infos, names)

        with self.ssh(to_infos[0]) as ssh:
            for from_info, to_info, name in zip(from_infos, to_infos, names):
                if to_info.scheme != self.scheme:
                    raise NotImplementedError

                if from_info.scheme != "local":
                    raise NotImplementedError

                try:
                    ssh.upload(
                        from_info.path,
                        to_info.path,
                        progress_title=name,
                        no_progress_bar=no_progress_bar,
                    )
                except Exception:
                    logger.exception(
                        "failed to upload '{host}/{path}' to '{dest}'".format(
                            host=from_info.host,
                            path=from_info.path,
                            dest=to_info.path,
                        )
                    )
                    pass

    def list_cache_paths(self):
        with self.ssh(self.path_info) as ssh:
            return list(ssh.walk_files(self.prefix))

    def walk(self, path_info):
        with self.ssh(path_info) as ssh:
            for entry in ssh.walk(path_info.path):
                yield entry

    def makedirs(self, path_info):
        with self.ssh(path_info) as ssh:
            ssh.makedirs(path_info.path)
