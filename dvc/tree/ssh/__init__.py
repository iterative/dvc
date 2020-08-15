import getpass
import io
import logging
import os
import posixpath
import threading
from contextlib import closing, contextmanager
from urllib.parse import urlparse

from funcy import first, memoize, silent, wrap_with

import dvc.prompt as prompt
from dvc.scheme import Schemes

from ..base import BaseTree
from ..pool import get_connection

logger = logging.getLogger(__name__)


@wrap_with(threading.Lock())
@memoize
def ask_password(host, user, port):
    return prompt.password(
        "Enter a private key passphrase or a password for "
        "host '{host}' port '{port}' user '{user}'".format(
            host=host, port=port, user=user
        )
    )


class SSHTree(BaseTree):
    scheme = Schemes.SSH
    REQUIRES = {"paramiko": "paramiko"}
    JOBS = 4

    PARAM_CHECKSUM = "md5"
    # At any given time some of the connections will go over network and
    # paramiko stuff, so we would ideally have it double of server processors.
    # We use conservative setting of 4 instead to not exhaust max sessions.
    CHECKSUM_JOBS = 4
    DEFAULT_CACHE_TYPES = ["copy"]
    TRAVERSE_PREFIX_LEN = 2

    DEFAULT_PORT = 22
    TIMEOUT = 1800

    def __init__(self, repo, config):
        super().__init__(repo, config)
        url = config.get("url")
        if url:
            parsed = urlparse(url)
            user_ssh_config = self._load_user_ssh_config(parsed.hostname)

            host = user_ssh_config.get("hostname", parsed.hostname)
            user = (
                config.get("user")
                or parsed.username
                or user_ssh_config.get("user")
                or getpass.getuser()
            )
            port = (
                config.get("port")
                or parsed.port
                or self._try_get_ssh_config_port(user_ssh_config)
                or self.DEFAULT_PORT
            )
            self.path_info = self.PATH_CLS.from_parts(
                scheme=self.scheme,
                host=host,
                user=user,
                port=port,
                path=parsed.path,
            )
        else:
            self.path_info = None
            user_ssh_config = {}

        self.keyfile = config.get(
            "keyfile"
        ) or self._try_get_ssh_config_keyfile(user_ssh_config)
        self.timeout = config.get("timeout", self.TIMEOUT)
        self.password = config.get("password", None)
        self.ask_password = config.get("ask_password", False)
        self.gss_auth = config.get("gss_auth", False)
        proxy_command = user_ssh_config.get("proxycommand", False)
        if proxy_command:
            import paramiko

            self.sock = paramiko.ProxyCommand(proxy_command)
        else:
            self.sock = None

    @staticmethod
    def ssh_config_filename():
        return os.path.expanduser(os.path.join("~", ".ssh", "config"))

    @staticmethod
    def _load_user_ssh_config(hostname):
        import paramiko

        user_config_file = SSHTree.ssh_config_filename()
        user_ssh_config = {}
        if hostname and os.path.exists(user_config_file):
            ssh_config = paramiko.SSHConfig()
            with open(user_config_file) as f:
                # For whatever reason parsing directly from f is unreliable
                f_copy = io.StringIO(f.read())
                ssh_config.parse(f_copy)
            user_ssh_config = ssh_config.lookup(hostname)
        return user_ssh_config

    @staticmethod
    def _try_get_ssh_config_port(user_ssh_config):
        return silent(int)(user_ssh_config.get("port"))

    @staticmethod
    def _try_get_ssh_config_keyfile(user_ssh_config):
        return first(user_ssh_config.get("identityfile") or ())

    def ensure_credentials(self, path_info=None):
        if path_info is None:
            path_info = self.path_info

        # NOTE: we use the same password regardless of the server :(
        if self.ask_password and self.password is None:
            host, user, port = path_info.host, path_info.user, path_info.port
            self.password = ask_password(host, user, port)

    def ssh(self, path_info):
        self.ensure_credentials(path_info)

        from .connection import SSHConnection

        return get_connection(
            SSHConnection,
            path_info.host,
            username=path_info.user,
            port=path_info.port,
            key_filename=self.keyfile,
            timeout=self.timeout,
            password=self.password,
            gss_auth=self.gss_auth,
            sock=self.sock,
        )

    @contextmanager
    def open(self, path_info, mode="r", encoding=None):
        assert mode in {"r", "rt", "rb", "wb"}

        with self.ssh(path_info) as ssh, closing(
            ssh.sftp.open(path_info.path, mode)
        ) as fd:
            if "b" in mode:
                yield fd
            else:
                yield io.TextIOWrapper(fd, encoding=encoding)

    def exists(self, path_info, use_dvcignore=True):
        with self.ssh(path_info) as ssh:
            return ssh.exists(path_info.path)

    def isdir(self, path_info):
        with self.ssh(path_info) as ssh:
            return ssh.isdir(path_info.path)

    def isfile(self, path_info):
        with self.ssh(path_info) as ssh:
            return ssh.isfile(path_info.path)

    def walk_files(self, path_info, **kwargs):
        with self.ssh(path_info) as ssh:
            for fname in ssh.walk_files(path_info.path):
                yield path_info.replace(path=fname)

    def remove(self, path_info):
        if path_info.scheme != self.scheme:
            raise NotImplementedError

        with self.ssh(path_info) as ssh:
            ssh.remove(path_info.path)

    def makedirs(self, path_info):
        with self.ssh(path_info) as ssh:
            ssh.makedirs(path_info.path)

    def move(self, from_info, to_info, mode=None):
        assert mode is None
        if from_info.scheme != self.scheme or to_info.scheme != self.scheme:
            raise NotImplementedError

        with self.ssh(from_info) as ssh:
            ssh.move(from_info.path, to_info.path)

    def copy(self, from_info, to_info):
        if not from_info.scheme == to_info.scheme == self.scheme:
            raise NotImplementedError

        with self.ssh(from_info) as ssh:
            ssh.atomic_copy(from_info.path, to_info.path)

    def symlink(self, from_info, to_info):
        if not from_info.scheme == to_info.scheme == self.scheme:
            raise NotImplementedError

        with self.ssh(from_info) as ssh:
            ssh.symlink(from_info.path, to_info.path)

    def hardlink(self, from_info, to_info):
        if not from_info.scheme == to_info.scheme == self.scheme:
            raise NotImplementedError

        # See dvc/remote/local/__init__.py - hardlink()
        if self.getsize(from_info) == 0:

            with self.ssh(to_info) as ssh:
                ssh.sftp.open(to_info.path, "w").close()

            logger.debug(
                "Created empty file: {src} -> {dest}".format(
                    src=str(from_info), dest=str(to_info)
                )
            )
            return

        with self.ssh(from_info) as ssh:
            ssh.hardlink(from_info.path, to_info.path)

    def reflink(self, from_info, to_info):
        if from_info.scheme != self.scheme or to_info.scheme != self.scheme:
            raise NotImplementedError

        with self.ssh(from_info) as ssh:
            ssh.reflink(from_info.path, to_info.path)

    def get_file_hash(self, path_info):
        if path_info.scheme != self.scheme:
            raise NotImplementedError

        with self.ssh(path_info) as ssh:
            return self.PARAM_CHECKSUM, ssh.md5(path_info.path)

    def getsize(self, path_info):
        with self.ssh(path_info) as ssh:
            return ssh.getsize(path_info.path)

    def _download(self, from_info, to_file, name=None, no_progress_bar=False):
        assert from_info.isin(self.path_info)
        with self.ssh(self.path_info) as ssh:
            ssh.download(
                from_info.path,
                to_file,
                progress_title=name,
                no_progress_bar=no_progress_bar,
            )

    def _upload(self, from_file, to_info, name=None, no_progress_bar=False):
        assert to_info.isin(self.path_info)
        with self.ssh(self.path_info) as ssh:
            ssh.upload(
                from_file,
                to_info.path,
                progress_title=name,
                no_progress_bar=no_progress_bar,
            )

    def list_paths(self, prefix=None, progress_callback=None):
        if prefix:
            root = posixpath.join(self.path_info.path, prefix[:2])
        else:
            root = self.path_info.path
        with self.ssh(self.path_info) as ssh:
            if prefix and not ssh.exists(root):
                return
            # If we simply return an iterator then with above closes instantly
            if progress_callback:
                for path in ssh.walk_files(root):
                    progress_callback()
                    yield path
            else:
                yield from ssh.walk_files(root)
