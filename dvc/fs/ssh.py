import getpass
import os.path
import threading

from funcy import cached_property, first, memoize, silent, wrap_prop, wrap_with

import dvc.prompt as prompt
from dvc.scheme import Schemes

from .fsspec_wrapper import CallbackMixin, FSSpecWrapper

_SSH_TIMEOUT = 60 * 30
_SSH_CONFIG_FILE = os.path.expanduser(os.path.join("~", ".ssh", "config"))


@wrap_with(threading.Lock())
@memoize
def ask_password(host, user, port):
    return prompt.password(
        "Enter a private key passphrase or a password for "
        "host '{host}' port '{port}' user '{user}'".format(
            host=host, port=port, user=user
        )
    )


# pylint:disable=abstract-method
class SSHFileSystem(CallbackMixin, FSSpecWrapper):
    scheme = Schemes.SSH
    REQUIRES = {"sshfs": "sshfs"}

    DEFAULT_PORT = 22
    PARAM_CHECKSUM = "md5"

    @staticmethod
    def _get_kwargs_from_urls(urlpath):
        from fsspec.implementations.sftp import SFTPFileSystem

        # pylint:disable=protected-access
        kwargs = SFTPFileSystem._get_kwargs_from_urls(urlpath)
        if "username" in kwargs:
            kwargs["user"] = kwargs.pop("username")
        return kwargs

    def _with_bucket(self, path):
        if isinstance(path, self.PATH_CLS):
            return path.path
        return super()._with_bucket(path)

    def _prepare_credentials(self, **config):
        self.CAN_TRAVERSE = True
        from sshfs.config import parse_config

        login_info = {}

        try:
            user_ssh_config = parse_config(host=config["host"])
        except FileNotFoundError:
            user_ssh_config = {}

        login_info["host"] = user_ssh_config.get("Hostname", config["host"])

        login_info["username"] = (
            config.get("user")
            or user_ssh_config.get("User")
            or getpass.getuser()
        )

        login_info["port"] = (
            config.get("port")
            or silent(int)(user_ssh_config.get("Port"))
            or self.DEFAULT_PORT
        )

        login_info["password"] = config.get("password")

        if user_ssh_config.get("IdentityFile"):
            config.setdefault(
                "keyfile", first(user_ssh_config.get("IdentityFile"))
            )

        login_info["client_keys"] = [config.get("keyfile")]
        login_info["timeout"] = config.get("timeout", _SSH_TIMEOUT)

        # These two settings fine tune the asyncssh to use the
        # fastest encryption algorithm and disable compression
        # altogether (since it blocking, it is slowing down
        # the transfers in a considerable rate, and even for
        # compressible data it is making it extremely slow).
        # See: https://github.com/ronf/asyncssh/issues/374
        login_info["encryption_algs"] = [
            "aes128-gcm@openssh.com",
            "aes256-ctr",
            "aes192-ctr",
            "aes128-ctr",
        ]
        login_info["compression_algs"] = None

        login_info["gss_auth"] = config.get("gss_auth", False)
        login_info["agent_forwarding"] = config.get("agent_forwarding", True)
        login_info["proxy_command"] = user_ssh_config.get("ProxyCommand")

        if config.get("ask_password") and login_info["password"] is None:
            login_info["password"] = ask_password(
                login_info["host"], login_info["username"], login_info["port"]
            )

        # We are going to automatically add stuff to known_hosts
        # something like paramiko's AutoAddPolicy()
        login_info["known_hosts"] = None
        return login_info

    @wrap_prop(threading.Lock())
    @cached_property
    def fs(self):
        from sshfs import SSHFileSystem as _SSHFileSystem

        return _SSHFileSystem(**self.fs_args)

    def _upload(
        self, from_file, to_info, name=None, no_progress_bar=False, **pbar_args
    ):
        # Ensure that if an interrupt happens during the transfer, we don't
        # pollute the cache.
        from dvc.utils import tmp_fname

        tmp_file = tmp_fname(to_info)
        try:
            super()._upload(
                from_file,
                tmp_file,
                name=name,
                no_progress_bar=no_progress_bar,
                **pbar_args
            )
        except BaseException:
            self.remove(tmp_file)
            raise
        else:
            self.move(tmp_file, to_info)
