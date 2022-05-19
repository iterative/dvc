import getpass
import os.path
import threading

from funcy import cached_property, memoize, silent, wrap_prop, wrap_with

from ..base import FileSystem
from ..callbacks import DEFAULT_CALLBACK
from ..utils import as_atomic

DEFAULT_PORT = 22


@wrap_with(threading.Lock())
@memoize
def ask_password(host, user, port):
    return getpass.getpass(
        "Enter a private key passphrase or a password for "
        f"host '{host}' port '{port}' user '{user}':\n"
    )


# pylint:disable=abstract-method
class SSHFileSystem(FileSystem):
    protocol = "ssh"
    REQUIRES = {"sshfs": "sshfs"}
    PARAM_CHECKSUM = "md5"

    @classmethod
    def _strip_protocol(cls, path: str) -> str:
        from fsspec.utils import infer_storage_options

        return infer_storage_options(path)["path"]

    def unstrip_protocol(self, path: str) -> str:
        host = self.fs_args["host"]
        port = self.fs_args["port"]
        path = path.lstrip("/")
        return f"ssh://{host}:{port}/{path}"

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
            or config.get("username")
            or user_ssh_config.get("User")
            or getpass.getuser()
        )
        login_info["port"] = (
            config.get("port")
            or silent(int)(user_ssh_config.get("Port"))
            or DEFAULT_PORT
        )

        if config.get("ask_password") and config.get("password") is None:
            config["password"] = ask_password(
                login_info["host"], login_info["username"], login_info["port"]
            )

        login_info["password"] = config.get("password")
        login_info["passphrase"] = config.get("password")

        raw_keys = []
        if config.get("keyfile"):
            raw_keys.append(config.get("keyfile"))
        elif user_ssh_config.get("IdentityFile"):
            raw_keys.extend(user_ssh_config.get("IdentityFile"))

        if raw_keys:
            login_info["client_keys"] = [
                os.path.expanduser(key) for key in raw_keys
            ]

        login_info["timeout"] = config.get("timeout", 1800)

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

        # We are going to automatically add stuff to known_hosts
        # something like paramiko's AutoAddPolicy()
        login_info["known_hosts"] = None
        return login_info

    @wrap_prop(threading.Lock())
    @cached_property
    def fs(self):
        from sshfs import SSHFileSystem as _SSHFileSystem

        return _SSHFileSystem(**self.fs_args)

    # Ensure that if an interrupt happens during the transfer, we don't
    # pollute the cache.

    def upload_fobj(self, fobj, to_info, **kwargs):
        with as_atomic(self, to_info) as tmp_file:
            super().upload_fobj(fobj, tmp_file, **kwargs)

    def put_file(
        self,
        from_file,
        to_info,
        callback=DEFAULT_CALLBACK,
        size=None,
        **kwargs,
    ):
        with as_atomic(self, to_info) as tmp_file:
            super().put_file(
                from_file, tmp_file, callback=callback, size=size, **kwargs
            )
