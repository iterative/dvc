import threading

from funcy import cached_property, memoize, wrap_with

from dvc import prompt
from dvc.path_info import HTTPURLInfo
from dvc.scheme import Schemes

from .fsspec_wrapper import FSSpecWrapper, NoDirectoriesMixin


@wrap_with(threading.Lock())
@memoize
def ask_password(host, user):
    return prompt.password(
        "Enter a password for "
        "host '{host}' user '{user}'".format(host=host, user=user)
    )


def make_context(ssl_verify):
    if isinstance(ssl_verify, bool) or ssl_verify is None:
        return ssl_verify

    # If this is a path, then we will create an
    # SSL context for it, and load the given certificate.
    import ssl

    context = ssl.create_default_context()
    context.load_verify_locations(ssl_verify)
    return context


# pylint: disable=abstract-method
class HTTPFileSystem(NoDirectoriesMixin, FSSpecWrapper):
    scheme = Schemes.HTTP
    PATH_CLS = HTTPURLInfo
    PARAM_CHECKSUM = "checksum"
    REQUIRES = {"aiohttp": "aiohttp", "aiohttp-retry": "aiohttp_retry"}
    CAN_TRAVERSE = False

    SESSION_RETRIES = 5
    SESSION_BACKOFF_FACTOR = 0.1
    REQUEST_TIMEOUT = 60

    def _prepare_credentials(self, **config):
        import aiohttp
        from fsspec.asyn import fsspec_loop

        from dvc.config import ConfigError

        credentials = {}
        client_kwargs = credentials.setdefault("client_kwargs", {})

        if config.get("auth"):
            user = config.get("user")
            password = config.get("password")
            custom_auth_header = config.get("custom_auth_header")

            if password is None and config.get("ask_password"):
                password = ask_password(config.get("url"), user or "custom")

            auth_method = config["auth"]
            if auth_method == "basic":
                if user is None or password is None:
                    raise ConfigError(
                        "HTTP 'basic' authentication require both "
                        "'user' and 'password'"
                    )

                client_kwargs["auth"] = aiohttp.BasicAuth(user, password)
            elif auth_method == "custom":
                if custom_auth_header is None or password is None:
                    raise ConfigError(
                        "HTTP 'custom' authentication require both "
                        "'custom_auth_header' and 'password'"
                    )
                credentials["headers"] = {custom_auth_header: password}
            else:
                raise NotImplementedError(
                    f"Auth method {auth_method!r} is not supported."
                )

        if "ssl_verify" in config:
            with fsspec_loop():
                client_kwargs["connector"] = aiohttp.TCPConnector(
                    ssl=make_context(config["ssl_verify"])
                )

        credentials["get_client"] = self.get_client
        self.upload_method = config.get("method", "POST")
        return credentials

    async def get_client(self, **kwargs):
        import aiohttp
        from aiohttp_retry import ExponentialRetry, RetryClient

        kwargs["retry_options"] = ExponentialRetry(
            attempts=self.SESSION_RETRIES,
            factor=self.SESSION_BACKOFF_FACTOR,
            max_timeout=self.REQUEST_TIMEOUT,
        )

        # The default timeout for the aiohttp is 300 seconds
        # which is too low for DVC's interactions (especially
        # on the read) when dealing with large data blobs. We
        # unlimit the total time to read, and only limit the
        # time that is spent when connecting to the remote server.
        kwargs["timeout"] = aiohttp.ClientTimeout(
            total=None,
            connect=self.REQUEST_TIMEOUT,
            sock_connect=self.REQUEST_TIMEOUT,
            sock_read=None,
        )

        return RetryClient(**kwargs)

    @cached_property
    def fs(self):
        from fsspec.implementations.http import (
            HTTPFileSystem as _HTTPFileSystem,
        )

        return _HTTPFileSystem(**self.fs_args)

    def _entry_hook(self, entry):
        entry["checksum"] = entry.get("ETag") or entry.get("Content-MD5")
        return entry
