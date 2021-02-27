import logging
import os.path
import threading

from funcy import cached_property, memoize, wrap_prop, wrap_with

import dvc.prompt as prompt
from dvc.exceptions import DvcException, HTTPError
from dvc.path_info import Nexus3UnsecureURLInfo, Nexus3URLInfo, PathInfo
from dvc.progress import Tqdm
from dvc.scheme import Schemes

from .base import BaseFileSystem

logger = logging.getLogger(__name__)


@wrap_with(threading.Lock())
@memoize
def ask_password(host, user):
    return prompt.password(
        "Enter a password for "
        "host '{host}' user '{user}'".format(host=host, user=user)
    )


class Nexus3FileSystem(BaseFileSystem):  # pylint:disable=abstract-method
    scheme = Schemes.NEXUS3
    PATH_CLS = Nexus3URLInfo

    PARAM_CHECKSUM = "md5"
    SESSION_RETRIES = 5
    SESSION_BACKOFF_FACTOR = 0.1
    REQUEST_TIMEOUT = 60

    def __init__(self, repo, config):
        super().__init__(repo, config)

        url = config.get("url")
        if url:
            # allow the usage of an unsecure http storage, if 'unsecure' is
            # set in the config
            # used for example for local testing with a real nexus client
            self.unsecure = config.get("unsecure", False)
            if self.unsecure:
                self.PATH_CLS = Nexus3UnsecureURLInfo
            self.path_info = self.PATH_CLS(url)
            (
                self.hostname,
                self.repository,
                self.directory,
                _,
            ) = self.extract_nexus_repo_info_from_url(self.path_info)
            self.url = self.path_info.url
            self.user = config.get("user", None)
        else:
            self.path_info = None

        self.auth = config.get("auth", None)
        self.password = config.get("password") or os.getenv("NEXUS3_PASSWORD")
        self.ask_password = config.get("ask_password", False)
        self.headers = {}
        self.ssl_verify = config.get("ssl_verify", True)

    def _auth_method(self):
        from requests.auth import HTTPBasicAuth

        if self.auth:
            if self.ask_password and self.password is None:
                self.password = ask_password(self.hostname, self.user)
            if self.auth == "basic":
                return HTTPBasicAuth(self.user, self.password)
        return None

    def _generate_download_url(self, path_info):
        """
        This method is used to construct a authorized download URL if an
        authentication is used.

        Hereby Nexus3 repositories are supported, which prohibit public user
        access.
        The `dvc/utils/http.py:iter_url` method doesn't use custom
        authentication headers when downloading files yet.
        """
        auth_method = self._auth_method()
        if auth_method:
            parsed_url = self.PATH_CLS(path_info)
            return "{}://{}:{}@{}{}".format(
                parsed_url.scheme,
                auth_method.username,
                auth_method.password,
                parsed_url.netloc,
                parsed_url._spath,  # pylint: disable=protected-access
            )
        return path_info.url

    @wrap_prop(threading.Lock())
    @cached_property
    def _session(self):
        import requests
        from requests.adapters import HTTPAdapter
        from urllib3.util.retry import Retry

        session = requests.Session()

        session.verify = self.ssl_verify

        retries = Retry(
            total=self.SESSION_RETRIES,
            backoff_factor=self.SESSION_BACKOFF_FACTOR,
        )

        session.mount("http://", HTTPAdapter(max_retries=retries))
        session.mount("https://", HTTPAdapter(max_retries=retries))

        return session

    def request(self, method, url, **kwargs):
        import requests

        kwargs.setdefault("allow_redirects", True)
        kwargs.setdefault("timeout", self.REQUEST_TIMEOUT)

        try:
            headers = self.headers
            if "headers" in kwargs:
                headers = kwargs.pop("headers")
                headers.update(self.headers)
            res = self._session.request(
                method,
                url,
                auth=self._auth_method(),
                headers=headers,
                **kwargs,
            )

            redirect_no_location = (
                kwargs["allow_redirects"]
                and res.status_code in (301, 302)
                and "location" not in res.headers
            )

            if redirect_no_location:
                raise requests.exceptions.RequestException

            return res

        except requests.exceptions.RequestException:
            raise DvcException(f"could not perform a {method} request")

    def _head(self, url):
        return self.request("HEAD", url)

    def exists(self, path_info, use_dvcignore=True):
        res = self._head(path_info.url)
        if res.status_code == 404:
            return False
        if bool(res):
            return True
        raise HTTPError(res.status_code, res.reason)

    def info(self, path_info):
        size = self._content_length(self._head(path_info.url))
        checksum = self._checksum(self._get_full_item_info(path_info))
        return {"size": size, self.PARAM_CHECKSUM: checksum}

    def _upload_fobj(self, fobj, to_info):
        _, _, directory, filename = self.extract_nexus_repo_info_from_url(
            to_info
        )
        assert directory

        # streaming multipart 'form-data objects'
        # Nexus uses a 'form' for file submission
        from requests_toolbelt import MultipartEncoder

        multipart_encoder = MultipartEncoder(
            fields={
                "raw.directory": (None, directory),
                "raw.asset1.filename": (None, filename),
                "raw.asset1": (filename, fobj),
            }
        )
        params = {"repository": self.repository}
        api_path = f"{self.hostname}/service/rest/v1/components"
        response = self.request(
            "POST",
            api_path,
            data=multipart_encoder,
            params=params,
            headers={"Content-Type": multipart_encoder.content_type},
        )

        if response.status_code != 204:
            raise HTTPError(response.status_code, response.reason)

    def _download(self, from_info, to_file, name=None, no_progress_bar=False):
        response = self.request("GET", from_info.url, stream=True)
        if response.status_code != 200:
            raise HTTPError(response.status_code, response.reason)
        with open(to_file, "wb") as fd:
            with Tqdm.wrapattr(
                fd,
                "write",
                total=None
                if no_progress_bar
                else self._content_length(response),
                leave=False,
                desc=from_info.url if name is None else name,
                disable=no_progress_bar,
            ) as fd_wrapped:
                for chunk in response.iter_content(chunk_size=self.CHUNK_SIZE):
                    fd_wrapped.write(chunk)

    def _upload(
        self, from_file, to_info, name=None, no_progress_bar=False, **_kwargs
    ):
        with open(from_file, "rb") as fobj:
            self.upload_fobj(
                fobj,
                to_info,
                no_progress_bar=no_progress_bar,
                desc=name or to_info.url,
                total=None if no_progress_bar else os.path.getsize(from_file),
            )

    def _search_for_items(self, params):
        api_path = f"{self.hostname}/service/rest/v1/search"
        response = self.request("GET", url=api_path, params=params)
        if not response.ok:
            raise HTTPError(response.status_code, response.reason)
        return response.json().get("items")

    def _get_full_item_info(self, path_info):
        params = {
            "repository": self.repository,
            "group": f"/{self.directory}",
            "name": f"{self.directory}/{path_info.name}",
        }

        found_item = self._search_for_items(params)
        if len(found_item) > 0:
            return found_item[0]
        return None

    def _get_item_id(self, path_info):
        full_item_info = self._get_full_item_info(path_info)
        return full_item_info[0]["id"] if full_item_info else None

    def remove(self, path_info):
        if isinstance(path_info, PathInfo):
            if path_info.scheme != "nexus3":
                raise NotImplementedError

        item_id = self._get_item_id(path_info)
        if item_id:
            api_path = f"{self.hostname}/service/rest/v1/components/{item_id}"
            response = self.request("DELETE", url=api_path)
            if response.status_code != 204:
                raise HTTPError(response.status_code, response.reason)

    def ls(
        self, path_info, detail=False, recursive=False
    ):  # pylint: disable=arguments-differ
        _, _, directory, _ = self.extract_nexus_repo_info_from_url(path_info)

        params = {
            "repository": self.repository,
            "group": f"/{directory}",
            # Nexus needs a "/" in front of group/directory
        }
        if recursive:
            params.update({"group": f"{params['group']}*"})

        found_item = self._search_for_items(params)
        for obj_summary in found_item:
            item_path = obj_summary["name"]
            if detail:
                yield {
                    "type": "file",
                    "name": item_path,
                    "checksum": self._checksum(obj_summary),
                }
            else:
                yield item_path

    def walk_files(self, path_info, **kwargs):
        for item_path in self.ls(path_info, recursive=True):
            yield path_info.replace(path=f"{path_info.path}{item_path}")

    @staticmethod
    def extract_nexus_repo_info_from_url(url_info):
        """
        Extracts all the information to interact with the Nexus3 API from a
        given url
        """
        if "repository/" not in url_info.path:
            raise DvcException(
                "Please specify a repository in your Nexus3 url"
            )
        hostname = url_info.replace(path="").url
        # extract the part after "/repository/"
        repository = url_info.path[len("/repository/") :].split("/")[0]
        directory_and_filename = url_info.path[
            len(f"/repository/{repository}/") :
        ]
        # a directory MUST be present for Nexus3 to work, if only a single
        # string is left -> it is a directory
        filename = (
            directory_and_filename.split("/")[-1]
            if "/" in directory_and_filename
            else ""
        )
        # if no filename present, use whole directory_and_filename str as a
        # directory
        directory = (
            directory_and_filename
            if len(filename) == 0
            else directory_and_filename[: -len(filename)]
        )
        directory = directory.rstrip("/")  # convention
        if len(directory) == 0:
            raise DvcException("Please specify a folder in your Nexus3 url")
        return hostname, repository, directory, filename

    @staticmethod
    def _content_length(response):
        res = response.headers.get("Content-Length")
        return int(res) if res else None

    @staticmethod
    def _checksum(obj_summary):
        return (
            obj_summary["assets"][0]["checksum"]["md5"]
            if obj_summary
            else None
        )
