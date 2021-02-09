import logging
import os
import threading

from funcy import cached_property, wrap_prop

from dvc.exceptions import DvcException
from dvc.hash_info import HashInfo
from dvc.path_info import URLInfo
from dvc.progress import Tqdm
from dvc.scheme import Schemes
from dvc.tree.base import RemoteActionNotImplemented
from dvc.utils import format_link

from .base import BaseTree

logger = logging.getLogger(__name__)


class OSFAuthError(DvcException):
    def __init__(self):
        message = (
            f"OSF authorization failed. Please check or provide"
            " user and password. See "
            f"{format_link('https://man.dvc.org/remote/modify')}"
            " for details"
        )
        super().__init__(message)


class OSFTree(BaseTree):
    scheme = Schemes.OSF
    REQUIRES = {"ofsclient": "osfclient"}
    PARAM_CHECKSUM = "md5"
    PATH_CLS = URLInfo

    def __init__(self, repo, config):
        super().__init__(repo, config)

        url = config.get("url", "osf://")
        self.path_info = self.PATH_CLS(url)

        self.user = os.getenv("OSF_USER", config.get("user"))
        self.project_guid = os.getenv("OSF_PROJECT", config.get("project"))
        self.password = os.getenv("OSF_PASSWORD", config.get("password"))

        if (
            self.user is None
            or self.password is None
            or self.project_guid is None
        ):
            raise DvcException(
                f"Empty OSF user, project or password."
                f" Learn more at "
                f"{format_link('https://man.dvc.org/remote/modify')}"
            )

    @wrap_prop(threading.Lock())
    @cached_property
    def osf(self):
        import osfclient

        osf = osfclient.OSF()
        return osf

    @wrap_prop(threading.Lock())
    @cached_property
    def project(self):
        from osfclient.exceptions import UnauthorizedException

        osf = self.osf
        osf.login(self.user, self.password)
        try:
            project = osf.project(self.project_guid)
        except UnauthorizedException as e:
            raise OSFAuthError from e
        return project

    @wrap_prop(threading.Lock())
    @cached_property
    def storage(self):
        return self.project.storage()

    def _get_file_obj(self, path_info):
        folder_names = path_info.path.split("/")[1:-1]
        file_name = path_info.path.split("/")[-1]
        try:
            folders = self.storage.folders
            for name in folder_names:
                try:
                    item = next((i for i in folders if i.name == name))
                except StopIteration:
                    return None
                folders = item.folders
            file = next((i for i in item.files if i.name == file_name), None)
            return file
        except RuntimeError as e:
            message = (
                f"OSF API error. {str(e)} See "
                f"https://developer.osf.io/#tag/Errors-and-Error-Codes"
                f" for details."
            )
            raise DvcException(message) from e

    def get_md5(self, path_info):
        file = self._get_file_obj(path_info)
        md5 = file.hashes.get("md5")
        return md5

    def exists(self, path_info, use_dvcignore=True):
        return self._get_file_obj(path_info) is not None

    def _list_paths(self):
        try:
            for file in self.storage.files:
                yield file.path
        except RuntimeError as e:
            message = (
                f"OSF API error. {str(e)} See "
                f"https://developer.osf.io/#tag/Errors-and-Error-Codes"
                f" for details."
            )
            raise DvcException(message) from e

    def isdir(self, path_info):
        file = self._get_file_obj(path_info)
        return file.path.endswith("/")

    def walk_files(self, path_info, **kwargs):
        for fname in self._list_paths():
            if fname.endswith("/"):
                continue

            yield path_info.replace(path=fname)

    def remove(self, path_info):
        if path_info.scheme != self.scheme:
            raise NotImplementedError

        file = self._get_file_obj(path_info)
        try:
            file.remove()
        except RuntimeError as e:
            message = (
                f"OSF API error. {str(e)} See "
                f"https://developer.osf.io/#tag/Errors-and-Error-Codes"
                f" for details."
            )
            raise DvcException(message) from e
        logger.debug(f"Removing {path_info}")

    def get_file_hash(self, path_info):
        return HashInfo(self.PARAM_CHECKSUM, self.get_md5(path_info))

    def _download(
        self, from_info, to_file, name=None, no_progress_bar=False, **_kwargs
    ):
        file = self._get_file_obj(from_info)
        total = file.size

        with open(to_file, "wb") as fobj:
            with Tqdm.wrapattr(
                fobj, "write", desc=name, total=total, disable=no_progress_bar
            ) as wrapped:
                try:
                    file.write_to(wrapped)
                except RuntimeError as e:
                    message = (
                        f"OSF API error. {str(e)} See "
                        f"https://developer.osf.io/#tag/Errors-and-Error-Codes"
                        f" for details."
                    )
                    raise DvcException(message) from e

    def _upload(
        self, from_file, to_info, name=None, no_progress_bar=False, **_kwargs
    ):
        total = os.path.getsize(from_file)
        with open(from_file, "rb") as fobj:
            with Tqdm.wrapattr(
                fobj, "read", desc=name, total=total, disable=no_progress_bar
            ) as wrapped:
                try:
                    self.storage.create_file(
                        to_info.path, wrapped, force=False, update=False
                    )
                except RuntimeError as e:
                    message = (
                        f"OSF API error. {str(e)} See "
                        f"https://developer.osf.io/#tag/Errors-and-Error-Codes"
                        f" for details."
                    )
                    raise DvcException(message) from e

    def open(self, path_info, mode="r", encoding: str = None, **kwargs):
        raise RemoteActionNotImplemented("open", self.scheme)
