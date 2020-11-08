import logging
import os
import threading

from funcy import cached_property, wrap_prop

from dvc.exceptions import DvcException
from dvc.hash_info import HashInfo
from dvc.path_info import CloudURLInfo
from dvc.progress import Tqdm
from dvc.scheme import Schemes

from .base import BaseTree

logger = logging.getLogger(__name__)


class OSFAuthError(DvcException):
    def __init__(self):
        message = (
            "OSF authorization failed. Please check or provide a password."
            " It is also possible that you do not have access"
            " to a private project."
        )
        super().__init__(message)


class OSFTree(BaseTree):
    scheme = Schemes.OSF
    REQUIRES = {"ofsclient": "osfclient"}
    PARAM_CHECKSUM = "md5"
    PATH_CLS = CloudURLInfo

    def __init__(self, repo, config):
        super().__init__(repo, config)

        self.path_info = self.PATH_CLS(config["url"])

        self.osf_username = config.get("osf_username")
        self.project = config.get("project")
        self.password = os.getenv(
            "OSF_PASSWORD", None
        )  # need for private projects
        if self.password is None:
            self.password = config.get("password")
        logger.debug(OSFTree)

    @wrap_prop(threading.Lock())
    @cached_property
    def storage(self):
        import osfclient

        osf = osfclient.OSF()
        osf.login(self.osf_username, self.password)
        try:
            storage = osf.project(self.project).storage()
        except osfclient.exceptions.UnauthorizedException:
            raise OSFAuthError
        return storage

    def _get_file_obj(self, path_info):
        for file in self.storage.files:
            if file.path == path_info.path:
                return file

    def get_md5(self, path_info):
        file = self._get_file_obj(path_info)
        md5 = file.hashes.get("md5")
        return md5

    def exists(self, path_info, use_dvcignore=True):
        paths = self.list_paths()
        return any(path_info.path == path for path in paths)

    def _list_paths(self):
        for file in self.storage.files:
            yield file.path

    def walk_files(self, path_info, **kwargs):
        for fname in self._list_paths():
            if fname.endswith("/"):
                continue

            yield path_info.replace(path=fname)

    def remove(self, path_info):
        if path_info.scheme != self.scheme:
            raise NotImplementedError

        file = self._get_file_obj(path_info)
        file.remove()
        logger.debug(f"Removing {path_info}")

    def get_file_hash(self, path_info):
        return HashInfo(self.PARAM_CHECKSUM, self.get_md5(path_info))

    def _download(self, from_info, to_file, **_kwargs):
        file = self._get_file_obj(from_info)
        # total = file._get_attribute(file, "attributes", "self", "size")
        with open(to_file, "wb") as fobj:
            file.write_to(fobj)

    def _upload(
        self, from_file, to_info, name=None, no_progress_bar=False, **_kwargs
    ):
        total = os.path.getsize(from_file)
        with open(from_file, "rb") as fobj:
            with Tqdm.wrapattr(
                fobj, "read", desc=name, total=total, disable=no_progress_bar
            ) as wrapped:
                self.storage.create_file(
                    to_info.path, wrapped, force=False, update=False
                )
