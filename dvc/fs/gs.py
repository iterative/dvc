import logging
import os.path
import threading
from datetime import timedelta
from functools import wraps

from funcy import cached_property, wrap_prop

from dvc.exceptions import DvcException
from dvc.path_info import CloudURLInfo
from dvc.progress import Tqdm
from dvc.scheme import Schemes

from .base import BaseFileSystem

logger = logging.getLogger(__name__)


def dynamic_chunk_size(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        import requests
        from google.cloud.storage.blob import Blob

        # `ConnectionError` may be due to too large `chunk_size`
        # (see [#2572]) so try halving on error.
        # Note: start with 40 * [default: 256K] = 10M.
        # Note: must be multiple of 256K.
        #
        # [#2572]: https://github.com/iterative/dvc/issues/2572
        # skipcq: PYL-W0212
        multiplier = 40
        while True:
            try:
                # skipcq: PYL-W0212
                # pylint: disable=protected-access
                chunk_size = Blob._CHUNK_SIZE_MULTIPLE * multiplier
                return func(*args, chunk_size=chunk_size, **kwargs)
            except requests.exceptions.ConnectionError:
                multiplier //= 2
                if not multiplier:
                    raise

    return wrapper


@dynamic_chunk_size
def _upload_to_bucket(bucket, fobj, to_info, chunk_size=None):
    blob = bucket.blob(to_info.path, chunk_size=chunk_size)
    blob.upload_from_file(fobj)


class GSFileSystem(BaseFileSystem):
    scheme = Schemes.GS
    PATH_CLS = CloudURLInfo
    REQUIRES = {"google-cloud-storage": "google.cloud.storage"}
    PARAM_CHECKSUM = "md5"
    DETAIL_FIELDS = frozenset(("md5", "size"))

    def __init__(self, repo, config):
        super().__init__(repo, config)

        url = config.get("url", "gs:///")
        self.path_info = self.PATH_CLS(url)

        self.projectname = config.get("projectname", None)
        self.credentialpath = config.get("credentialpath")

    @wrap_prop(threading.Lock())
    @cached_property
    def gs(self):
        from google.cloud.storage import Client

        return (
            Client.from_service_account_json(self.credentialpath)
            if self.credentialpath
            else Client(self.projectname)
        )

    def _generate_download_url(self, path_info, expires=3600):
        import google.auth
        from google.auth import compute_engine

        expiration = timedelta(seconds=int(expires))

        bucket = self.gs.bucket(path_info.bucket)
        blob = bucket.get_blob(path_info.path)
        if blob is None:
            raise FileNotFoundError

        if isinstance(
            blob.client._credentials,  # pylint: disable=protected-access
            google.auth.credentials.Signing,
        ):
            # sign if we're able to sign with credentials.
            return blob.generate_signed_url(expiration=expiration)

        auth_request = google.auth.transport.requests.Request()
        # create signing credentials with the default credentials
        # for use with Compute Engine and other environments where
        # Client credentials cannot sign.
        signing_credentials = compute_engine.IDTokenCredentials(
            auth_request, ""
        )
        return signing_credentials.signer.sign(blob)

    def exists(self, path_info, use_dvcignore=True):
        """Check if the blob exists. If it does not exist,
        it could be a part of a directory path.

        eg: if `data/file.txt` exists, check for `data` should return True
        """
        return self.isfile(path_info) or self.isdir(path_info)

    def isdir(self, path_info):
        dir_path = path_info / ""
        return bool(list(self._list_paths(dir_path, max_items=1)))

    def isfile(self, path_info):
        if path_info.path.endswith("/"):
            return False

        blob = self.gs.bucket(path_info.bucket).blob(path_info.path)
        return blob.exists()

    def info(self, path_info):
        bucket = self.gs.bucket(path_info.bucket)
        blob = bucket.get_blob(path_info.path)
        return {"type": "file", "size": blob.size, "etag": blob.etag}

    def _list_paths(self, path_info, max_items=None):
        for blob in self.gs.bucket(path_info.bucket).list_blobs(
            prefix=path_info.path, max_results=max_items
        ):
            yield blob.name

    def walk_files(self, path_info, **kwargs):
        if not kwargs.pop("prefix", False):
            path_info = path_info / ""
        for fname in self._list_paths(path_info, **kwargs):
            # skip nested empty directories
            if fname.endswith("/"):
                continue
            yield path_info.replace(fname)

    def ls(
        self, path_info, detail=False, recursive=False
    ):  # pylint: disable=arguments-differ
        import base64

        assert recursive

        for blob in self.gs.bucket(path_info.bucket).list_blobs(
            prefix=path_info.path
        ):
            if detail:
                md5_hash = base64.b64decode(blob.md5_hash)
                yield {
                    "type": "file",
                    "name": blob.name,
                    "md5": md5_hash.hex(),
                    "size": blob.size,
                    "etag": blob.etag,
                }
            else:
                yield blob.name

    def remove(self, path_info):
        if path_info.scheme != "gs":
            raise NotImplementedError

        logger.debug(f"Removing gs://{path_info}")
        blob = self.gs.bucket(path_info.bucket).get_blob(path_info.path)
        if not blob:
            return

        blob.delete()

    def makedirs(self, path_info):
        if not path_info.path:
            return

        self.gs.bucket(path_info.bucket).blob(
            (path_info / "").path
        ).upload_from_string("")

    def copy(self, from_info, to_info):
        from_bucket = self.gs.bucket(from_info.bucket)
        blob = from_bucket.get_blob(from_info.path)
        if not blob:
            msg = f"'{from_info.path}' doesn't exist in the cloud"
            raise DvcException(msg)

        to_bucket = self.gs.bucket(to_info.bucket)
        from_bucket.copy_blob(blob, to_bucket, new_name=to_info.path)

    def _upload_fobj(self, fobj, to_info):
        bucket = self.gs.bucket(to_info.bucket)
        # With other references being given in the @dynamic_chunk_size
        # this function does not respect fs.CHUNK_SIZE, since it is
        # too big for GS to handle. Rather it dynamically calculates the
        # best possible chunk size
        _upload_to_bucket(bucket, fobj, to_info)

    def _upload(
        self, from_file, to_info, name=None, no_progress_bar=False, **_kwargs
    ):
        with open(from_file, mode="rb") as fobj:
            self.upload_fobj(
                fobj,
                to_info,
                desc=name or to_info.path,
                total=os.path.getsize(from_file),
                no_progress_bar=no_progress_bar,
            )

    def _download(self, from_info, to_file, name=None, no_progress_bar=False):
        bucket = self.gs.bucket(from_info.bucket)
        blob = bucket.get_blob(from_info.path)
        with open(to_file, mode="wb") as fobj:
            with Tqdm.wrapattr(
                fobj,
                "write",
                desc=name or from_info.path,
                total=blob.size,
                disable=no_progress_bar,
            ) as wrapped:
                blob.download_to_file(wrapped)
