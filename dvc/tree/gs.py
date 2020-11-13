import logging
import os.path
import threading
from datetime import timedelta
from functools import wraps

from funcy import cached_property, wrap_prop

from dvc.exceptions import DvcException
from dvc.hash_info import HashInfo
from dvc.path_info import CloudURLInfo
from dvc.progress import Tqdm
from dvc.scheme import Schemes

from .base import BaseTree

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
def _upload_to_bucket(
    bucket,
    from_file,
    to_info,
    chunk_size=None,
    name=None,
    no_progress_bar=False,
):
    blob = bucket.blob(to_info.path, chunk_size=chunk_size)
    with open(from_file, mode="rb") as fobj:
        with Tqdm.wrapattr(
            fobj,
            "read",
            desc=name or to_info.path,
            total=os.path.getsize(from_file),
            disable=no_progress_bar,
        ) as wrapped:
            blob.upload_from_file(wrapped)


class GSTree(BaseTree):
    scheme = Schemes.GS
    PATH_CLS = CloudURLInfo
    REQUIRES = {"google-cloud-storage": "google.cloud.storage"}
    PARAM_CHECKSUM = "md5"

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

    def get_file_hash(self, path_info):
        import base64
        import codecs

        bucket = path_info.bucket
        path = path_info.path
        blob = self.gs.bucket(bucket).get_blob(path)
        if not blob:
            return HashInfo(self.PARAM_CHECKSUM, None)

        b64_md5 = blob.md5_hash
        md5 = base64.b64decode(b64_md5)
        return HashInfo(
            self.PARAM_CHECKSUM,
            codecs.getencoder("hex")(md5)[0].decode("utf-8"),
            size=blob.size,
        )

    def _upload(
        self, from_file, to_info, name=None, no_progress_bar=False, **_kwargs
    ):
        bucket = self.gs.bucket(to_info.bucket)
        _upload_to_bucket(
            bucket,
            from_file,
            to_info,
            name=name,
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
