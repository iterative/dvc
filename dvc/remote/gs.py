from __future__ import unicode_literals

import logging
from datetime import timedelta
from functools import wraps

from funcy import cached_property

from dvc.utils.compat import FileNotFoundError  # skipcq: PYL-W0622
from dvc.remote.base import RemoteBASE
from dvc.config import Config
from dvc.exceptions import DvcException
from dvc.path_info import CloudURLInfo
from dvc.scheme import Schemes

logger = logging.getLogger(__name__)


def dynamic_chunk_size(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        import requests
        from google.cloud.storage.blob import Blob, _DEFAULT_CHUNKSIZE

        # Default chunk size for gs is 100M, which might be too much for
        # particular network (see [1]). So if we are getting ConnectionError,
        # we should try lowering the chunk size until we reach the minimum
        # allowed chunk size of 256K. Also note that `chunk_size` must be a
        # multiple of 256K per the API specification.
        #
        # [1] https://github.com/iterative/dvc/issues/2572

        # skipcq: PYL-W0212
        multiplier = int(_DEFAULT_CHUNKSIZE / Blob._CHUNK_SIZE_MULTIPLE)
        while True:
            try:
                # skipcq: PYL-W0212
                chunk_size = Blob._CHUNK_SIZE_MULTIPLE * multiplier
                return func(*args, chunk_size=chunk_size, **kwargs)
            except requests.exceptions.ConnectionError:
                multiplier = int(multiplier / 2)
                if not multiplier:
                    raise

    return wrapper


@dynamic_chunk_size
def _upload_to_bucket(bucket, from_file, to_info, **kwargs):
    blob = bucket.blob(to_info.path, **kwargs)
    blob.upload_from_filename(from_file)


class RemoteGS(RemoteBASE):
    scheme = Schemes.GS
    path_cls = CloudURLInfo
    REQUIRES = {"google-cloud-storage": "google.cloud.storage"}
    PARAM_CHECKSUM = "md5"

    def __init__(self, repo, config):
        super(RemoteGS, self).__init__(repo, config)

        storagepath = "gs://" + config.get(Config.SECTION_GCP_STORAGEPATH, "/")
        url = config.get(Config.SECTION_REMOTE_URL, storagepath)
        self.path_info = self.path_cls(url)

        self.projectname = config.get(Config.SECTION_GCP_PROJECTNAME, None)
        self.credentialpath = config.get(Config.SECTION_GCP_CREDENTIALPATH)

    @cached_property
    def gs(self):
        from google.cloud.storage import Client

        return (
            Client.from_service_account_json(self.credentialpath)
            if self.credentialpath
            else Client(self.projectname)
        )

    def get_file_checksum(self, path_info):
        import base64
        import codecs

        bucket = path_info.bucket
        path = path_info.path
        blob = self.gs.bucket(bucket).get_blob(path)
        if not blob:
            return None

        b64_md5 = blob.md5_hash
        md5 = base64.b64decode(b64_md5)
        return codecs.getencoder("hex")(md5)[0].decode("utf-8")

    def copy(self, from_info, to_info):
        from_bucket = self.gs.bucket(from_info.bucket)
        blob = from_bucket.get_blob(from_info.path)
        if not blob:
            msg = "'{}' doesn't exist in the cloud".format(from_info.path)
            raise DvcException(msg)

        to_bucket = self.gs.bucket(to_info.bucket)
        from_bucket.copy_blob(blob, to_bucket, new_name=to_info.path)

    def remove(self, path_info):
        if path_info.scheme != "gs":
            raise NotImplementedError

        logger.debug("Removing gs://{}".format(path_info))
        blob = self.gs.bucket(path_info.bucket).get_blob(path_info.path)
        if not blob:
            return

        blob.delete()

    def _list_paths(self, bucket, prefix):
        for blob in self.gs.bucket(bucket).list_blobs(prefix=prefix):
            yield blob.name

    def list_cache_paths(self):
        return self._list_paths(self.path_info.bucket, self.path_info.path)

    def exists(self, path_info):
        paths = set(self._list_paths(path_info.bucket, path_info.path))
        return any(path_info.path == path for path in paths)

    def _upload(self, from_file, to_info, **_kwargs):
        bucket = self.gs.bucket(to_info.bucket)
        _upload_to_bucket(bucket, from_file, to_info)

    def _download(self, from_info, to_file, **_kwargs):
        bucket = self.gs.bucket(from_info.bucket)
        blob = bucket.get_blob(from_info.path)
        blob.download_to_filename(to_file)

    def _generate_download_url(self, path_info, expires=3600):
        expiration = timedelta(seconds=int(expires))

        bucket = self.gs.bucket(path_info.bucket)
        blob = bucket.get_blob(path_info.path)
        if blob is None:
            raise FileNotFoundError
        return blob.generate_signed_url(expiration=expiration)
