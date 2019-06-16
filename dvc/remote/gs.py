from __future__ import unicode_literals

import logging
import itertools
from contextlib import contextmanager

try:
    from google.cloud import storage
except ImportError:
    storage = None

from dvc.remote.base import RemoteBASE
from dvc.config import Config
from dvc.exceptions import DvcException
from dvc.path_info import CloudURLInfo
from dvc.scheme import Schemes


logger = logging.getLogger(__name__)


class RemoteGS(RemoteBASE):
    scheme = Schemes.GS
    path_cls = CloudURLInfo
    REQUIRES = {"google.cloud.storage": storage}
    PARAM_CHECKSUM = "md5"

    def __init__(self, repo, config):
        super(RemoteGS, self).__init__(repo, config)

        storagepath = "gs://" + config.get(Config.SECTION_GCP_STORAGEPATH, "/")
        url = config.get(Config.SECTION_REMOTE_URL, storagepath)
        self.path_info = self.path_cls(url)

        self.projectname = config.get(Config.SECTION_GCP_PROJECTNAME, None)
        self.credentialpath = config.get(Config.SECTION_GCP_CREDENTIALPATH)

    @staticmethod
    def compat_config(config):
        ret = config.copy()
        url = "gs://" + ret.pop(Config.SECTION_GCP_STORAGEPATH, "").lstrip("/")
        ret[Config.SECTION_REMOTE_URL] = url
        return ret

    @property
    def gs(self):
        return (
            storage.Client.from_service_account_json(self.credentialpath)
            if self.credentialpath
            else storage.Client(self.projectname)
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

    def copy(self, from_info, to_info, ctx=None):
        gs = ctx or self.gs

        from_bucket = gs.bucket(from_info.bucket)
        blob = from_bucket.get_blob(from_info.path)
        if not blob:
            msg = "'{}' doesn't exist in the cloud".format(from_info.path)
            raise DvcException(msg)

        to_bucket = gs.bucket(to_info.bucket)
        from_bucket.copy_blob(blob, to_bucket, new_name=to_info.path)

    def remove(self, path_info):
        if path_info.scheme != "gs":
            raise NotImplementedError

        logger.debug("Removing gs://{}".format(path_info))
        blob = self.gs.bucket(path_info.bucket).get_blob(path_info.path)
        if not blob:
            return

        blob.delete()

    def _list_paths(self, bucket, prefix, gs=None):
        gs = gs or self.gs

        for blob in gs.bucket(bucket).list_blobs(prefix=prefix):
            yield blob.name

    def list_cache_paths(self):
        return self._list_paths(self.path_info.bucket, self.path_info.path)

    def exists(self, path_info):
        paths = set(self._list_paths(path_info.bucket, path_info.path))
        return any(path_info.path == path for path in paths)

    def batch_exists(self, path_infos, callback):
        paths = []
        gs = self.gs

        for path_info in path_infos:
            paths.append(
                self._list_paths(path_info.bucket, path_info.path, gs)
            )
            callback.update(str(path_info))

        paths = set(itertools.chain.from_iterable(paths))

        return [path_info.path in paths for path_info in path_infos]

    @contextmanager
    def transfer_context(self):
        yield self.gs

    def _upload(self, from_file, to_info, ctx=None, **_kwargs):
        bucket = ctx.bucket(to_info.bucket)
        blob = bucket.blob(to_info.path)
        blob.upload_from_filename(from_file)

    def _download(self, from_info, to_file, ctx=None, **_kwargs):
        bucket = ctx.bucket(from_info.bucket)
        blob = bucket.get_blob(from_info.path)
        blob.download_to_filename(to_file)
