from __future__ import unicode_literals

import os
import logging

from dvc.path import Schemes
from dvc.path.gs import GSPathInfo

try:
    from google.cloud import storage
except ImportError:
    storage = None

from dvc.utils import tmp_fname, move
from dvc.utils.compat import urlparse, makedirs
from dvc.remote.base import RemoteBase
from dvc.config import Config
from dvc.progress import progress
from dvc.exceptions import DvcException


logger = logging.getLogger(__name__)


class RemoteGS(RemoteBase):
    scheme = Schemes.GS
    REGEX = r"^gs://(?P<path>.*)$"
    REQUIRES = {"google.cloud.storage": storage}
    PARAM_CHECKSUM = "md5"

    def __init__(self, repo, config):
        super(RemoteGS, self).__init__(repo, config)
        storagepath = "gs://"
        storagepath += config.get(Config.SECTION_AWS_STORAGEPATH, "/")
        storagepath.lstrip("/")
        self.url = config.get(Config.SECTION_REMOTE_URL, storagepath)
        self.projectname = config.get(Config.SECTION_GCP_PROJECTNAME, None)
        self.credentialpath = config.get(Config.SECTION_GCP_CREDENTIALPATH)

        parsed = urlparse(self.url)
        self.bucket = parsed.netloc
        self.prefix = parsed.path.lstrip("/")

        self.path_info = GSPathInfo(bucket=self.bucket)

    @staticmethod
    def compat_config(config):
        ret = config.copy()
        url = "gs://" + ret.pop(Config.SECTION_AWS_STORAGEPATH, "").lstrip("/")
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

    def copy(self, from_info, to_info, gs=None):
        gs = gs if gs else self.gs

        blob = gs.bucket(from_info.bucket).get_blob(from_info.path)
        if not blob:
            msg = "'{}' doesn't exist in the cloud".format(from_info.path)
            raise DvcException(msg)

        bucket = self.gs.bucket(to_info.bucket)
        bucket.copy_blob(
            blob, self.gs.bucket(to_info.bucket), new_name=to_info.path
        )

    def remove(self, path_info):
        if path_info.scheme != "gs":
            raise NotImplementedError

        logger.debug(
            "Removing gs://{}/{}".format(path_info.bucket, path_info.path)
        )

        blob = self.gs.bucket(path_info.bucket).get_blob(path_info.path)
        if not blob:
            return

        blob.delete()

    def _list_paths(self, bucket, prefix):
        for blob in self.gs.bucket(bucket).list_blobs(prefix=prefix):
            yield blob.name

    def list_cache_paths(self):
        return self._list_paths(self.bucket, self.prefix)

    def exists(self, path_info):
        assert not isinstance(path_info, list)
        assert path_info.scheme == "gs"

        paths = self._list_paths(path_info.bucket, path_info.path)
        return any(path_info.path == path for path in paths)

    def upload(self, from_infos, to_infos, names=None, no_progress_bar=False):
        names = self._verify_path_args(to_infos, from_infos, names)

        gs = self.gs

        for from_info, to_info, name in zip(from_infos, to_infos, names):
            if to_info.scheme != "gs":
                raise NotImplementedError

            if from_info.scheme != "local":
                raise NotImplementedError

            logger.debug(
                "Uploading '{}' to '{}/{}'".format(
                    from_info.path, to_info.bucket, to_info.path
                )
            )

            if not name:
                name = os.path.basename(from_info.path)

            if not no_progress_bar:
                progress.update_target(name, 0, None)

            try:
                bucket = gs.bucket(to_info.bucket)
                blob = bucket.blob(to_info.path)
                blob.upload_from_filename(from_info.path)
            except Exception:
                msg = "failed to upload '{}' to '{}/{}'"
                logger.exception(
                    msg.format(from_info.path, to_info.bucket, to_info.path)
                )
                continue

            progress.finish_target(name)

    def download(
        self,
        from_infos,
        to_infos,
        no_progress_bar=False,
        names=None,
        resume=False,
    ):
        names = self._verify_path_args(from_infos, to_infos, names)

        gs = self.gs

        for to_info, from_info, name in zip(to_infos, from_infos, names):
            if from_info.scheme != "gs":
                raise NotImplementedError

            if to_info.scheme == "gs":
                self.copy(from_info, to_info, gs=gs)
                continue

            if to_info.scheme != "local":
                raise NotImplementedError

            msg = "Downloading '{}/{}' to '{}'".format(
                from_info.bucket, from_info.path, to_info.path
            )
            logger.debug(msg)

            tmp_file = tmp_fname(to_info.path)
            if not name:
                name = os.path.basename(to_info.path)

            if not no_progress_bar:
                # percent_cb is not available for download_to_filename, so
                # lets at least update progress at pathpoints(start, finish)
                progress.update_target(name, 0, None)

            makedirs(os.path.dirname(to_info.path), exist_ok=True)

            try:
                bucket = gs.bucket(from_info.bucket)
                blob = bucket.get_blob(from_info.path)
                blob.download_to_filename(tmp_file)
            except Exception:
                msg = "failed to download '{}/{}' to '{}'"
                logger.exception(
                    msg.format(from_info.bucket, from_info.path, to_info.path)
                )
                continue

            move(tmp_file, to_info.path)

            if not no_progress_bar:
                progress.finish_target(name)
