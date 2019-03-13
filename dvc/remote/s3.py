from __future__ import unicode_literals

import os
import threading

try:
    import boto3
except ImportError:
    boto3 = None

import dvc.logger as logger
from dvc.utils import tmp_fname, move
from dvc.utils.compat import urlparse, makedirs
from dvc.progress import progress
from dvc.config import Config
from dvc.remote.base import RemoteBase
from dvc.exceptions import DvcException


class Callback(object):
    def __init__(self, name, total):
        self.name = name
        self.total = total
        self.current = 0
        self.lock = threading.Lock()

    def __call__(self, byts):
        with self.lock:
            self.current += byts
            progress.update_target(self.name, self.current, self.total)


class RemoteS3(RemoteBase):
    scheme = "s3"
    REGEX = r"^s3://(?P<path>.*)$"
    REQUIRES = {"boto3": boto3}
    PARAM_CHECKSUM = "etag"

    def __init__(self, repo, config):
        super(RemoteS3, self).__init__(repo, config)

        storagepath = "s3://{}".format(
            config.get(Config.SECTION_AWS_STORAGEPATH, "").lstrip("/")
        )

        self.url = config.get(Config.SECTION_REMOTE_URL, storagepath)

        self.region = os.environ.get("AWS_DEFAULT_REGION") or config.get(
            Config.SECTION_AWS_REGION
        )

        self.profile = os.environ.get("AWS_PROFILE") or config.get(
            Config.SECTION_AWS_PROFILE
        )

        self.endpoint_url = config.get(Config.SECTION_AWS_ENDPOINT_URL)

        self.list_objects = config.get(Config.SECTION_AWS_LIST_OBJECTS)

        self.use_ssl = config.get(Config.SECTION_AWS_USE_SSL, True)

        shared_creds = config.get(Config.SECTION_AWS_CREDENTIALPATH)
        if shared_creds:
            os.environ.setdefault("AWS_SHARED_CREDENTIALS_FILE", shared_creds)

        parsed = urlparse(self.url)
        self.bucket = parsed.netloc
        self.prefix = parsed.path.lstrip("/")

        self.path_info = {"scheme": self.scheme, "bucket": self.bucket}

    @staticmethod
    def compat_config(config):
        ret = config.copy()
        url = "s3://" + ret.pop(Config.SECTION_AWS_STORAGEPATH, "").lstrip("/")
        ret[Config.SECTION_REMOTE_URL] = url
        return ret

    @property
    def s3(self):
        session = boto3.session.Session(
            profile_name=self.profile, region_name=self.region
        )

        return session.client(
            "s3", endpoint_url=self.endpoint_url, use_ssl=self.use_ssl
        )

    def get_etag(self, bucket, path):
        try:
            obj = self.s3.head_object(Bucket=bucket, Key=path)
        except Exception:
            raise DvcException(
                "s3://{}/{} does not exist".format(bucket, path)
            )

        return obj["ETag"].strip('"')

    def save_info(self, path_info):
        if path_info["scheme"] != "s3":
            raise NotImplementedError

        return {
            self.PARAM_CHECKSUM: self.get_etag(
                path_info["bucket"], path_info["path"]
            )
        }

    def copy(self, from_info, to_info, s3=None):
        s3 = s3 if s3 else self.s3

        source = {"Bucket": from_info["bucket"], "Key": from_info["path"]}
        self.s3.copy(source, to_info["bucket"], to_info["path"])

    def remove(self, path_info):
        if path_info["scheme"] != "s3":
            raise NotImplementedError

        logger.debug(
            "Removing s3://{}/{}".format(
                path_info["bucket"], path_info["path"]
            )
        )

        self.s3.delete_object(
            Bucket=path_info["bucket"], Key=path_info["path"]
        )

    def _list_paths(self, bucket, prefix):
        """ Read config for list object api, paginate through list objects."""
        s3 = self.s3
        kwargs = {"Bucket": bucket, "Prefix": prefix}
        if self.list_objects:
            list_objects_api = "list_objects"
        else:
            list_objects_api = "list_objects_v2"
        paginator = s3.get_paginator(list_objects_api)
        for page in paginator.paginate(**kwargs):
            contents = page.get("Contents", None)
            if not contents:
                continue
            for item in contents:
                yield item["Key"]

    def list_cache_paths(self):
        return self._list_paths(self.bucket, self.prefix)

    def exists(self, path_info):
        assert not isinstance(path_info, list)
        assert path_info["scheme"] == "s3"

        paths = self._list_paths(path_info["bucket"], path_info["path"])
        return any(path_info["path"] == path for path in paths)

    def upload(self, from_infos, to_infos, names=None):
        names = self._verify_path_args(to_infos, from_infos, names)

        s3 = self.s3

        for from_info, to_info, name in zip(from_infos, to_infos, names):
            if to_info["scheme"] != "s3":
                raise NotImplementedError

            if from_info["scheme"] != "local":
                raise NotImplementedError

            logger.debug(
                "Uploading '{}' to '{}/{}'".format(
                    from_info["path"], to_info["bucket"], to_info["path"]
                )
            )

            if not name:
                name = os.path.basename(from_info["path"])

            total = os.path.getsize(from_info["path"])
            cb = Callback(name, total)

            try:
                s3.upload_file(
                    from_info["path"],
                    to_info["bucket"],
                    to_info["path"],
                    Callback=cb,
                )
            except Exception:
                msg = "failed to upload '{}'".format(from_info["path"])
                logger.error(msg)
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

        s3 = self.s3

        for to_info, from_info, name in zip(to_infos, from_infos, names):
            if from_info["scheme"] != "s3":
                raise NotImplementedError

            if to_info["scheme"] == "s3":
                self.copy(from_info, to_info, s3=s3)
                continue

            if to_info["scheme"] != "local":
                raise NotImplementedError

            msg = "Downloading '{}/{}' to '{}'".format(
                from_info["bucket"], from_info["path"], to_info["path"]
            )
            logger.debug(msg)

            tmp_file = tmp_fname(to_info["path"])
            if not name:
                name = os.path.basename(to_info["path"])

            makedirs(os.path.dirname(to_info["path"]), exist_ok=True)

            try:
                if no_progress_bar:
                    cb = None
                else:
                    total = s3.head_object(
                        Bucket=from_info["bucket"], Key=from_info["path"]
                    )["ContentLength"]
                    cb = Callback(name, total)

                s3.download_file(
                    from_info["bucket"],
                    from_info["path"],
                    tmp_file,
                    Callback=cb,
                )
            except Exception:
                msg = "failed to download '{}/{}'".format(
                    from_info["bucket"], from_info["path"]
                )
                logger.error(msg)
                continue

            move(tmp_file, to_info["path"])

            if not no_progress_bar:
                progress.finish_target(name)
