import os
import threading
import posixpath

try:
    import boto3
except ImportError:
    boto3 = None

try:
    from urlparse import urlparse
except ImportError:
    from urllib.parse import urlparse

from dvc.logger import logger
from dvc.progress import progress
from dvc.config import Config
from dvc.remote.base import RemoteBase
from dvc.remote.local import RemoteLOCAL
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
    scheme = 's3'
    REGEX = r'^s3://(?P<path>.*)$'
    REQUIRES = {'boto3': boto3}
    PARAM_ETAG = 'etag'

    def __init__(self, project, config):
        self.project = project

        storagepath = 's3://{}'.format(
            config.get(Config.SECTION_AWS_STORAGEPATH, '').lstrip('/')
        )

        self.url = config.get(Config.SECTION_REMOTE_URL, storagepath)

        self.region = (
            os.environ.get('AWS_DEFAULT_REGION') or
            config.get(Config.SECTION_AWS_REGION)
        )

        self.profile = (
            os.environ.get('AWS_PROFILE') or
            config.get(Config.SECTION_AWS_PROFILE)
        )

        self.endpoint_url = config.get(Config.SECTION_AWS_ENDPOINT_URL)

        self.use_ssl = config.get(Config.SECTION_AWS_USE_SSL, True)

        shared_creds = config.get(Config.SECTION_AWS_CREDENTIALPATH)
        if shared_creds:
            os.environ.setdefault('AWS_SHARED_CREDENTIALS_FILE', shared_creds)

        parsed = urlparse(self.url)
        self.bucket = parsed.netloc
        self.prefix = parsed.path.lstrip('/')

    @staticmethod
    def compat_config(config):
        ret = config.copy()
        url = 's3://' + ret.pop(Config.SECTION_AWS_STORAGEPATH, '').lstrip('/')
        ret[Config.SECTION_REMOTE_URL] = url
        return ret

    @property
    def s3(self):
        session = boto3.session.Session(profile_name=self.profile,
                                        region_name=self.region)

        return session.client('s3',
                              endpoint_url=self.endpoint_url,
                              use_ssl=self.use_ssl)

    def get_etag(self, bucket, path):
        try:
            obj = self.s3.head_object(Bucket=bucket, Key=path)
        except Exception:
            raise DvcException('s3://{}/{} does not exist'.format(bucket,
                                                                  path))

        return obj['ETag'].strip('"')

    def save_info(self, path_info):
        if path_info['scheme'] != 's3':
            raise NotImplementedError

        return {self.PARAM_ETAG: self.get_etag(path_info['bucket'],
                                               path_info['path'])}

    def changed(self, path_info, checksum_info):
        if not self.exists(path_info):
            return True

        etag = checksum_info.get(self.PARAM_ETAG, None)
        if etag is None:
            return True

        if self.changed_cache(etag):
            return True

        return checksum_info != self.save_info(path_info)

    def _copy(self, from_info, to_info, s3=None):
        s3 = s3 if s3 else self.s3

        source = {'Bucket': from_info['bucket'],
                  'Key': from_info['path']}
        self.s3.copy(source, to_info['bucket'], to_info['path'])

    def save(self, path_info):
        if path_info['scheme'] != 's3':
            raise NotImplementedError

        etag = self.get_etag(path_info['bucket'], path_info['path'])
        path = posixpath.join(self.prefix, etag[0:2], etag[2:])
        to_info = {'scheme': 's3', 'bucket': self.bucket, 'path': path}

        self._copy(path_info, to_info)

        return {self.PARAM_ETAG: etag}

    @staticmethod
    def to_string(path_info):
        return "s3://{}/{}".format(path_info['bucket'], path_info['path'])

    def changed_cache(self, etag):
        path = posixpath.join(self.prefix, etag[0:2], etag[2:])
        cache = {'scheme': 's3', 'bucket': self.bucket, 'path': path}

        if {self.PARAM_ETAG: etag} != self.save_info(cache):
            if self.exists(cache):
                msg = 'Corrupted cache file {}'
                logger.warn(msg.format(self.to_string(cache)))
                self.remove(cache)
            return True

        return False

    def checkout(self, path_info, checksum_info):
        if path_info['scheme'] != 's3':
            raise NotImplementedError

        etag = checksum_info.get(self.PARAM_ETAG, None)
        if not etag:
            return

        if not self.changed(path_info, checksum_info):
            msg = "Data '{}' didn't change."
            logger.info(msg.format(self.to_string(path_info)))
            return

        if self.changed_cache(etag):
            msg = "Cache '{}' not found. File '{}' won't be created."
            logger.warn(msg.format(etag, self.to_string(path_info)))
            return

        if self.exists(path_info):
            msg = "Data '{}' exists. Removing before checkout."
            logger.warn(msg.format(self.to_string(path_info)))
            self.remove(path_info)
            return

        msg = "Checking out '{}' with cache '{}'."
        logger.info(msg.format(self.to_string(path_info), etag))

        path = posixpath.join(self.prefix, etag[0:2], etag[2:])
        from_info = {'scheme': 's3', 'bucket': self.bucket, 'path': path}

        self._copy(from_info, path_info)

    def remove(self, path_info):
        if path_info['scheme'] != 's3':
            raise NotImplementedError

        logger.debug('Removing s3://{}/{}'.format(path_info['bucket'],
                                                  path_info['path']))

        self.s3.delete_object(Bucket=path_info['bucket'],
                              Key=path_info['path'])

    def md5s_to_path_infos(self, md5s):
        return [{'scheme': self.scheme,
                 'bucket': self.bucket,
                 'path': posixpath.join(self.prefix,
                                        md5[0:2], md5[2:])} for md5 in md5s]

    def _list_paths(self, bucket, prefix):
        s3 = self.s3

        kwargs = {'Bucket': bucket,
                  'Prefix': prefix}
        while True:
            # NOTE: list_objects_v2() is 90% faster than head_object [1]
            #
            # [1] https://www.peterbe.com/plog/
            #     fastest-way-to-find-out-if-a-file-exists-in-s3
            resp = s3.list_objects_v2(**kwargs)
            contents = resp.get('Contents', None)
            if not contents:
                break

            for obj in contents:
                yield obj['Key']

            token = resp.get('NextContinuationToken', None)
            if not token:
                break

            kwargs['ContinuationToken'] = token

    def exists(self, path_info):
        assert not isinstance(path_info, list)
        assert path_info['scheme'] == 's3'

        paths = self._list_paths(path_info['bucket'], path_info['path'])
        return any(path_info['path'] == path for path in paths)

    def cache_exists(self, md5s):
        assert isinstance(md5s, list)

        if len(md5s) == 0:
            return []

        ret = len(md5s) * [False]
        paths = [posixpath.join(self.prefix,
                                md5[0:2],
                                md5[2:]) for md5 in md5s]
        for path in self._list_paths(self.bucket, self.prefix):
            for i, k in enumerate(paths):
                if k == path:
                    ret[i] = True

        assert len(paths) == len(ret) == len(md5s)

        return ret

    def upload(self, from_infos, to_infos, names=None):
        names = self._verify_path_args(to_infos, from_infos, names)

        s3 = self.s3

        for from_info, to_info, name in zip(from_infos, to_infos, names):
            if to_info['scheme'] != 's3':
                raise NotImplementedError

            if from_info['scheme'] != 'local':
                raise NotImplementedError

            logger.debug("Uploading '{}' to '{}/{}'".format(from_info['path'],
                                                            to_info['bucket'],
                                                            to_info['path']))

            if not name:
                name = os.path.basename(from_info['path'])

            total = os.path.getsize(from_info['path'])
            cb = Callback(name, total)

            try:
                s3.upload_file(from_info['path'],
                               to_info['bucket'],
                               to_info['path'],
                               Callback=cb)
            except Exception as exc:
                msg = "Failed to upload '{}'".format(from_info['path'])
                logger.warn(msg, exc)
                continue

            progress.finish_target(name)

    def download(self,
                 from_infos,
                 to_infos,
                 no_progress_bar=False,
                 names=None):
        names = self._verify_path_args(from_infos, to_infos, names)

        s3 = self.s3

        for to_info, from_info, name in zip(to_infos, from_infos, names):
            if from_info['scheme'] != 's3':
                raise NotImplementedError

            if to_info['scheme'] == 's3':
                self._copy(from_info, to_info, s3=s3)
                continue

            if to_info['scheme'] != 'local':
                raise NotImplementedError

            msg = "Downloading '{}/{}' to '{}'".format(from_info['bucket'],
                                                       from_info['path'],
                                                       to_info['path'])
            logger.debug(msg)

            tmp_file = self.tmp_file(to_info['path'])
            if not name:
                name = os.path.basename(to_info['path'])

            self._makedirs(to_info['path'])

            try:
                if no_progress_bar:
                    cb = None
                else:
                    total = s3.head_object(
                                    Bucket=from_info['bucket'],
                                    Key=from_info['path'])['ContentLength']
                    cb = Callback(name, total)

                s3.download_file(from_info['bucket'],
                                 from_info['path'],
                                 tmp_file,
                                 Callback=cb)
            except Exception as exc:
                msg = "Failed to download '{}/{}'".format(from_info['bucket'],
                                                          from_info['path'])
                logger.warn(msg, exc)
                continue

            os.rename(tmp_file, to_info['path'])

            if not no_progress_bar:
                progress.finish_target(name)

    def _path_to_etag(self, path):
        relpath = posixpath.relpath(path, self.prefix)
        return posixpath.dirname(relpath) + posixpath.basename(relpath)

    def _all(self):
        # NOTE: The list might be way too big(e.g. 100M entries, md5 for each
        # is 32 bytes, so ~3200Mb list) and we don't really need all of it at
        # the same time, so it makes sense to use a generator to gradually
        # iterate over it, without keeping all of it in memory.
        return (
            self._path_to_etag(path)
            for path in self._list_paths(self.bucket, self.prefix)
        )

    def gc(self, cinfos):
        used_etags = [info[self.PARAM_ETAG] for info in cinfos['s3']]
        used_etags += [info[RemoteLOCAL.PARAM_MD5] for info in cinfos['local']]

        removed = False
        for etag in self._all():
            if etag in used_etags:
                continue
            path_info = {'scheme': 's3',
                         'path': posixpath.join(self.prefix,
                                                etag[0:2], etag[2:]),
                         'bucket': self.bucket}
            self.remove(path_info)
            removed = True

        return removed
