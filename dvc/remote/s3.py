import os
import math
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

from dvc.logger import Logger
from dvc.progress import progress
from dvc.config import Config
from dvc.remote.base import RemoteBase


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
        storagepath = 's3://' + config.get(Config.SECTION_AWS_STORAGEPATH, '').lstrip('/')
        self.url = config.get(Config.SECTION_REMOTE_URL, storagepath)
        self.region = config.get(Config.SECTION_AWS_REGION, None)
        self.profile = config.get(Config.SECTION_AWS_PROFILE, None)
        self.credentialpath = config.get(Config.SECTION_AWS_CREDENTIALPATH, None)
        self.endpoint_url = config.get(Config.SECTION_AWS_ENDPOINT_URL, None)

    @property
    def bucket(self):
        return urlparse(self.url).netloc

    @property
    def prefix(self):
        return urlparse(self.url).path.lstrip('/')

    @property
    def s3(self):
        return boto3.resource('s3', endpoint_url=self.endpoint_url)

    def get_etag(self, bucket, key):
        try:
            obj = self.s3.Object(bucket, key).get()
        except Exception:
            return None

        return obj['ETag'].strip('"')

    def save_info(self, path_info):
        if path_info['scheme'] != 's3':
            raise NotImplementedError

        return {self.PARAM_ETAG: self.get_etag(path_info['bucket'], path_info['key'])}

    def save(self, path_info):
        if path_info['scheme'] != 's3':
            raise NotImplementedError

        etag = self.get_etag(path_info['bucket'], path_info['key'])
        dest_key = posixpath.join(self.prefix, etag[0:2], etag[2:])

        source = {'Bucket': path_info['bucket'],
                  'Key': path_info['key']}
        self.s3.Bucket(self.bucket).copy(source, dest_key)

        return {self.PARAM_ETAG: etag}

    def checkout(self, path_info, checksum_info):
        if path_info['scheme'] != 's3':
            raise NotImplementedError

        etag = checksum_info.get(self.PARAM_ETAG, None)
        if not etag:
            return

        key = posixpath.join(self.prefix, etag[0:2], etag[2:])
        source = {'Bucket': self.bucket,
                  'Key': key}

        self.s3.Bucket(path_info['bucket']).copy(source, path_info['key'])

    def remove(self, path_info):
        if path_info['scheme'] != 's3':
            raise NotImplementedError

        Logger.debug('Removing s3://{}/{}'.format(path_info['bucket'],
                                                  path_info['key']))

        try:
            obj = self.s3.Object(path_info['bucket'], path_info['key']).get()
            obj.delete()
        except Exception:
            pass

    def md5s_to_path_infos(self, md5s):
        return [{'scheme': self.scheme,
                 'bucket': self.bucket,
                 'key': posixpath.join(self.prefix, md5[0:2], md5[2:])} for md5 in md5s]

    def exists(self, path_infos):
        ret = []
        session = boto3.session.Session()
        s3 = session.client('s3')

        keys = []
        kwargs = {'Bucket': self.bucket,
                  'Prefix': self.prefix}
        while True:
            resp = s3.list_objects_v2(**kwargs)
            contents = resp.get('Contents', None)
            if not contents:
                break

            for obj in contents:
                keys.append(obj['Key'])

            token = resp.get('NextContinuationToken', None)
            if not token:
                break

            kwargs['ContinuationToken'] = token

        for path_info in path_infos:
            exists = False
            if path_info['key'] in keys:
                exists = True
            ret.append(exists)

        return ret

    def upload(self, paths, path_infos, names=None):
        assert isinstance(paths, list)
        assert isinstance(path_infos, list)
        assert len(paths) == len(path_infos)
        if not names:
            names = len(paths) * [None]
        else:
            assert isinstance(names, list)
            assert len(names) == len(paths)

        session = boto3.session.Session()
        s3 = session.client('s3')

        for path, path_info, name in zip(paths, path_infos, names):
            if path_info['scheme'] != 's3':
                raise NotImplementedError

            Logger.debug("Uploading '{}' to '{}/{}'".format(path,
                                                            path_info['bucket'],
                                                            path_info['key']))

            if not name:
                name = os.path.basename(path)

            total = os.path.getsize(path)
            cb = Callback(name, total)

            try:
                s3.upload_file(path, path_info['bucket'], path_info['key'], Callback=cb)
            except Exception as exc:
                Logger.error("Failed to upload '{}'".format(path), exc)
                continue

            progress.finish_target(name)

    def download(self, path_infos, fnames, no_progress_bar=False, names=None):
        assert isinstance(fnames, list)
        assert isinstance(path_infos, list)
        assert len(fnames) == len(path_infos)
        if not names:
            names = len(fnames) * [None]
        else:
            assert isinstance(names, list)
            assert len(names) == len(fnames)

        session = boto3.session.Session()
        s3 = session.client('s3')

        for fname, path_info, name in zip(fnames, path_infos, names):
            if path_info['scheme'] != 's3':
                raise NotImplementedError

            Logger.debug("Downloading '{}/{}' to '{}'".format(path_info['bucket'],
                                                              path_info['key'],
                                                              fname))

            tmp_file = self.tmp_file(fname)
            if not name:
                name = os.path.basename(fname)

            if no_progress_bar:
                cb = None
            else:
                total = s3.head_object(Bucket=path_info['bucket'],
                                       Key=path_info['key'])['ContentLength']
                cb = Callback(name, total)

            self._makedirs(fname)

            try:
                s3.download_file(path_info['bucket'], path_info['key'], tmp_file, Callback=cb)
            except Exception as exc:
                Logger.error("Failed to download '{}/{}'".format(path_info['bucket'],
                                                                 path_info['key']), exc)
                return

            os.rename(tmp_file, fname)

            if not no_progress_bar:
                progress.finish_target(name)

    def _path_to_etag(self, path):
        relpath = posixpath.relpath(path, self.prefix)
        return posixpath.dirname(relpath) + posixpath.basename(relpath)

    def _all(self):
        objects = self.s3.Bucket(self.bucket).objects.filter(Prefix=self.prefix)
        return [self._path_to_etag(obj.key) for obj in objects]

    def gc(self, checksum_infos):
        used_etags = [info[self.PARAM_ETAG] for info in checksum_infos]

        for etag in self._all():
            if etag in used_etags:
                continue
            path_info = {'scheme': 's3',
                         'key': posixpath.join(self.prefix, etag[0:2], etag[2:]),
                         'bucket': self.bucket}
            self.remove(path_info)
