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

from dvc.logger import Logger
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
        import configobj

        self.project = project
        storagepath = 's3://' + config.get(Config.SECTION_AWS_STORAGEPATH,
                                           '').lstrip('/')
        self.url = config.get(Config.SECTION_REMOTE_URL, storagepath)
        self.region = config.get(Config.SECTION_AWS_REGION, None)
        self.profile = os.getenv('AWS_PROFILE',
                                 config.get(Config.SECTION_AWS_PROFILE,
                                            'default'))
        self.endpoint_url = config.get(Config.SECTION_AWS_ENDPOINT_URL, None)

        credentialpath = config.get(Config.SECTION_AWS_CREDENTIALPATH, None)
        if credentialpath:
            creds_conf = configobj.ConfigObj(credentialpath)
            creds = creds_conf.get(self.profile, {})
        else:
            creds = self._get_aws_config_from_env()

        self.creds = creds
        self.region = creds.get('region', self.region)
        self.aws_access_key_id = creds.get('aws_access_key_id', None)
        self.aws_secret_access_key = creds.get('aws_secret_access_key', None)

    @staticmethod
    def compat_config(config):
        ret = config.copy()
        url = 's3://' + ret.pop(Config.SECTION_AWS_STORAGEPATH, '').lstrip('/')
        ret[Config.SECTION_REMOTE_URL] = url
        return ret

    @property
    def bucket(self):
        return urlparse(self.url).netloc

    @property
    def prefix(self):
        return urlparse(self.url).path.lstrip('/')

    @property
    def s3(self):
        if not self.creds:
            session = boto3.session.Session(profile_name=self.profile)
        else:
            session = boto3.session.Session(
                             aws_access_key_id=self.aws_access_key_id,
                             aws_secret_access_key=self.aws_secret_access_key,
                             region_name=self.region)
        return session.client('s3', endpoint_url=self.endpoint_url)

    def _get_aws_config_from_env(self):
        creds = {
            'region': os.getenv('AWS_DEFAULT_REGION'),
            'aws_access_key_id': os.getenv('AWS_ACCESS_KEY_ID'),
            'aws_secret_access_key': os.getenv('AWS_SECRET_ACCESS_KEY'),
        }
        return {k: v for k, v in creds.items() if v is not None}

    def get_etag(self, bucket, key):
        try:
            obj = self.s3.head_object(Bucket=bucket, Key=key)
        except Exception:
            raise DvcException('s3://{}/{} does not exist'.format(bucket, key))

        return obj['ETag'].strip('"')

    def save_info(self, path_info):
        if path_info['scheme'] != 's3':
            raise NotImplementedError

        return {self.PARAM_ETAG: self.get_etag(path_info['bucket'],
                                               path_info['key'])}

    def changed(self, path_info, checksum_info):
        if not self.exists([path_info])[0]:
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
                  'Key': from_info['key']}
        self.s3.copy(source, to_info['bucket'], to_info['key'])

    def save(self, path_info):
        if path_info['scheme'] != 's3':
            raise NotImplementedError

        etag = self.get_etag(path_info['bucket'], path_info['key'])
        key = posixpath.join(self.prefix, etag[0:2], etag[2:])
        to_info = {'scheme': 's3', 'bucket': self.bucket, 'key': key}

        self._copy(path_info, to_info)

        return {self.PARAM_ETAG: etag}

    @staticmethod
    def to_string(path_info):
        return "s3://{}/{}".format(path_info['bucket'], path_info['key'])

    def changed_cache(self, etag):
        key = posixpath.join(self.prefix, etag[0:2], etag[2:])
        cache = {'scheme': 's3', 'bucket': self.bucket, 'key': key}

        if {self.PARAM_ETAG: etag} != self.save_info(cache):
            if self.exists([cache])[0]:
                msg = 'Corrupted cache file {}'
                Logger.warn(msg.format(self.to_string(cache)))
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
            Logger.info(msg.format(self.to_string(path_info)))
            return

        if self.changed_cache(etag):
            msg = "Cache '{}' not found. File '{}' won't be created."
            Logger.warn(msg.format(etag, self.to_string(path_info)))
            return

        if self.exists([path_info])[0]:
            msg = "Data '{}' exists. Removing before checkout."
            Logger.warn(msg.format(self.to_string(path_info)))
            self.remove(path_info)
            return

        msg = "Checking out '{}' with cache '{}'."
        Logger.info(msg.format(self.to_string(path_info), etag))

        key = posixpath.join(self.prefix, etag[0:2], etag[2:])
        from_info = {'scheme': 's3', 'bucket': self.bucket, 'key': key}

        self._copy(from_info, path_info)

    def remove(self, path_info):
        if path_info['scheme'] != 's3':
            raise NotImplementedError

        Logger.debug('Removing s3://{}/{}'.format(path_info['bucket'],
                                                  path_info['key']))

        self.s3.delete_object(Bucket=path_info['bucket'],
                              Key=path_info['key'])

    def md5s_to_path_infos(self, md5s):
        return [{'scheme': self.scheme,
                 'bucket': self.bucket,
                 'key': posixpath.join(self.prefix,
                                       md5[0:2], md5[2:])} for md5 in md5s]

    def _all_keys(self):
        s3 = self.s3

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

        return keys

    def exists(self, path_infos):
        # NOTE: We mostly use exists() method when filtering a bulk of cache
        # files to decide if we need to download/upload them and in s3
        # list_objects_v2() is much-much faster than trying to check keys
        # one-by-one.
        ret = []

        keys = self._all_keys()

        for path_info in path_infos:
            exists = False
            if path_info['key'] in keys:
                exists = True
            ret.append(exists)

        return ret

    def upload(self, from_infos, to_infos, names=None):
        names = self._verify_path_args(to_infos, from_infos, names)

        s3 = self.s3

        for from_info, to_info, name in zip(from_infos, to_infos, names):
            if to_info['scheme'] != 's3':
                raise NotImplementedError

            if from_info['scheme'] != 'local':
                raise NotImplementedError

            Logger.debug("Uploading '{}' to '{}/{}'".format(from_info['path'],
                                                            to_info['bucket'],
                                                            to_info['key']))

            if not name:
                name = os.path.basename(from_info['path'])

            total = os.path.getsize(from_info['path'])
            cb = Callback(name, total)

            try:
                s3.upload_file(from_info['path'],
                               to_info['bucket'],
                               to_info['key'],
                               Callback=cb)
            except Exception as exc:
                msg = "Failed to upload '{}'".format(from_info['path'])
                Logger.warn(msg, exc)
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
                                                       from_info['key'],
                                                       to_info['path'])
            Logger.debug(msg)

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
                                    Key=from_info['key'])['ContentLength']
                    cb = Callback(name, total)

                s3.download_file(from_info['bucket'],
                                 from_info['key'],
                                 tmp_file,
                                 Callback=cb)
            except Exception as exc:
                msg = "Failed to download '{}/{}'".format(from_info['bucket'],
                                                          from_info['key'])
                Logger.warn(msg, exc)
                continue

            os.rename(tmp_file, to_info['path'])

            if not no_progress_bar:
                progress.finish_target(name)

    def _path_to_etag(self, path):
        relpath = posixpath.relpath(path, self.prefix)
        return posixpath.dirname(relpath) + posixpath.basename(relpath)

    def _all(self):
        keys = self._all_keys()
        return [self._path_to_etag(key) for key in keys]

    def gc(self, cinfos):
        used_etags = [info[self.PARAM_ETAG] for info in cinfos['s3']]
        used_etags += [info[RemoteLOCAL.PARAM_MD5] for info in cinfos['local']]

        removed = False
        for etag in self._all():
            if etag in used_etags:
                continue
            path_info = {'scheme': 's3',
                         'key': posixpath.join(self.prefix,
                                               etag[0:2], etag[2:]),
                         'bucket': self.bucket}
            self.remove(path_info)
            removed = True

        return removed
