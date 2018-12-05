import os
import posixpath

try:
    from google.cloud import storage
except ImportError:
    storage = None

try:
    from urlparse import urlparse
except ImportError:
    from urllib.parse import urlparse

from dvc.logger import Logger
from dvc.remote.base import RemoteBase
from dvc.remote.local import RemoteLOCAL
from dvc.config import Config
from dvc.progress import progress
from dvc.exceptions import DvcException


class RemoteGS(RemoteBase):
    scheme = 'gs'
    REGEX = r'^gs://(?P<path>.*)$'
    REQUIRES = {'google.cloud.storage': storage}
    PARAM_MD5 = 'md5'

    def __init__(self, project, config):
        self.project = project
        storagepath = 'gs://'
        storagepath += config.get(Config.SECTION_AWS_STORAGEPATH, '/')
        storagepath.lstrip('/')
        self.url = config.get(Config.SECTION_REMOTE_URL, storagepath)
        self.projectname = config.get(Config.SECTION_GCP_PROJECTNAME, None)

    @staticmethod
    def compat_config(config):
        ret = config.copy()
        url = 'gs://' + ret.pop(Config.SECTION_AWS_STORAGEPATH, '').lstrip('/')
        ret[Config.SECTION_REMOTE_URL] = url
        return ret

    @property
    def bucket(self):
        return urlparse(self.url).netloc

    @property
    def prefix(self):
        return urlparse(self.url).path.lstrip('/')

    @property
    def gs(self):
        return storage.Client()

    def get_md5(self, bucket, key):
        import base64
        import codecs

        blob = self.gs.bucket(bucket).get_blob(key)
        if not blob:
            return None

        b64_md5 = blob.md5_hash
        md5 = base64.b64decode(b64_md5)
        return codecs.getencoder('hex')(md5)[0].decode('utf-8')

    def save_info(self, path_info):
        if path_info['scheme'] != 'gs':
            raise NotImplementedError

        return {self.PARAM_MD5: self.get_md5(path_info['bucket'],
                                             path_info['key'])}

    @staticmethod
    def to_string(path_info):
        return "{}://{}/{}".format(path_info['scheme'],
                                   path_info['bucket'],
                                   path_info['key'])

    def changed_cache(self, md5):
        key = posixpath.join(self.prefix, md5[0:2], md5[2:])
        cache = {'scheme': 'gs', 'bucket': self.bucket, 'key': key}

        if {self.PARAM_MD5: md5} != self.save_info(cache):
            if self.exists([cache])[0]:
                msg = 'Corrupted cache file {}'
                Logger.warn(msg.format(self.to_string(cache)))
                self.remove(cache)
            return True

        return False

    def changed(self, path_info, checksum_info):
        if not self.exists([path_info])[0]:
            return True

        md5 = checksum_info.get(self.PARAM_MD5, None)
        if md5 is None:
            return True

        if self.changed_cache(md5):
            return True

        return checksum_info != self.save_info(path_info)

    def _copy(self, from_info, to_info, gs=None):
        gs = gs if gs else self.gs

        blob = gs.bucket(from_info['bucket']).get_blob(from_info['key'])
        if not blob:
            msg = '{} doesn\'t exist in the cloud'
            raise DvcException(msg.format(from_info['key']))

        bucket = self.gs.bucket(to_info['bucket'])
        bucket.copy_blob(blob,
                         self.gs.bucket(to_info['bucket']),
                         new_name=to_info['key'])

    def save(self, path_info):
        if path_info['scheme'] != 'gs':
            raise NotImplementedError

        md5 = self.get_md5(path_info['bucket'], path_info['key'])
        key = posixpath.join(self.prefix, md5[0:2], md5[2:])
        to_info = {'scheme': 'gs', 'bucket': self.bucket, 'key': key}

        self._copy(path_info, to_info)

        return {self.PARAM_MD5: md5}

    def checkout(self, path_info, checksum_info):
        if path_info['scheme'] != 'gs':
            raise NotImplementedError

        md5 = checksum_info.get(self.PARAM_MD5, None)
        if not md5:
            return

        if not self.changed(path_info, checksum_info):
            msg = "Data '{}' didn't change."
            Logger.info(msg.format(self.to_string(path_info)))
            return

        if self.changed_cache(md5):
            msg = "Cache '{}' not found. File '{}' won't be created."
            Logger.warn(msg.format(md5, self.to_string(path_info)))
            return

        if self.exists([path_info])[0]:
            msg = "Data '{}' exists. Removing before checkout."
            Logger.warn(msg.format(self.to_string(path_info)))
            self.remove(path_info)
            return

        msg = "Checking out '{}' with cache '{}'."
        Logger.info(msg.format(self.to_string(path_info), md5))

        key = posixpath.join(self.prefix, md5[0:2], md5[2:])
        from_info = {'scheme': 'gs', 'bucket': self.bucket, 'key': key}

        self._copy(from_info, path_info)

    def remove(self, path_info):
        if path_info['scheme'] != 'gs':
            raise NotImplementedError

        Logger.debug("Removing gs://{}/{}".format(path_info['bucket'],
                                                  path_info['key']))

        blob = self.gs.bucket(path_info['bucket']).get_blob(path_info['key'])
        if not blob:
            return

        blob.delete()

    def md5s_to_path_infos(self, md5s):
        return [{'scheme': 'gs',
                 'bucket': self.bucket,
                 'key': posixpath.join(self.prefix,
                                       md5[0:2], md5[2:])} for md5 in md5s]

    def exists(self, path_infos):
        ret = []

        if len(path_infos) == 0:
            return ret

        gs = self.gs
        bucket_name = path_infos[0]['bucket']
        bucket = gs.bucket(bucket_name)

        for path_info in path_infos:
            assert self.scheme == path_info['scheme']
            assert bucket_name == path_info['bucket']

            exists = False
            key = path_info['key']
            keys = [blob.name for blob in bucket.list_blobs(prefix=key)]
            if key in keys:
                exists = True
            ret.append(exists)
        return ret

    def upload(self, from_infos, to_infos, names=None):
        names = self._verify_path_args(to_infos, from_infos, names)

        gs = self.gs

        for from_info, to_info, name in zip(from_infos, to_infos, names):
            if to_info['scheme'] != 'gs':
                raise NotImplementedError

            if from_info['scheme'] != 'local':
                raise NotImplementedError

            Logger.debug("Uploading '{}' to '{}/{}'".format(from_info['path'],
                                                            to_info['bucket'],
                                                            to_info['key']))

            if not name:
                name = os.path.basename(from_info['path'])

            progress.update_target(name, 0, None)

            try:
                bucket = gs.bucket(to_info['bucket'])
                blob = bucket.blob(to_info['key'])
                blob.upload_from_filename(from_info['path'])
            except Exception as exc:
                msg = "Failed to upload '{}' to '{}/{}'"
                Logger.warn(msg.format(from_info['path'],
                                       to_info['bucket'],
                                       to_info['key']), exc)
                continue

            progress.finish_target(name)

    def download(self,
                 from_infos,
                 to_infos,
                 no_progress_bar=False,
                 names=None):
        names = self._verify_path_args(from_infos, to_infos, names)

        gs = self.gs

        for to_info, from_info, name in zip(to_infos, from_infos, names):
            if from_info['scheme'] != 'gs':
                raise NotImplementedError

            if to_info['scheme'] == 'gs':
                self._copy(from_info, to_info, gs=gs)
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

            if not no_progress_bar:
                # percent_cb is not available for download_to_filename, so
                # lets at least update progress at keypoints(start, finish)
                progress.update_target(name, 0, None)

            self._makedirs(to_info['path'])

            try:
                bucket = gs.bucket(from_info['bucket'])
                blob = bucket.get_blob(from_info['key'])
                blob.download_to_filename(tmp_file)
            except Exception as exc:
                msg = "Failed to download '{}/{}' to '{}'"
                Logger.warn(msg.format(from_info['bucket'],
                                       from_info['key'],
                                       to_info['path']), exc)
                continue

            os.rename(tmp_file, to_info['path'])

            if not no_progress_bar:
                progress.finish_target(name)

    def _path_to_md5(self, path):
        relpath = posixpath.relpath(path, self.prefix)
        return posixpath.dirname(relpath) + posixpath.basename(relpath)

    def _all_md5s(self):
        blobs = self.gs.bucket(self.bucket).list_blobs(prefix=self.prefix)
        blobs = list(blobs)
        return [self._path_to_md5(blob.name) for blob in blobs]

    def gc(self, cinfos):
        used = [info[self.PARAM_MD5] for info in cinfos['gs']]
        used += [info[RemoteLOCAL.PARAM_MD5] for info in cinfos['local']]

        removed = False
        for md5 in self._all_md5s():
            if md5 in used:
                continue
            path_info = {'scheme': 'gs',
                         'bucket': self.bucket,
                         'key': posixpath.join(self.prefix,
                                               md5[0:2], md5[2:])}
            self.remove(path_info)
            removed = True

        return removed
