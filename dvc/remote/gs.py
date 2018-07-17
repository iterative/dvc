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


class RemoteGS(RemoteBase):
    scheme = 'gs'
    REGEX = r'^gs://(?P<path>.*)$'
    REQUIRES = {'google.cloud.storage': storage}
    PARAM_ETAG = 'etag'

    def __init__(self, project, config):
        self.project = project
        storagepath = 'gs://' + config.get(Config.SECTION_AWS_STORAGEPATH, '/').lstrip('/')
        self.url = config.get(Config.SECTION_REMOTE_URL, storagepath)
        self.projectname = config.get(Config.SECTION_GCP_PROJECTNAME, None)

    @property
    def bucket(self):
        return urlparse(self.url).netloc

    @property
    def prefix(self):
        return urlparse(self.url).path.lstrip('/')

    @property
    def gs(self):
        return storage.Client()

    def get_etag(self, bucket, key):
        blob = self.gs.bucket(bucket).get_blob(key)
        if not blob:
            return None

        return blob.etag

    def save_info(self, path_info):
        if path_info['scheme'] != 'gs':
            raise NotImplementedError

        return {self.PARAM_ETAG: self.get_etag(path_info['bucket'], path_info['key'])}

    def _copy(self, from_info, to_info, gs=None):
        gs = gs if gs else self.gs
 
        blob = gs.bucket(from_info['bucket']).get_blob(from_info['key'])
        if not blob:
            raise DvcException('{} doesn\'t exist in the cloud'.format(from_info['key']))

        self.gs.bucket(to_info['bucket']).copy_blob(blob, self.gs.bucket(to_info['bucket']), new_name=to_info['key'])

    def save(self, path_info):
        if path_info['scheme'] != 'gs':
            raise NotImplementedError

        etag = self.get_etag(path_info['bucket'], path_info['key'])
        key = posixpath.join(self.prefix, etag[0:2], etag[2:])
        to_info = {'scheme': 'gs', 'bucket': self.bucket, 'key': key}

        self._copy(path_info, to_info)

        return {self.PARAM_ETAG: etag}

    def checkout(self, path_info, checksum_info):
        if path_info['scheme'] != 'gs':
            raise NotImplementedError

        etag = checksum_info.get(self.PARAM_ETAG, None)
        if not etag:
            return

        key = posixpath.join(self.prefix, etag[0:2], etag[2:])
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
                 'key': posixpath.join(self.prefix, md5[0:2], md5[2:])} for md5 in md5s]

    def exists(self, path_infos):
        ret = []
        gs = self.gs

        keys = [blob.name for blob in gs.bucket(self.bucket).list_blobs(prefix=self.prefix)]

        for path_info in path_infos:
            exists = False
            if path_info['key'] in keys:
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
                gs.bucket(to_info['bucket']).blob(to_info['key']).upload_from_filename(from_info['path'])
            except Exception as exc:
                Logger.error("Failed to upload '{}' to '{}/{}'".format(from_info['path'],
                                                                       to_info['bucket'],
                                                                       to_info['key']), exc)
                continue

            progress.finish_target(name)

    def download(self, from_infos, to_infos, no_progress_bar=False, names=None):
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

            Logger.debug("Downloading '{}/{}' to '{}'".format(from_info['bucket'],
                                                              from_info['key'],
                                                              to_info['path']))

            tmp_file = self.tmp_file(to_info['path'])
            if not name:
                name = os.path.basename(to_info['path'])

            if not no_progress_bar:
                # percent_cb is not available for download_to_filename, so
                # lets at least update progress at keypoints(start, finish)
                progress.update_target(name, 0, None)

            self._makedirs(to_info['path'])

            try:
                gs.bucket(from_info['bucket']).get_blob(from_info['key']).download_to_filename(tmp_file)
            except Exception as exc:
                Logger.error("Failed to download '{}/{}' to '{}'".format(from_info['bucket'],
                                                                         from_info['key'],
                                                                         to_info['path']), exc)
                continue

            os.rename(tmp_file, to_info['path'])

            if not no_progress_bar:
                progress.finish_target(name)

    def _path_to_etag(self, path):
        relpath = posixpath.relpath(path, self.prefix)
        return posixpath.dirname(relpath) + posixpath.basename(relpath)

    def _all_etags(self):
        blobs = list(self.gs.bucket(self.bucket).list_blobs(prefix=self.prefix))
        return [self._path_to_etag(blob.name) for blob in blobs]

    def gc(self, checksum_infos):
        used_etags = [info[self.PARAM_ETAG] for info in checksum_infos['gs']]
        used_etags += [info[RemoteLOCAL.PARAM_MD5] for info in checksum_infos['local']]

        for etag in self._all_etags():
            if etag in used_etags:
                continue
            path_info = {'scheme': 'gs',
                         'bucket': self.bucket,
                         'key': posixpath.join(self.prefix, etag[0:2], etag[2:])}
            self.remove(path_info)
