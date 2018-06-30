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

    def save(self, path_info):
        if path_info['scheme'] != 'gs':
            raise NotImplementedError

        etag = self.get_etag(path_info['bucket'], path_info['key'])
        dest_key = posixpath.join(self.prefix, etag[0:2], etag[2:])

        blob = self.gs.bucket(path_info['bucket']).get_blob(path_info['key'])
        if not blob:
            raise DvcException('{} doesn\'t exist in the cloud'.format(path_info['key']))

        self.gs.bucket(self.bucket).copy_blob(blob, self.gs.bucket(path_info['bucket']), new_name=dest_key)

        return {self.PARAM_ETAG: etag}

    def checkout(self, path_info, checksum_info):
        if path_info['scheme'] != 'gs':
            raise NotImplementedError

        etag = checksum_info.get(self.PARAM_ETAG, None)
        if not etag:
            return

        key = posixpath.join(self.prefix, etag[0:2], etag[2:])
        blob = self.gs.bucket(self.bucket).get_blob(key)
        if not blob:
            raise DvcException('{} doesn\'t exist in the cloud'.format(key))

        self.gs.bucket(path_info['bucket']).copy_blob(blob, self.gs.bucket(self.bucket), new_name=path_info['key'])

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

    def upload(self, paths, path_infos, names=None):
        assert isinstance(paths, list)
        assert isinstance(path_infos, list)
        assert len(paths) == len(path_infos)
        if not names:
            names = len(paths) * [None]
        else:
            assert isinstance(names, list)
            assert len(names) == len(paths)

        gs = self.gs

        for path, path_info, name in zip(paths, path_infos, names):
            if path_info['scheme'] != 'gs':
                raise NotImplementedError

            Logger.debug("Uploading '{}' to '{}/{}'".format(path,
                                                            path_info['bucket'],
                                                            path_info['key']))

            if not name:
                name = os.path.basename(path)

            progress.update_target(name, 0, None)

            try:
                gs.bucket(path_info['bucket']).blob(path_info['key']).upload_from_filename(path)
            except Exception as exc:
                Logger.error("Failed to upload '{}' to '{}/{}'".format(path,
                                                                       path_info['bucket'],
                                                                       path_info['key']), exc)
                continue

            progress.finish_target(name)

    def download(self, path_infos, paths, no_progress_bar=False, names=None):
        assert isinstance(paths, list)
        assert isinstance(path_infos, list)
        assert len(paths) == len(path_infos)
        if not names:
            names = len(paths) * [None]
        else:
            assert isinstance(names, list)
            assert len(names) == len(paths)

        gs = self.gs

        for path, path_info, name in zip(paths, path_infos, names):
            if path_info['scheme'] != 'gs':
                raise NotImplementedError

            Logger.debug("Downloading '{}/{}' to '{}'".format(path_info['bucket'],
                                                              path_info['key'],
                                                              path))

            tmp_file = self.tmp_file(path)
            if not name:
                name = os.path.basename(path)

            if not no_progress_bar:
                # percent_cb is not available for download_to_filename, so
                # lets at least update progress at keypoints(start, finish)
                progress.update_target(name, 0, None)

            self._makedirs(path)

            try:
                gs.bucket(path_info['bucket']).get_blob(path_info['key']).download_to_filename(tmp_file)
            except Exception as exc:
                Logger.error("Failed to download '{}/{}' to '{}'".format(path_info['bucket'],
                                                                         path_info['key'],
                                                                         path), exc)
                continue

            os.rename(tmp_file, path)

            if not no_progress_bar:
                progress.finish_target(name)

    def _path_to_etag(self, path):
        relpath = posixpath.relpath(path, self.prefix)
        return posixpath.dirname(relpath) + posixpath.basename(relpath)

    def _all_etags(self):
        blobs = list(self.gs.bucket(self.bucket).list_blobs(prefix=self.prefix))
        return [self._path_to_etag(blob.name) for blob in blobs]

    def gc(self, checksum_infos):
        used_etags = [info[self.PARAM_ETAG] for info in checksum_infos]

        for etag in self._all_etags():
            if etag in used_etags:
                continue
            path_info = {'scheme': 'gs',
                         'bucket': self.bucket,
                         'key': posixpath.join(self.prefix, etag[0:2], etag[2:])}
            self.remove(path_info)
