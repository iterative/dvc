from __future__ import absolute_import
import os
import re
import posixpath

try:
    from azure.storage.blob import BlockBlobService
except ImportError:
    BlockBlobService = None

try:
    from urlparse import urlparse
except ImportError:
    from urllib.parse import urlparse

from dvc.logger import logger
from dvc.progress import progress
from dvc.config import Config
from dvc.remote.base import RemoteBase


class Callback(object):
    def __init__(self, name):
        self.name = name

    def __call__(self, current, total):
        progress.update_target(self.name, current, total)


class RemoteAzure(RemoteBase):
    scheme = 'azure'
    REGEX = (r'^azure://((?P<path>[^=;]*)|('
             # backward compatibility
             r'(ContainerName=(?P<container_name>[^;]+);?)?'
             r'(?P<connection_string>.+)?))$')
    REQUIRES = {'azure-storage-blob': BlockBlobService}
    PARAM_ETAG = 'etag'
    COPY_POLL_SECONDS = 5

    def __init__(self, project, config):
        super(RemoteAzure, self).__init__(project, config)
        self.project = project

        self.url = config.get(Config.SECTION_REMOTE_URL)
        match = re.match(self.REGEX, self.url)  # backward compatibility

        self.bucket = (
            urlparse(self.url).netloc
            or match.group('container_name')  # backward compatibility
            or os.getenv('AZURE_STORAGE_CONTAINER_NAME'))

        self.prefix = urlparse(self.url).path.lstrip('/')

        self.connection_string = (
            config.get(Config.SECTION_AZURE_CONNECTION_STRING)
            or match.group('connection_string')  # backward compatibility
            or os.getenv('AZURE_STORAGE_CONNECTION_STRING'))

        if not self.bucket:
            raise ValueError('Azure Storage container name missing')

        if not self.connection_string:
            raise ValueError('Azure Storage connection string missing')

        self.__blob_service = None

    @property
    def blob_service(self):
        if self.__blob_service is None:
            self.__blob_service = BlockBlobService(
                connection_string=self.connection_string)
            self.__blob_service.create_container(self.bucket)
        return self.__blob_service

    def remove(self, path_info):
        if path_info['scheme'] != self.scheme:
            raise NotImplementedError

        logger.debug('Removing azure://{}/{}'.format(path_info['bucket'],
                                                     path_info['path']))

        self.blob_service.delete_blob(path_info['bucket'], path_info['path'])

    def md5s_to_path_infos(self, md5s):
        return [{
            'scheme': self.scheme,
            'bucket': self.bucket,
            'path': posixpath.join(self.prefix, md5[0:2], md5[2:])
        } for md5 in md5s]

    def _list_paths(self, bucket, prefix):
        blob_service = self.blob_service
        next_marker = None
        while True:
            blobs = blob_service.list_blobs(bucket,
                                            prefix=prefix,
                                            marker=next_marker)

            for blob in blobs:
                yield blob.name

            if not blobs.next_marker:
                break

            next_marker = blobs.next_marker

    def cache_exists(self, md5s):
        assert isinstance(md5s, list)

        if len(md5s) == 0:
            return []

        ret = len(md5s) * [False]
        paths = [posixpath.join(self.prefix,
                                md5[0:2],
                                md5[2:]) for md5 in md5s]
        for path in self._list_keys(self.bucket, self.prefix):
            for i, k in enumerate(paths):
                if k == path:
                    ret[i] = True

        assert len(ret) == len(paths) == len(md5s)

        return ret

    def upload(self, from_infos, to_infos, names=None):
        names = self._verify_path_args(to_infos, from_infos, names)

        for from_info, to_info, name in zip(from_infos, to_infos, names):
            if to_info['scheme'] != self.scheme:
                raise NotImplementedError

            if from_info['scheme'] != 'local':
                raise NotImplementedError

            bucket = to_info['bucket']
            path = to_info['path']

            logger.debug("Uploading '{}' to '{}/{}'".format(
                from_info['path'], bucket, path))

            if not name:
                name = os.path.basename(from_info['path'])

            cb = Callback(name)

            try:
                self.blob_service.create_blob_from_path(
                    bucket, path, from_info['path'], progress_callback=cb)
            except Exception as ex:
                msg = "Failed to upload '{}'".format(from_info['path'])
                logger.warn(msg, ex)
            else:
                progress.finish_target(name)

    def download(self,
                 from_infos,
                 to_infos,
                 no_progress_bar=False,
                 names=None):
        names = self._verify_path_args(from_infos, to_infos, names)

        for to_info, from_info, name in zip(to_infos, from_infos, names):
            if from_info['scheme'] != self.scheme:
                raise NotImplementedError

            if to_info['scheme'] != 'local':
                raise NotImplementedError

            bucket = from_info['bucket']
            path = from_info['path']

            logger.debug("Downloading '{}/{}' to '{}'".format(
                bucket, path, to_info['path']))

            tmp_file = self.tmp_file(to_info['path'])
            if not name:
                name = os.path.basename(to_info['path'])

            cb = None if no_progress_bar else Callback(name)

            self._makedirs(to_info['path'])

            try:
                self.blob_service.get_blob_to_path(
                    bucket, path, tmp_file, progress_callback=cb)
            except Exception as exc:
                msg = "Failed to download '{}/{}'".format(bucket, path)
                logger.warn(msg, exc)
            else:
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
            for path in self._list_keys(self.bucket, self.prefix)
        )

    def gc(self, cinfos):
        from dvc.remote.local import RemoteLOCAL

        used = [info[self.PARAM_ETAG] for info in cinfos['azure']]
        used += [info[RemoteLOCAL.PARAM_MD5] for info in cinfos['local']]

        removed = False
        for etag in self._all():
            if etag in used:
                continue
            path_info = {'scheme': self.scheme,
                         'path': posixpath.join(self.prefix,
                                                etag[0:2],
                                                etag[2:]),
                         'bucket': self.bucket}
            self.remove(path_info)
            removed = True

        return removed
