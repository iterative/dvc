from __future__ import absolute_import
import os
import re

try:
    from azure.storage.blob import BlockBlobService
except ImportError:
    BlockBlobService = None

from dvc.logger import Logger
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
    REGEX = (r'^azure://'
             r'(ContainerName=(?P<container_name>[^;]+);?)?'
             r'(?P<connection_string>.+)?$')
    REQUIRES = {'azure-storage-blob': BlockBlobService}
    PARAM_ETAG = 'etag'
    COPY_POLL_SECONDS = 5

    def __init__(self, project, config):
        super(RemoteAzure, self).__init__(project, config)
        self.project = project

        url = config.get(Config.SECTION_REMOTE_URL)
        match = re.match(self.REGEX, url)

        self.bucket = (
            match.group('container_name')
            or os.getenv('AZURE_STORAGE_CONTAINER_NAME'))

        self.connection_string = (
            match.group('connection_string')
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

# FIXME: temporarily disabled because of the lack of test for external azure
# dependencies/outputs/cache.
#
#    def _get_etag(self, bucket, key):
#        try:
#            blob = self.blob_service.get_blob_properties(bucket, key)
#            return blob.properties.etag
#        except Exception:
#            return None
#
#    def save_info(self, path_info):
#        if path_info['scheme'] != self.scheme:
#            raise NotImplementedError
#
#        return {self.PARAM_ETAG: self._get_etag(
#            path_info['bucket'], path_info['key'])}
#
#    def save(self, path_info):
#        if path_info['scheme'] != self.scheme:
#            raise NotImplementedError
#
#        etag = self._get_etag(path_info['bucket'], path_info['key'])
#        dest_key = '{}/{}'.format(etag[0:2], etag[2:])
#
#        self._copy_blob(to_bucket=self.bucket,
#                        to_key=dest_key,
#                        from_bucket=path_info['bucket'],
#                        from_key=path_info['key'])
#
#        return {self.PARAM_ETAG: etag}
#
#    def _copy_blob(self, to_bucket, to_key, from_bucket, from_key):
#        source = self.blob_service.make_blob_url(from_bucket, from_key)
#
#        copy = self.blob_service.copy_blob(to_bucket, to_key, source)
#
#        if self.COPY_POLL_SECONDS <= 0:
#            return
#
#        while copy.status != 'success':
#            time.sleep(self.COPY_POLL_SECONDS)
#            copy = self.blob_service.get_blob_properties(
#                to_bucket, to_key).properties.copy
#
#    def checkout(self, path_info, checksum_info):
#        if path_info['scheme'] != self.scheme:
#            raise NotImplementedError
#
#        etag = checksum_info.get(self.PARAM_ETAG, None)
#        if not etag:
#            return
#
#        self._copy_blob(to_bucket=path_info['bucket'],
#                        to_key=path_info['key'],
#                        from_bucket=self.bucket,
#                        from_key='{}/{}'.format(etag[0:2], etag[2:]))
#
#    def remove(self, path_info):
#        if path_info['scheme'] != self.scheme:
#            raise NotImplementedError
#
#        Logger.debug('Removing azure://{}/{}'.format(path_info['bucket'],
#                                                     path_info['key']))
#
#        self.blob_service.delete_blob(path_info['bucket'], path_info['key'])

    def md5s_to_path_infos(self, md5s):
        return [{
            'scheme': self.scheme,
            'bucket': self.bucket,
            'key': '{}/{}'.format(md5[0:2], md5[2:])
        } for md5 in md5s]

    def exists(self, path_infos):
        keys = {blob.name
                for blob in self.blob_service.list_blobs(self.bucket)}

        ret = []
        for path_info in path_infos:
            if path_info['scheme'] != self.scheme:
                raise NotImplementedError

            key = path_info['key']
            ret.append(key in keys)

        return ret

    def upload(self, from_infos, to_infos, names=None):
        names = self._verify_path_args(to_infos, from_infos, names)

        for from_info, to_info, name in zip(from_infos, to_infos, names):
            if to_info['scheme'] != self.scheme:
                raise NotImplementedError

            if from_info['scheme'] != 'local':
                raise NotImplementedError

            bucket = to_info['bucket']
            key = to_info['key']

            Logger.debug("Uploading '{}' to '{}/{}'".format(
                from_info['path'], bucket, key))

            if not name:
                name = os.path.basename(from_info['path'])

            cb = Callback(name)

            try:
                self.blob_service.create_blob_from_path(
                    bucket, key, from_info['path'], progress_callback=cb)
            except Exception as ex:
                Logger.error("Failed to upload '{}'".format(from_info['path']),
                             ex)
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
            key = from_info['key']

            Logger.debug("Downloading '{}/{}' to '{}'".format(
                bucket, key, to_info['path']))

            tmp_file = self.tmp_file(to_info['path'])
            if not name:
                name = os.path.basename(to_info['path'])

            cb = None if no_progress_bar else Callback(name)

            self._makedirs(to_info['path'])

            try:
                self.blob_service.get_blob_to_path(
                    bucket, key, tmp_file, progress_callback=cb)
            except Exception as exc:
                Logger.error("Failed to download '{}/{}'".format(
                    bucket, key), exc)
            else:
                os.rename(tmp_file, to_info['path'])

                if not no_progress_bar:
                    progress.finish_target(name)

# FIXME: temporarily disabled because of the lack of test for external azure
# dependencies/outputs/cache.
#
#    def gc(self, cinfos):
#        used = [info[self.PARAM_ETAG] for info in cinfos['azure']]
#        used += [info[RemoteLOCAL.PARAM_MD5] for info in cinfos['local']]
#
#        all_blobs = self.blob_service.list_blobs(self.bucket)
#
#        for blob in all_blobs:
#            etag = blob.properties.etag
#            if etag in used:
#                continue
#            path_info = {'scheme': self.scheme,
#                         'key': blob.name,
#                         'bucket': self.bucket}
#            self.remove(path_info)
