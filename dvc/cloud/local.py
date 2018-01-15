import os
import filecmp

from dvc.logger import Logger
from dvc.utils import copyfile, file_md5
from dvc.cloud.base import DataCloudError, DataCloudBase


class DataCloudLOCAL(DataCloudBase):
    """
    Driver for local storage.
    """
    def push(self, path):
        Logger.debug('sync to cloud ' + path + " " + self.storage_path)
        copyfile(path, self.storage_path)
        return path

    def _import(self, bucket, i, path):
        inp = os.path.join(self.storage_path, i)
        tmp_file = self.tmp_file(path)
        try:
            copyfile(inp, tmp_file)
        except Exception as exc:
            Logger.error('Failed to copy "{}": {}'.format(i, exc))
            return None

        os.rename(tmp_file, path)

        return path

    def pull(self, path):
        Logger.debug('sync from cloud ' + path)
        return self._import(None, path, path)

    def remove(self, path):
        Logger.debug('rm from cloud ' + path)
        os.remove(path)

    def import_data(self, path, out):
        Logger.debug('import from cloud ' + path + " " + out)
        return self._import(None, path, out)

    def _status(self, path):
        local = path
        remote = '{}/{}'.format(self.storage_path, os.path.basename(local))

        remote_exists = os.path.exists(remote)
        local_exists = os.path.exists(local)
        diff = None
        if local_exists and remote_exists:
            diff = filecmp.cmp(local, remote)

        return (local_exists, remote_exists, diff)
