import os
import filecmp

from dvc.logger import Logger
from dvc.utils import copyfile, file_md5
from dvc.cloud.base import DataCloudError, DataCloudBase


class LocalKey(object):
    def __init__(self, bucket, name):
        self.name = name
        self.bucket = bucket

    @property
    def path(self):
        return os.path.join(self.bucket, self.name)

    def delete(self):
        os.unlink(self.path)


class DataCloudLOCAL(DataCloudBase):
    """
    Driver for local storage.
    """
    def cache_file_key(self, path):
        return os.path.relpath(os.path.abspath(path), self._cloud_settings.cache_dir)

    def _cmp_checksum(self, key, path):
        return filecmp.cmp(path, key.path)

    def _get_key(self, path):
        key_name = self.cache_file_key(path)
        key = LocalKey(self.storage_path, key_name)
        if os.path.exists(key.path) and os.path.isfile(key.path):
            return key
        return None

    def _get_keys(self, path):
        key_name = self.cache_file_key(path)
        key = LocalKey(self.storage_path, key_name)

        if not os.path.isdir(key.path):
            return None

        res = []
        for root, dirs, files in os.walk(key.path):
            for fname in files:
                p = os.path.join(root, fname)
                k_name = os.path.relpath(p, self.storage_path)
                res.append(LocalKey(self.storage_path, k_name))
        return res

    def _new_key(self, path):
        key_name = self.cache_file_key(path)
        key = LocalKey(self.storage_path, key_name)
        self._makedirs(key.path)
        return key

    def _push_key(self, key, path):
        self._makedirs(key.path)
        copyfile(path, key.path)
        return path

    def _pull_key(self, key, path):
        self._makedirs(path)

        tmp_file = self.tmp_file(path)
        try:
            copyfile(key.path, tmp_file)
        except Exception as exc:
            Logger.error('Failed to copy "{}": {}'.format(key.path, exc))
            return None

        os.rename(tmp_file, path)

        return path
