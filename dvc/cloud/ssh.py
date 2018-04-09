import os
import errno
import filecmp
import paramiko
import posixpath

from dvc.logger import Logger
from dvc.utils import copyfile, file_md5
from dvc.cloud.base import DataCloudError, DataCloudBase
from dvc.cloud.aws import create_cb
from dvc.progress import progress


class SSHKey(object):
    def __init__(self, bucket, name):
        self.name = name
        self.bucket = bucket

    @property
    def path(self):
        return os.path.join(self.bucket, self.name)


class DataCloudSSH(DataCloudBase):
    """
    Driver for remote storage over ssh.
    """
    #NOTE: ~/ paths are temporarily forbidden
    REGEX = r'^(?P<user>.*)@(?P<host>.*):(?P<path>/+.*)$'

    @property
    def hostname(self):
        return self.group('host')

    @property
    def username(self):
        return self.group('user')

    def connect(self):
        self.ssh = paramiko.SSHClient()

        self.ssh.load_system_host_keys()
        self.ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())

        self.ssh.connect(self.hostname, username=self.username)

    def disconnect(self):
        self.ssh.close()

    def get_sftp(self):
        #NOTE: SFTP doesn't seem to be thread safe
        return self.ssh.open_sftp()

    def cache_file_key(self, path):
        relpath = os.path.relpath(os.path.abspath(path), self._cloud_settings.cache.cache_dir)
        return relpath.replace('\\', '/')

    def _isfile_remote(self, path):
        try:
            self.get_sftp().open(path, 'r')
            return True
        except IOError as exc:
            if exc.errno != errno.ENOENT:
                raise
        return False

    def _get_key(self, path):
        key_name = self.cache_file_key(path)
        key = SSHKey(self.path, key_name)
        if self._isfile_remote(key.path):
            return key
        return None

    def _makedirs_remote(self, dname):
        try:
            self.get_sftp().chdir(dname)
        except IOError as exc:
            if exc.errno != errno.ENOENT:
                raise

            parent = posixpath.dirname(dname)
            if len(parent):
                self._makedirs_remote(parent)
            self.get_sftp().mkdir(dname)

    def _new_key(self, path):
        key_name = self.cache_file_key(path)
        key = SSHKey(self.path, key_name)
        self._makedirs_remote(posixpath.dirname(key.path))
        return key

    def _push_key(self, key, path):
        self.get_sftp().put(path, key.path, callback=create_cb(key.name))
        progress.finish_target(key.name)
        return path

    def _pull_key(self, key, path, no_progress_bar=False):
        self._makedirs(path)

        tmp_file = self.tmp_file(path)
        try:
            self.get_sftp().get(key.path, tmp_file, callback=create_cb(key.name))
        except Exception as exc:
            Logger.error('Failed to copy "{}": {}'.format(key.path, exc))
            return None

        os.rename(tmp_file, path)
        progress.finish_target(key.name)

        return path
