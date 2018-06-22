import os
import errno
import filecmp
import paramiko
import posixpath

from dvc.logger import Logger
from dvc.utils import copyfile, file_md5
from dvc.progress import progress
from dvc.remote.base import RemoteBase
from dvc.config import Config


def sizeof_fmt(num, suffix='B'):
    """ Convert number of bytes to human-readable string """
    for unit in ['', 'K', 'M', 'G', 'T', 'P', 'E', 'Z']:
        if abs(num) < 1024.0:
            return "%3.1f%s%s" % (num, unit, suffix)
        num /= 1024.0
    return "%.1f%s%s" % (num, 'Y', suffix)


def percent_cb(name, complete, total):
    """ Callback for updating target progress """
    Logger.debug('{}: {} transferred out of {}'.format(name,
                                                       sizeof_fmt(complete),
                                                       sizeof_fmt(total)))
    progress.update_target(name, complete, total)


def create_cb(name):
    """ Create callback function for multipart object """
    return (lambda cur, tot: percent_cb(name, cur, tot))


class RemoteSSH(RemoteBase):
    scheme = 'ssh'

    #NOTE: ~/ paths are temporarily forbidden
    REGEX = r'^(?P<user>.*)@(?P<host>.*):(?P<path>/+.*)$'

    def __init__(self, project, config):
        self.project = project
        storagepath = config.get(Config.SECTION_AWS_STORAGEPATH, '/')
        self.url = config.get(Config.SECTION_REMOTE_URL, storagepath)
        self.host = self.group('host')
        self.user = self.group('user')
        self.prefix = self.group('path')

    def group(self, group):
        return self.match(self.url).group(group)

    def cache_file_key(self, path):
        relpath = os.path.relpath(os.path.abspath(path), self.project.cache.local.cache_dir)
        return posixpath.join(self.prefix, relpath.replace('\\', '/'))

    def ssh(self, host=None, user=None):
        Logger.debug("Establishing ssh connection with '{}' as user '{}'".format(host, user))

        ssh = paramiko.SSHClient()

        ssh.load_system_host_keys()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())

        ssh.connect(host, username=user)

        return ssh

    def _isfile_remote(self, path_info):
        try:
            ssh = self.ssh(path_info['host'], path_info['user'])
            ssh.open_sftp().open(path_info['path'], 'r')
            ssh.close()
            return True
        except IOError as exc:
            if exc.errno != errno.ENOENT:
                raise
        return False

    def _get_path_info(self, path):
        key = self.cache_file_key(path)
        ret = {'scheme': 'ssh',
               'host': self.host,
               'user': self.user,
               'path': key}
        if self._isfile_remote(ret):
            return ret
        return None

    def _do_makedirs_remote(self, sftp, dname):
        try:
            sftp.chdir(dname)
        except IOError as exc:
            if exc.errno != errno.ENOENT:
                raise

            parent = posixpath.dirname(dname)
            if len(parent):
                self._do_makedirs_remote(sftp, parent)

            sftp.mkdir(dname)

    def _makedirs_remote(self, path_info):
        dname = posixpath.dirname(path_info['path'])
        ssh = self.ssh(path_info['host'], path_info['user'])
        sftp = ssh.open_sftp()
        self._do_makedirs_remote(sftp, dname)
        ssh.close()

    def _new_path_info(self, path):
        key = self.cache_file_key(path)
        ret = {'scheme': 'ssh',
               'host': self.host,
               'user': self.user,
               'path': key}
        self._makedirs_remote(ret)
        return ret

    def download(self, path_info, path, no_progress_bar=False, name=None):
        if path_info['scheme'] != 'ssh':
            raise NotImplementedError

        Logger.debug("Downloading '{}/{}' to '{}'".format(path_info['host'],
                                                          path_info['path'],
                                                          path))
        if not name:
            name = os.path.basename(path)

        self._makedirs(path)
        tmp_file = self.tmp_file(path)
        try:
            ssh = self.ssh(host=path_info['host'], user=path_info['user'])
            ssh.open_sftp().get(path_info['path'], tmp_file, callback=create_cb(name))
            ssh.close()
        except Exception as exc:
            Logger.error("Failed to download '{}/{}' to '{}'".format(path_info['host'],
                                                                     path_info['path'],
                                                                     path), exc)
            return None

        os.rename(tmp_file, path)
        progress.finish_target(name)

        return path

    def upload(self, path, path_info, name=None):
        if path_info['scheme'] != 'ssh':
            raise NotImplementedError

        Logger.debug("Uploading '{}' to '{}/{}'".format(path,
                                                        path_info['host'],
                                                        path_info['path']))

        if not name:
            name = os.path.basename(path)

        try:
            ssh = self.ssh(host=path_info['host'], user=path_info['user'])
            ssh.open_sftp().put(path, path_info['path'], callback=create_cb(name))
            ssh.close()
        except Exception as exc:
            Logger.error("Failed to upload '{}' to '{}/{}'".format(path,
                                                                   path_info['host'],
                                                                   path_info['path']), exc)
            return None

        progress.finish_target(name)

        return path
