import os
import re
import errno
import getpass
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

    #NOTE: temporarily only absolute paths are allowed
    REGEX = r'^ssh://((?P<user>.*)@)?(?P<host>[^/]*):(?P<path>/.*)$'

    PARAM_MD5 = 'md5'

    def __init__(self, project, config):
        self.project = project
        self.url = config.get(Config.SECTION_REMOTE_URL, '/')
        self.host = self.group('host')
        self.user = self.group('user')
        if not self.user:
            self.user = config.get(Config.SECTION_REMOTE_USER, getpass.getuser())
        self.prefix = self.group('path')

    def group(self, group):
        m = self.match(self.url)
        if not m:
            return None
        return m.group(group)

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

    def md5(self, path_info):
        if path_info['scheme'] != 'ssh':
            raise NotImplementedError

        ssh = self.ssh(host=path_info['host'], user=path_info['user'])
        stdin, stdout, stderr = ssh.exec_command('md5sum {}'.format(path_info['path']))
        md5 = re.match(r'^(?P<md5>.*)  .*$', stdout.read().decode('utf-8')).group('md5')
        ssh.close()

        return md5

    def cp(self, from_info, to_info):
        if from_info['scheme'] != 'ssh' or to_info['scheme'] != 'ssh':
            raise NotImplementedError

        assert from_info['host'] == to_info['host']
        assert from_info['user'] == to_info['user']

        ssh = self.ssh(host=from_info['host'], user=from_info['user'])
        ssh.exec_command('cp {} {}'.format(from_info['path'], to_info['path']))
        ssh.close()

    def save_info(self, path_info):
        if path_info['scheme'] != 'ssh':
            raise NotImplementedError

        return {self.PARAM_MD5: self.md5(path_info)}

    def save(self, path_info):
        if path_info['scheme'] != 'ssh':
            raise NotImplementedError

        md5 = self.md5(path_info)
        dest = path_info.copy()
        dest['path'] = posixpath.join(self.prefix, md5[0:2], md5[2:])

        self.cp(path_info, dest)

        return {self.PARAM_MD5: md5}

    def checkout(self, path_info, checksum_info):
        if path_info['scheme'] != 'ssh':
            raise NotImplementedError

        md5 = checksum_info.get(self.PARAM_MD5, None)
        if not md5:
            return

        src = path_info.copy()
        src['path'] = posixpath.join(self.prefix, md5[0:2], md5[2:])

        self.cp(src, path_info)

    def remove(self, path_info):
        if path_info['scheme'] != 'ssh':
            raise NotImplementedError

        ssh = self.ssh(host=path_info['host'], user=path_info['user'])
        ssh.open_sftp().remove(path_info['path'])
        ssh.close()

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
