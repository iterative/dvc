import os
import getpass
import posixpath

try:
    import paramiko
except ImportError:
    paramiko = None

from dvc.logger import Logger
from dvc.progress import progress
from dvc.remote.base import RemoteBase
from dvc.remote.local import RemoteLOCAL
from dvc.config import Config
from dvc.exceptions import DvcException


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

    # NOTE: temporarily only absolute paths are allowed
    REGEX = r'^ssh://((?P<user>.*)@)?(?P<host>[^/]*):(?P<path>/.*)$'

    REQUIRES = {'paramiko': paramiko}
    PARAM_MD5 = 'md5'

    def __init__(self, project, config):
        self.project = project
        self.url = config.get(Config.SECTION_REMOTE_URL, '/')
        self.host = self.group('host')
        self.user = self.group('user')
        if not self.user:
            self.user = config.get(Config.SECTION_REMOTE_USER,
                                   getpass.getuser())
        self.prefix = self.group('path')

    def md5s_to_path_infos(self, md5s):
        return [{'scheme': 'ssh',
                 'host': self.host,
                 'user': self.user,
                 'path': posixpath.join(self.prefix,
                                        md5[0:2], md5[2:])} for md5 in md5s]

    def ssh(self, host=None, user=None):
        msg = "Establishing ssh connection with '{}' as user '{}'"
        Logger.debug(msg.format(host, user))

        ssh = paramiko.SSHClient()

        ssh.load_system_host_keys()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())

        ssh.connect(host, username=user)

        return ssh

    def exists(self, path_infos):
        ret = []
        ssh = self.ssh(host=self.host, user=self.user)
        cmd = 'find {} -type f -follow -print'.format(self.prefix)
        stdout = self._exec(ssh, cmd)
        plist = stdout.split()
        ssh.close()

        for path_info in path_infos:
            exists = False
            if path_info['path'] in plist:
                exists = True
            ret.append(exists)

        return ret

    def _exec(self, ssh, cmd):
        stdin, stdout, stderr = ssh.exec_command(cmd)
        if stdout.channel.recv_exit_status() != 0:
            DvcException('SSH command \'{}\' failed: {}'.format(cmd,
                                                                stderr.read()))
        return stdout.read().decode('utf-8')

    def md5(self, path_info):
        if path_info['scheme'] != 'ssh':
            raise NotImplementedError

        ssh = self.ssh(host=path_info['host'], user=path_info['user'])

        # Use different md5 commands depending on os
        stdout = self._exec(ssh, 'uname').strip()
        if stdout == 'Darwin':
            md5cmd = 'md5'
            index = -1
        elif stdout == 'Linux':
            md5cmd = 'md5sum'
            index = 0
        else:
            msg = '\'{}\' is not supported as a remote'.format(stdout)
            raise DvcException(msg)

        stdout = self._exec(ssh, '{} {}'.format(md5cmd, path_info['path']))
        md5 = stdout.split()[index]
        ssh.close()

        assert len(md5) == 32

        return md5

    def cp(self, from_info, to_info, ssh=None):
        if from_info['scheme'] != 'ssh' or to_info['scheme'] != 'ssh':
            raise NotImplementedError

        assert from_info['host'] == to_info['host']
        assert from_info['user'] == to_info['user']

        s = ssh if ssh else self.ssh(host=from_info['host'],
                                     user=from_info['user'])

        dname = posixpath.dirname(to_info['path'])
        self._exec(s, 'mkdir -p {}'.format(dname))
        self._exec(s, 'cp {} {}'.format(from_info['path'], to_info['path']))

        if not ssh:
            s.close()

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

        Logger.debug('Removing ssh://{}@{}/{}'.format(path_info['user'],
                                                      path_info['host'],
                                                      path_info['path']))

        ssh = self.ssh(host=path_info['host'], user=path_info['user'])
        ssh.open_sftp().remove(path_info['path'])
        ssh.close()

    def download(self,
                 from_infos,
                 to_infos,
                 no_progress_bar=False,
                 names=None):
        names = self._verify_path_args(from_infos, to_infos, names)

        ssh = self.ssh(host=from_infos[0]['host'], user=from_infos[0]['user'])

        for to_info, from_info, name in zip(to_infos, from_infos, names):
            if from_info['scheme'] != 'ssh':
                raise NotImplementedError

            if to_info['scheme'] == 'ssh':
                assert from_info['host'] == to_info['host']
                assert from_info['user'] == to_info['user']
                self.cp(from_info, to_info, ssh=ssh)
                continue

            if to_info['scheme'] != 'local':
                raise NotImplementedError

            msg = "Downloading '{}/{}' to '{}'".format(from_info['host'],
                                                       from_info['path'],
                                                       to_info['path'])
            Logger.debug(msg)

            if not name:
                name = os.path.basename(to_info['path'])

            self._makedirs(to_info['path'])
            tmp_file = self.tmp_file(to_info['path'])
            try:
                ssh.open_sftp().get(from_info['path'],
                                    tmp_file,
                                    callback=create_cb(name))
            except Exception as exc:
                msg = "Failed to download '{}/{}' to '{}'"
                Logger.error(msg.format(from_info['host'],
                                        from_info['path'],
                                        to_info['path']), exc)
                continue

            os.rename(tmp_file, to_info['path'])
            progress.finish_target(name)

        ssh.close()

    def upload(self, from_infos, to_infos, names=None):
        names = self._verify_path_args(to_infos, from_infos, names)

        ssh = self.ssh(host=to_infos[0]['host'], user=to_infos[0]['user'])
        sftp = ssh.open_sftp()

        for from_info, to_info, name in zip(from_infos, to_infos, names):
            if to_info['scheme'] != 'ssh':
                raise NotImplementedError

            if from_info['scheme'] != 'local':
                raise NotImplementedError

            Logger.debug("Uploading '{}' to '{}/{}'".format(from_info['path'],
                                                            to_info['host'],
                                                            to_info['path']))

            if not name:
                name = os.path.basename(from_info['path'])

            dname = posixpath.dirname(to_info['path'])
            self._exec(ssh, 'mkdir -p {}'.format(dname))

            try:
                sftp.put(from_info['path'],
                         to_info['path'],
                         callback=create_cb(name))
            except Exception as exc:
                msg = "Failed to upload '{}' to '{}/{}'"
                Logger.error(msg.format(from_info['path'],
                                        to_info['host'],
                                        to_info['path'], exc))
                continue

            progress.finish_target(name)

        sftp.close()
        ssh.close()

    def _path_to_md5(self, path):
        relpath = posixpath.relpath(path, self.prefix)
        return posixpath.dirname(relpath) + posixpath.basename(relpath)

    def _all_md5s(self):
        ssh = self.ssh(host=self.host, user=self.user)
        cmd = 'find {} -type f -follow -print'.format(self.prefix)
        stdout = self._exec(ssh, cmd)
        flist = stdout.split()
        ssh.close()

        return [self._path_to_md5(path) for path in flist]

    def gc(self, cinfos):
        used = [info[self.PARAM_MD5] for info in cinfos['ssh']]
        used += [info[RemoteLOCAL.PARAM_MD5] for info in cinfos['local']]

        for md5 in self._all_md5s():
            if md5 in used:
                continue
            path_info = {'scheme': 'ssh',
                         'user': self.user,
                         'host': self.host,
                         'path': posixpath.join(self.prefix,
                                                md5[0:2], md5[2:])}
            self.remove(path_info)
