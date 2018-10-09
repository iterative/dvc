import os
import re
import getpass
import posixpath
from subprocess import Popen, PIPE

from dvc.config import Config
from dvc.remote.base import RemoteBase
from dvc.remote.local import RemoteLOCAL
from dvc.exceptions import DvcException
from dvc.logger import Logger
from dvc.utils import fix_env


class RemoteHDFS(RemoteBase):
    scheme = 'hdfs'
    REGEX = r'^hdfs://((?P<user>.*)@)?.*$'
    PARAM_CHECKSUM = 'checksum'

    def __init__(self, project, config):
        self.project = project
        self.url = config.get(Config.SECTION_REMOTE_URL, '/')
        self.user = self.group('user')
        if not self.user:
            self.user = config.get(Config.SECTION_REMOTE_USER,
                                   getpass.getuser())

    def hadoop_fs(self, cmd, user=None):
        cmd = 'hadoop fs -' + cmd
        if user:
            cmd = 'HADOOP_USER_NAME={} '.format(user) + cmd

        # NOTE: close_fds doesn't work with redirected stdin/stdout/stderr.
        # See https://github.com/iterative/dvc/issues/1197.
        close_fds = (os.name != 'nt')

        p = Popen(cmd,
                  shell=True,
                  close_fds=close_fds,
                  executable=os.getenv('SHELL'),
                  env=fix_env(os.environ),
                  stdin=PIPE,
                  stdout=PIPE,
                  stderr=PIPE)
        out, err = p.communicate()
        if p.returncode != 0:
            raise DvcException('HDFS command failed: {}: {}'.format(cmd, err))
        return out.decode('utf-8')

    @staticmethod
    def _group(regex, s, gname):
        match = re.match(regex, s)
        assert match is not None
        return match.group(gname)

    def checksum(self, path_info):
        regex = r'.*\t.*\t(?P<checksum>.*)'
        stdout = self.hadoop_fs('checksum {}'.format(path_info['url']),
                                user=path_info['user'])
        return self._group(regex, stdout, 'checksum')

    def cp(self, from_info, to_info):
        self.hadoop_fs('mkdir -p {}'.format(posixpath.dirname(to_info['url'])),
                       user=to_info['user'])
        self.hadoop_fs('cp -f {} {}'.format(from_info['url'], to_info['url']),
                       user=to_info['user'])

    def rm(self, path_info):
        self.hadoop_fs('rm {}'.format(path_info['url']),
                       user=path_info['user'])

    def save_info(self, path_info):
        if path_info['scheme'] != 'hdfs':
            raise NotImplementedError

        assert path_info.get('url')

        return {self.PARAM_CHECKSUM: self.checksum(path_info)}

    @staticmethod
    def to_string(path_info):
        return "{}://{}".format(path_info['scheme'],
                                path_info['url'])

    def changed_cache(self, checksum):
        cache = {}
        cache['scheme'] = 'hdfs'
        cache['user'] = self.user
        cache['url'] = posixpath.join(self.url, checksum[0:2], checksum[2:])

        if {self.PARAM_CHECKSUM: checksum} != self.save_info(cache):
            if self.exists([cache])[0]:
                msg = 'Corrupted cache file {}'
                Logger.warn(msg.format(self.to_string(cache)))
                self.remove(cache)
            return True

        return False

    def changed(self, path_info, checksum_info):
        if not self.exists([path_info])[0]:
            return True

        checksum = checksum_info.get(self.PARAM_CHECKSUM, None)
        if checksum is None:
            return True

        if self.changed_cache(self, checksum):
            return True

        return checksum_info != self.save_info(path_info)

    def save(self, path_info):
        if path_info['scheme'] != 'hdfs':
            raise NotImplementedError

        assert path_info.get('url')

        checksum = self.checksum(path_info)
        dest = path_info.copy()
        dest['url'] = posixpath.join(self.url, checksum[0:2], checksum[2:])

        self.cp(path_info, dest)

        return {self.PARAM_CHECKSUM: checksum}

    def checkout(self, path_info, checksum_info):
        if path_info['scheme'] != 'hdfs':
            raise NotImplementedError

        assert path_info.get('url')

        checksum = checksum_info.get(self.PARAM_CHECKSUM, None)
        if not checksum:
            return

        if not self.changed(path_info, checksum_info):
            msg = "Data '{}' didn't change."
            Logger.info(msg.format(self.to_string(path_info)))
            return

        if self.changed_cache(checksum):
            msg = "Cache '{}' not found. File '{}' won't be created."
            Logger.warn(msg.format(checksum, self.to_string(path_info)))
            return

        if self.exists([path_info])[0]:
            msg = "Data '{}' exists. Removing before checkout."
            Logger.warn(msg.format(self.to_string(path_info)))
            self.remove(path_info)
            return

        msg = "Checking out '{}' with cache '{}'."
        Logger.info(msg.format(self.to_string(path_info), checksum))

        src = path_info.copy()
        src['url'] = posixpath.join(self.url, checksum[0:2], checksum[2:])

        self.cp(src, path_info)

    def remove(self, path_info):
        if path_info['scheme'] != 'hdfs':
            raise NotImplementedError

        assert path_info.get('url')

        Logger.debug('Removing {}'.format(path_info['url']))

        self.rm(path_info)

    def md5s_to_path_infos(self, md5s):
        return [{'scheme': 'hdfs',
                 'user': self.user,
                 'url': posixpath.join(self.url,
                                       md5[0:2], md5[2:])} for md5 in md5s]

    def exists(self, path_infos):
        try:
            stdout = self.hadoop_fs('ls -R {}'.format(self.url))
        except DvcException:
            return len(path_infos) * [False]

        lines = stdout.split('\n')
        lurl = []
        for line in lines:
            if not line.startswith('-'):
                continue
            lurl.append(line.split()[-1])

        ret = []
        for path_info in path_infos:
            exists = False
            if path_info['url'] in lurl:
                exists = True
            ret.append(exists)

        return ret

    def upload(self, from_infos, to_infos, names=None):
        names = self._verify_path_args(to_infos, from_infos, names)

        for from_info, to_info, name in zip(from_infos, to_infos, names):
            if to_info['scheme'] != 'hdfs':
                raise NotImplementedError

            if from_info['scheme'] != 'local':
                raise NotImplementedError

            cmd = 'mkdir -p {}'.format(posixpath.dirname(to_info['url']))
            self.hadoop_fs(cmd, user=to_info['user'])

            cmd = 'copyFromLocal {} {}'.format(from_info['path'],
                                               to_info['url'])
            self.hadoop_fs(cmd, user=to_info['user'])

    def download(self,
                 from_infos,
                 to_infos,
                 no_progress_bar=False,
                 names=None):
        names = self._verify_path_args(from_infos, to_infos, names)

        for to_info, from_info, name in zip(to_infos, from_infos, names):
            if from_info['scheme'] != 'hdfs':
                raise NotImplementedError

            if to_info['scheme'] == 'hdfs':
                self.cp(from_info, to_info)
                continue

            if to_info['scheme'] != 'local':
                raise NotImplementedError

            dname = os.path.dirname(to_info['path'])
            if not os.path.exists(dname):
                os.makedirs(dname)

            cmd = 'copyToLocal {} {}'.format(from_info['url'], to_info['path'])
            self.hadoop_fs(cmd, user=from_info['user'])

    def _path_to_checksum(self, path):
        relpath = posixpath.relpath(path, self.url)
        return posixpath.dirname(relpath) + posixpath.basename(relpath)

    def _all_checksums(self):
        stdout = self.hadoop_fs('ls -R {}'.format(self.url))
        lines = stdout.split('\n')
        flist = []
        for line in lines:
            if not line.startswith('-'):
                continue
            flist.append(line.split()[-1])
        return [self._path_to_checksum(path) for path in flist]

    def gc(self, cinfos):
        used = [info[self.PARAM_CHECKSUM] for info in cinfos['hdfs']]
        used += [info[RemoteLOCAL.PARAM_MD5] for info in cinfos['local']]

        removed = False
        for checksum in self._all_checksums():
            if checksum in used:
                continue
            path_info = {'scheme': 'hdfs',
                         'user': self.user,
                         'url': posixpath.join(self.url,
                                               checksum[0:2], checksum[2:])}
            self.remove(path_info)
            removed = True

        return removed
