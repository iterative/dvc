import os
import re
import posixpath
from subprocess import Popen, PIPE

try:
    from urlparse import urlparse
except ImportError:
    from urllib.parse import urlparse

from dvc.config import Config
from dvc.remote.base import RemoteBase
from dvc.exceptions import DvcException


class RemoteHDFS(RemoteBase):
    scheme='hdfs'
    REGEX = r'^hdfs://(?P<path>.*)$'
    PARAM_CHECKSUM = 'checksum'

    def __init__(self, project, config):
        self.project = project
        self.url = config.get(Config.SECTION_REMOTE_URL, '/')
        self.user = config.get(Config.SECTION_REMOTE_USER, None)

    def hadoop_fs(self, cmd):
        cmd = 'hadoop fs -' + cmd
        env = os.environ.copy()
        if self.user:
            env['HADOOP_USER_NAME'] = self.user
        p = Popen(cmd,
                  shell=True,
                  close_fds=True,
                  env=env,
                  executable=os.getenv('SHELL'),
                  stdin=PIPE,
                  stdout=PIPE,
                  stderr=PIPE)
        out, err = p.communicate()
        if p.returncode != 0:
            raise DvcException('HDFS command failed: {}: {}'.format(cmd, err))
        return out

    @staticmethod
    def group(regex, s, gname):
        match = re.match(regex, s)
        assert match != None
        return match.group(gname)

    def checksum(self, url):
        regex = r'.*\t.*\t(?P<checksum>.*)'
        stdout = self.hadoop_fs('checksum {}'.format(url))
        return self.group(regex, stdout, 'checksum')

    def cp(self, from_url, to_url):
        self.hadoop_fs('mkdir -p {}'.format(posixpath.dirname(to_url)))
        self.hadoop_fs('cp {} {}'.format(from_url, to_url))

    def rm(self, url):
        self.hadoop_fs('rm {}'.format(url))

    def save_info(self, path_info):
        if path_info['scheme'] != 'hdfs':
            raise NotImplementedError

        assert path_info.get('url')

        return {self.PARAM_CHECKSUM: self.checksum(path_info['url'])}

    def save(self, path_info):
        if path_info['scheme'] != 'hdfs':
            raise NotImplementedError

        assert path_info.get('url')

        checksum = self.checksum(path_info['url'])
        dest_url = posixpath.join(self.url, checksum[0:2], checksum[2:])

        self.cp(path_info['url'], dest_url)

        return {self.PARAM_CHECKSUM: checksum}

    def checkout(self, path_info, checksum_info):
        if path_info['scheme'] != 'hdfs':
            raise NotImplementedError

        assert path_info.get('url')

        checksum = checksum_info.get(self.PARAM_CHECKSUM, None)
        if not checksum:
            return

        url = posixpath.join(self.url, checksum[0:2], checksum[2:])

        self.cp(url, path_info['url'])

    def remove(self, path_info):
        if path_info['scheme'] != 'hdfs':
            raise NotImplementedError

        assert path_info.get('url')

        self.rm(path_info['url'])

    def upload(self, path, path_info):
        if path_info['scheme'] != 'hdfs':
            raise NotImplementedError

        self.hadoop_fs('mkdir -p {}'.format(posixpath.dirname(path_info['url'])))
        self.hadoop_fs('copyFromLocal {} {}'.format(path, path_info['url']))

    def download(self, path_info, path):
        if path_info['scheme'] != 'hdfs':
            raise NotImplementedError

        dname = os.path.dirname(path)
        if not os.path.exists(dname):
            os.makedirs(dname)

        self.hadoop_fs('copyToLocal {} {}'.format(path_info['url'], path))
