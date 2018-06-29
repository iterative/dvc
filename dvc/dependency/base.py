import re
import schema
import posixpath
import ntpath

from dvc.exceptions import DvcException


class DependencyError(DvcException):
    def __init__(self, path, msg):
        super(DependencyError, self).__init__('Dependency \'{}\' error: {}'.format(path, msg))


class DependencyDoesNotExistError(DependencyError):
    def __init__(self, path):
        super(DependencyDoesNotExistError, self).__init__(path, 'does not exist')


class DependencyIsNotFileOrDirError(DependencyError):
    def __init__(self, path):
        super(DependencyIsNotFileOrDirError, self).__init__(path, 'not a file or directory')


class DependencyBase(object):
    REGEX = None

    PARAM_PATH = 'path'

    def __init__(self, stage, path):
        self.stage = stage
        self.project = stage.project
        self.path = path

    @classmethod
    def match(cls, url):
        return re.match(cls.REGEX, url)

    def group(self, name):
        match = self.match(self.path)
        if not match:
            return None
        return match.group(name)

    @classmethod
    def supported(cls, url):
        return cls.match(url) != None

    @property
    def sep(self):
        return '/'

    def changed(self):
        raise NotImplementedError

    def status(self):
        if self.changed():
            #FIXME better msgs
            return {self.rel_path: 'changed'}
        return {}

    def save(self):
        raise NotImplementedError

    def dumpd(self):
        return {self.PARAM_PATH: self.path}

    def download(self, path):
        self.remote.download([self.path_info], [path])
