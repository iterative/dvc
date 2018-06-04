import re
import schema
import posixpath
import ntpath

from dvc.exceptions import DvcException


class DependencyError(DvcException):
    def __init__(self, path, msg):
        super(DependencyError, self).__init__('Dependency \'{}\' error: {}'.format(path, msg))


class DependencyOutsideOfRepoError(DependencyError):
    def __init__(self, path):
        super(DependencyOutsideOfRepoError, self).__init__(path, 'outside of repository')


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

    @classmethod
    def supported(cls, url):
        return cls.match(url) != None

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
