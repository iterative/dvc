import os
import yaml

from dvc.exceptions import DvcException
from dvc.data_cloud import file_md5


class StateFileError(DvcException):
    def __init__(self, msg):
        DvcException.__init__(self, 'State file error: {}'.format(msg))


class StateFileBase(object):
    @staticmethod
    def _save(fname, data):
        with open(fname, 'w') as fd:
            yaml.dump(data, fd, default_flow_style=False)

    @staticmethod
    def _load(fname, state_class, *args):
        with open(fname, 'r') as fd:
            data = yaml.load(fd)
            return state_class.loadd(data, *args)


class StateFile(StateFileBase):
    MAGIC = 'DVC-State'
    VERSION = '0.1'

    PARAM_CMD = 'cmd'
    PARAM_DEPS = 'deps'
    PARAM_PATH = 'path'
    PARAM_MD5 = 'md5'
    PARAM_LOCKED = 'locked'
    PARAM_OUT = 'out'
    PARAM_OUT_GIT = 'out-git'

    def __init__(self,
                 data_item=None,
                 cmd=None,
                 out=None,
                 out_git=None,
                 deps=None,
                 locked=None,
                 md5=None):
        super(StateFile, self).__init__()
        self.data_item = data_item

        self.cmd = cmd
        self.out = out
        self.out_git = out_git
        self.deps = deps
        self.locked = locked
        self.md5 = md5

    @staticmethod
    def parse_deps_state(settings, deps):
        state = []
        for dep in deps:
            if isinstance(dep, dict):
                dep = dep[StateFile.PARAM_PATH]

            if settings.path_factory.is_data_item(dep):
                item = settings.path_factory.data_item(dep)
                state.append({StateFile.PARAM_PATH: item.data.dvc,
                              StateFile.PARAM_MD5: StateFile.load(item).md5})
            else:
                state.append({StateFile.PARAM_PATH: dep,
                              StateFile.PARAM_MD5: file_md5(os.path.join(settings.git.git_dir_abs, dep))[0]})
        return state

    @property
    def file(self):
        return self.data_item.state.relative

    @staticmethod
    def loadd(data):
        return StateFile(data_item=None,
                         cmd=data.get(StateFile.PARAM_CMD, None),
                         out=data.get(StateFile.PARAM_OUT, None),
                         out_git=data.get(StateFile.PARAM_OUT_GIT, None),
                         deps=data.get(StateFile.PARAM_DEPS, []),
                         md5=data.get(StateFile.PARAM_MD5, []),
                         locked=data.get(StateFile.PARAM_LOCKED, None))

    @staticmethod
    def load(data_item):
        return StateFile._load(data_item.state.relative, StateFile)

    @staticmethod
    def loads(content):
        data = yaml.load(content)
        return StateFile.loadd(data)

    def save(self):
        res = {
            self.PARAM_CMD: self.cmd,
            self.PARAM_OUT: self.out,
            self.PARAM_OUT_GIT: self.out_git,
            self.PARAM_DEPS: self.deps,
            self.PARAM_LOCKED: self.locked,
            self.PARAM_MD5: self.md5
        }

        file_dir = os.path.dirname(self.file)
        if file_dir != '' and not os.path.isdir(file_dir):
            os.makedirs(file_dir)

        self._save(self.file, res)


class LocalStateFile(StateFileBase):
    MAGIC = 'DVC-Local-State'
    VERSION = '0.1'

    PARAM_DATA_TIMESTAMP = 'DataTimestamp'
    PARAM_CACHE_TIMESTAMP = 'CacheTimestamp'

    def __init__(self, data_item, data_timestamp=None, cache_timestamp=None):
        super(LocalStateFile, self).__init__()

        self.data_item = data_item
        self.data_timestamp = data_timestamp
        self.cache_timestamp = cache_timestamp

        if not data_timestamp:
            self.data_timestamp = os.path.getmtime(self.data_item.data.relative)
        if not cache_timestamp:
            self.cache_timestamp = os.path.getmtime(self.data_item.cache.relative)

    @staticmethod
    def loadd(data):
        return LocalStateFile(None,
                              data.get(LocalStateFile.PARAM_DATA_TIMESTAMP, None),
                              data.get(LocalStateFile.PARAM_CACHE_TIMESTAMP, None))

    @staticmethod
    def load(data_item):
        return LocalStateFile._load(data_item.local_state.relative, LocalStateFile)

    def save(self):
        res = {
            self.PARAM_DATA_TIMESTAMP    : self.data_timestamp,
            self.PARAM_CACHE_TIMESTAMP   : self.cache_timestamp
        }

        file_dir = os.path.dirname(self.data_item.local_state.relative)
        if not os.path.isdir(file_dir):
            os.makedirs(file_dir)

        self._save(self.data_item.local_state.relative, res)
