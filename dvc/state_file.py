import os
import yaml

from dvc.exceptions import DvcException
from dvc.data_cloud import file_md5
from dvc.path.path import Path


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
    DVCFILE_NAME = 'Dvcfile'
    STATE_FILE_SUFFIX = '.dvc'

    PARAM_CMD = 'cmd'
    PARAM_DEPS = 'deps'
    PARAM_LOCKED = 'locked'
    PARAM_OUT = 'out'
    PARAM_OUT_GIT = 'out-git'

    def __init__(self,
                 fname=None,
                 cmd=None,
                 out=None,
                 out_git=None,
                 deps=None,
                 locked=None):
        super(StateFile, self).__init__()

        self.cmd = cmd
        self.out = out
        self.out_git = out_git
        self.deps = deps
        self.locked = locked

        self.path = os.path.abspath(fname) if fname else None
        self.cwd = os.path.dirname(self.path) if self.path else None

    @staticmethod
    def parse_deps_state(settings, deps, currdir=None):
        state = {}
        for dep in deps:
            if settings.path_factory.is_data_item(dep):
                item = settings.path_factory.data_item(dep)
                md5 = StateFile.find_md5(item)
            else:
                md5 = file_md5(os.path.join(settings.git.git_dir_abs, dep))[0]

            if currdir:
                name = os.path.relpath(dep, currdir)
            else:
                name = dep

            state[name] = md5
        return state

    @staticmethod
    def loadd(data, fname=None):
        return StateFile(fname=fname,
                         cmd=data.get(StateFile.PARAM_CMD, None),
                         out=data.get(StateFile.PARAM_OUT, None),
                         out_git=data.get(StateFile.PARAM_OUT_GIT, None),
                         deps=data.get(StateFile.PARAM_DEPS, []),
                         locked=data.get(StateFile.PARAM_LOCKED, None))

    @staticmethod
    def load(fname):
        return StateFile._load(fname, StateFile, fname)

    @staticmethod
    def _is_state_file(path):
        return (path.endswith(StateFile.STATE_FILE_SUFFIX) or \
                os.path.basename(path) == StateFile.DVCFILE_NAME) and \
               os.path.isfile(path)
 
    @staticmethod
    def _find_state(fname, dname):
        name = os.path.relpath(fname, dname)
        for entry in os.listdir(dname):
            state_file = os.path.join(dname, entry)
            if not StateFile._is_state_file(state_file):
                continue
            state = StateFile.load(os.path.join(dname, state_file))
            if name in state.out:
                return state
        return None

    @staticmethod
    def _find(name, start, finish):
        dname = start
        fname = name
        while True:
            state = StateFile._find_state(fname, dname)
            if state:
                return state

            if dname == finish:
                break

            dname = os.path.dirname(dname)
        return None

    @staticmethod
    def find(data_item):
        return StateFile._find(data_item.data.abs, data_item.data.dirname, data_item._git.git_dir_abs)

    @staticmethod
    def find_by_output(settings, output):
        path = os.path.abspath(output)
        return StateFile._find(path, os.path.dirname(path), settings.git.git_dir_abs)

    @staticmethod
    def find_all_state_files(git, subdir='.'):
        states = []
        for root, dirs, files in os.walk(os.path.join(git.git_dir_abs, subdir)):        
            for fname in files:
                path = os.path.join(root, fname)

                if not StateFile._is_state_file(path):
                    continue

                states.append(path)
        return states

    @staticmethod
    def find_all_states(git, subdir='.'):
        state_files = StateFile.find_all_state_files(git, subdir)
        return [StateFile.load(state_file) for state_file in state_files]

    @staticmethod
    def find_all_data_files(git, subdir='.'):
        states = StateFile.find_all_states(git, subdir)
        files = []
        for state in states:
            for out in state.out:
                files.append(os.path.join(state.cwd, out))
        return files

    @staticmethod
    def find_all_cache_files(git, subdir='.'):
        states = StateFile.find_all_states(git, subdir)
        cache_files = []
        for state in states:
            for out,md5 in state.out.items():
                cache_files.append(md5)
        return cache_files

    @staticmethod
    def find_md5(data_item):
        state = StateFile.find(data_item)
        name = os.path.relpath(data_item.data.abs, os.path.dirname(state.path))
        return state.out[name]

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
            self.PARAM_LOCKED: self.locked
        }

        self._save(self.path, res)


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

        self._save(self.data_item.local_state.relative, res)
