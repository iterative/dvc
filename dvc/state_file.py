import os
import ntpath
import sys
import json
import re

from dvc import utils
from dvc.exceptions import DvcException
from dvc.path.data_item import DataDirError
from dvc.system import System


class StateFileError(DvcException):
    def __init__(self, msg):
        DvcException.__init__(self, 'State file error: {}'.format(msg))


class StateFile(object):
    MAGIC = 'DVC-State'
    VERSION = '0.1'

    DVC_PYTHON_FILE_NAME = 'dvc.py'
    DVC_COMMAND = 'dvc'

    COMMAND_RUN = 'run'
    COMMAND_IMPORT_FILE = 'import-file'
    ACCEPTED_COMMANDS = {COMMAND_IMPORT_FILE, COMMAND_RUN}

    PARAM_COMMAND = 'Command'
    PARAM_TYPE = 'Type'
    PARAM_VERSION = 'Version'
    PARAM_ARGV = 'Argv'
    PARAM_CWD = 'Cwd'
    PARAM_INPUT_FILES = 'InputFiles'
    PARAM_OUTPUT_FILES = 'OutputFiles'
    PARAM_CODE_DEPENDENCIES = 'CodeDependencies'
    PARAM_LOCKED = 'Locked'
    PARAM_STDOUT = "Stdout"
    PARAM_STDERR = "Stderr"
    PARAM_SHELL = "Shell"
    PARAM_TARGET_METRICS = 'TargetMetrics'
    TARGET_METRICS_SINGLE_METRIC = 'SingleMetric'
    PARAM_MD5 = 'Md5'

    def __init__(self,
                 command,
                 data_item,
                 settings,
                 input_files,
                 output_files,
                 code_dependencies=[],
                 lock=False,
                 argv=sys.argv,
                 stdout=None,
                 stderr=None,
                 cwd=None,
                 shell=False,
                 target_metrics={},
                 md5=None):
        self.data_item = data_item

        self.settings = settings
        self.input_files = input_files
        self.output_files = output_files
        self.locked = lock
        self.code_dependencies = code_dependencies
        self.shell = shell

        self.md5 = md5
        if not md5 and data_item:
            self.md5 = os.path.basename(data_item.cache.relative)

        if command not in self.ACCEPTED_COMMANDS:
            raise StateFileError('Args error: unknown command %s' % command)
        self.command = command

        self._argv = argv

        self.stdout = stdout
        self.stderr = stderr

        if cwd:
            self.cwd = cwd
        else:
            self.cwd = self.get_dvc_path()

        self.target_metrics = target_metrics

    @property
    def file(self):
        return self.data_item.state.relative

    @property
    def single_target_metric(self):
        res = self.target_metrics.get(self.TARGET_METRICS_SINGLE_METRIC)
        if res is None:
            return None
        return float(res)

    def update_target_metrics(self):
        if not self.data_item:
            raise StateFileError('Unable to get target metric: data item is not defined')

        data_file = self.data_item.data.relative
        if not os.path.exists(data_file):
            raise StateFileError('Unable to get target metric: data item does not exist')

        target_metric = StateFile.try_parse_target_metric(data_file)
        if target_metric:
            self.target_metrics[StateFile.TARGET_METRICS_SINGLE_METRIC] = target_metric

    @staticmethod
    def try_parse_target_metric(file_name):
        try:
            metric = utils.parse_target_metric_file(file_name)
            if not metric:
                raise StateFileError('Unable to parse metrics from the first line of file {}'.format(file_name))
            return metric
        except StateFileError:
            return None

    @property
    def is_import_file(self):
        return self.command == self.COMMAND_IMPORT_FILE

    @property
    def is_run(self):
        return self.command == self.COMMAND_RUN

    @property
    def argv(self):
        return self._argv

    @staticmethod
    def _replace_paths(l, old, new):
        if os.path != ntpath:
            return l

        ret = []
        for x in l:
            if x == None:
                ret.append(None)
                continue

            ret.append(x.replace(old, new))

        return ret

    @staticmethod
    def decode_paths(l):
        return StateFile._replace_paths(l, '/', '\\')

    @staticmethod
    def decode_path(p):
        return StateFile.decode_paths([p])[0]

    @staticmethod
    def encode_paths(l):
        return StateFile._replace_paths(l, '\\', '/')

    @staticmethod
    def encode_path(p):
        return StateFile.encode_paths([p])[0]

    @staticmethod
    def load_json(json, settings):
        return StateFile(StateFile.decode_path(json.get(StateFile.PARAM_COMMAND)),
                         None,
                         settings,
                         StateFile.decode_paths(json.get(StateFile.PARAM_INPUT_FILES, [])),
                         StateFile.decode_paths(json.get(StateFile.PARAM_OUTPUT_FILES, [])),
                         StateFile.decode_paths(json.get(StateFile.PARAM_CODE_DEPENDENCIES, [])),
                         json.get(StateFile.PARAM_LOCKED, False),
                         StateFile.decode_paths(json.get(StateFile.PARAM_ARGV)),
                         StateFile.decode_path(json.get(StateFile.PARAM_STDOUT)),
                         StateFile.decode_path(json.get(StateFile.PARAM_STDERR)),
                         StateFile.decode_path(json.get(StateFile.PARAM_CWD)),
                         json.get(StateFile.PARAM_SHELL, False),
                         json.get(StateFile.PARAM_TARGET_METRICS, {}),
                         json.get(StateFile.PARAM_MD5, ''))

    @staticmethod
    def load(data_item, settings):
        with open(data_item.state.relative, 'r') as fd:
            data = json.load(fd)
            return StateFile.load_json(data, settings)

    @staticmethod
    def loads(content, settings):
        data = json.loads(content)
        return StateFile.load_json(data, settings)

    def save(self, is_update_target_metrics=True):
        argv = self._argv_paths_normalization(self._argv)

        if is_update_target_metrics:
            self.update_target_metrics()

        res = {
            self.PARAM_COMMAND:             self.encode_path(self.command),
            self.PARAM_TYPE:                self.MAGIC,
            self.PARAM_VERSION:             self.VERSION,
            self.PARAM_ARGV:                self.encode_paths(argv),
            self.PARAM_CWD:                 self.encode_path(self.cwd),
            self.PARAM_INPUT_FILES:         self.encode_paths(self.input_files),
            self.PARAM_OUTPUT_FILES:        self.encode_paths(self.output_files),
            self.PARAM_CODE_DEPENDENCIES:   self.encode_paths(self.code_dependencies),
            self.PARAM_STDOUT:              self.encode_path(self.stdout),
            self.PARAM_STDERR:              self.encode_path(self.stderr),
            self.PARAM_SHELL:               self.shell,
            self.PARAM_TARGET_METRICS:      self.target_metrics,
            self.PARAM_MD5:                 self.md5
        }

        if self.locked:
            res[self.PARAM_LOCKED] = True

        file_dir = os.path.dirname(self.file)
        if file_dir != '' and not os.path.isdir(file_dir):
            os.makedirs(file_dir)

        with open(self.file, 'w') as fd:
            json.dump(res, fd, indent=2, sort_keys=True)
        pass

    def _argv_paths_normalization(self, argv):
        result = []

        for arg in argv:
            try:
                data_item = self.settings.path_factory.data_item(arg)
                result.append(data_item.data.dvc)
            except DataDirError:
                result.append(arg)

        return result

    def get_dvc_path(self):
        pwd = System.get_cwd()
        if not pwd.startswith(self.settings.git.git_dir_abs):
            raise StateFileError('the file cannot be created outside of a git repository')

        return os.path.relpath(pwd, self.settings.git.git_dir_abs)

class LocalStateFile(object):
    MAGIC = 'DVC-Local-State'
    VERSION = '0.1'

    PARAM_DATA_TIMESTAMP = 'DataTimestamp'
    PARAM_CACHE_TIMESTAMP = 'CacheTimestamp'

    def __init__(self, data_item, data_timestamp=None, cache_timestamp=None):
        self.data_item = data_item
        self.data_timestamp = data_timestamp
        self.cache_timestamp = cache_timestamp

        if not data_timestamp:
            self.data_timestamp = os.path.getmtime(self.data_item.data.relative)
        if not cache_timestamp:
            self.cache_timestamp = os.path.getmtime(self.data_item.cache.relative)

    @staticmethod
    def load_json(json):
        return LocalStateFile(None,
                              json.get(LocalStateFile.PARAM_DATA_TIMESTAMP, None),
                              json.get(LocalStateFile.PARAM_CACHE_TIMESTAMP, None))

    @staticmethod
    def load(data_item):
        with open(data_item.local_state.relative, 'r') as fd:
            data = json.load(fd)
            return LocalStateFile.load_json(data)

    def save(self):
        res = {
            self.PARAM_DATA_TIMESTAMP    : self.data_timestamp,
            self.PARAM_CACHE_TIMESTAMP   : self.cache_timestamp
        }

        file_dir = os.path.dirname(self.data_item.local_state.relative)
        if not os.path.isdir(file_dir):
            os.makedirs(file_dir)

        with open(self.data_item.local_state.relative, 'w') as fd:
            json.dump(res, fd, indent=2, sort_keys=True)
