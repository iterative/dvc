import os
import ntpath
import sys
import json
import yaml
import re

from dvc import utils
from dvc.exceptions import DvcException
from dvc.system import System
from dvc.data_cloud import file_md5

class StateFileError(DvcException):
    def __init__(self, msg):
        DvcException.__init__(self, 'State file error: {}'.format(msg))


class StateFile(object):
    MAGIC = 'DVC-State'
    VERSION = '0.1'

    DVC_PYTHON_FILE_NAME = 'dvc.py'
    DVC_COMMAND = 'dvc'

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

    PARAM_COMMAND = 'command'
    PARAM_DEPS = 'deps'
    PARAM_PATH = 'path'
    PARAM_MD5 = 'md5'

    def __init__(self,
                 data_item,
                 settings,
                 command,
                 deps,
                 target_metrics={}):
        self.data_item = data_item

        self.settings = settings
        self.command = command
        self.deps = deps

        self.target_metrics = target_metrics

    @staticmethod
    def parse_deps_state(settings, deps):
        state = []
        for dep in deps:
            if settings.path_factory.is_data_item(dep):
                item = settings.path_factory.data_item(dep)
                state.append({StateFile.PARAM_PATH: item.data.dvc,
                              StateFile.PARAM_MD5: CacheStateFile.load(item).md5})
            else:
                state.append({StateFile.PARAM_PATH: dep,
                              StateFile.PARAM_MD5: file_md5(os.path.join(settings.git.git_dir_abs, dep))[0]})
        return state

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

    @staticmethod
    def load_json(json, settings):
        return StateFile(None,
                         settings,
                         json.get(StateFile.PARAM_COMMAND, None),
                         json.get(StateFile.PARAM_DEPS, []),
                         json.get(StateFile.PARAM_TARGET_METRICS, {}))

    @staticmethod
    def load(data_item, settings):
        with open(data_item.state.relative, 'r') as fd:
            data = yaml.load(fd)
            return StateFile.load_json(data, settings)

    @staticmethod
    def loads(content, settings):
        data = yaml.loads(content)
        return StateFile.load_json(data, settings)

    def save(self, is_update_target_metrics=True):
        if is_update_target_metrics:
            self.update_target_metrics()

        res = {
            self.PARAM_COMMAND:             self.command,
            self.PARAM_DEPS:                self.deps,
            self.PARAM_TARGET_METRICS:      self.target_metrics
        }

        file_dir = os.path.dirname(self.file)
        if file_dir != '' and not os.path.isdir(file_dir):
            os.makedirs(file_dir)

        with open(self.file, 'w') as fd:
            yaml.dump(res, fd, default_flow_style=False)
        pass


class CacheStateFile(object):
    MAGIC = 'DVC-Cache-State'
    VERSION = '0.1'

    PARAM_MD5 = StateFile.PARAM_MD5

    def __init__(self, data_item, md5=None):
        self.data_item = data_item
        self.md5 = md5

        if not md5:
            self.md5 = file_md5(data_item.data.relative)[0]

    @staticmethod
    def load_json(json):
        return CacheStateFile(None,
                              json.get(CacheStateFile.PARAM_MD5, None))

    @staticmethod
    def load(data_item):
        with open(data_item.cache_state.relative, 'r') as fd:
            data = json.load(fd)
            return CacheStateFile.load_json(data)

    def save(self):
        res = {
            self.PARAM_MD5 : self.md5,
        }

        file_dir = os.path.dirname(self.data_item.cache_state.relative)
        if not os.path.isdir(file_dir):
            os.makedirs(file_dir)

        with open(self.data_item.cache_state.relative, 'w') as fd:
            json.dump(res, fd, indent=2, sort_keys=True)


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
