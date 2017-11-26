import os
import sys
import yaml

from dvc.command.common.base import CmdBase
from dvc.exceptions import DvcException
from dvc.git_wrapper import GitWrapper
from dvc.logger import Logger
from dvc.repository_change import RepositoryChange
from dvc.state_file import StateFile
from dvc.utils import cached_property
from dvc.executor import Executor
from dvc.state_file import StateFileBase


class RunError(DvcException):
    def __init__(self, msg):
        DvcException.__init__(self, 'Run error: {}'.format(msg))


class CommandFile(StateFileBase):
    MAGIC = 'DVC-Command-State'
    VERSION = '0.1'

    PARAM_CMD = 'cmd'
    PARAM_OUT = 'out'
    PARAM_DEPS = 'deps'
    PARAM_LOCK = 'locked'

    def __init__(self, cmd, out, deps, locked, fname):
        self.cmd = cmd
        self.out = out
        self.deps = deps
        self.locked = locked
        self.fname = fname

    @property
    def dict(self):
        data = {
            self.PARAM_CMD: self.cmd,
            self.PARAM_OUT: self.out,
            self.PARAM_DEPS: self.deps,
            self.PARAM_LOCK: self.locked
        }

        return data

    def dumps(self):
        return yaml.dump(self.dict)

    def dump(self, fname):
        with open(fname, 'w+') as fd:
            fd.write(self.dumps())

    @staticmethod
    def loadd(data, fname=None):
        return CommandFile(data.get(CommandFile.PARAM_CMD, None),
                           data.get(CommandFile.PARAM_OUT, None),
                           data.get(CommandFile.PARAM_DEPS, None),
                           data.get(CommandFile.PARAM_LOCK, None),
                           fname)

    @staticmethod
    def load(fname):
        return CommandFile._load(fname, CommandFile, fname)


class CmdRun(CmdBase):
    def __init__(self, settings):
        super(CmdRun, self).__init__(settings)

    def run(self):
        cmd = ' '.join(self.parsed_args.command)
        try:
            command = CommandFile.load(cmd)
        except Exception as exc:
            Logger.debug("Failed to load {}: {}".format(cmd, str(exc)))
            command = CommandFile(cmd, self.parsed_args.out, self.parsed_args.deps, self.parsed_args.lock, None)

        self.run_command(self.settings, command)
        return self.commit_if_needed('DVC run: {}'.format(command.cmd))

    @staticmethod
    def run_command(settings, command):
        Executor.exec_cmd_only_success(command.cmd, shell=True)

        result = []
        items = settings.path_factory.to_data_items(command.out)[0]
        for data_item in items:
            Logger.debug('Move output file "{}" to cache dir "{}" and create a hardlink'.format(
                data_item.data.relative, data_item.cache_dir_abs))
            data_item.move_data_to_cache()

            Logger.debug('Create state file "{}"'.format(data_item.state.relative))

            state_file = StateFile(data_item,
                                   settings,
                                   command.fname if command.fname else command.dict,
                                   StateFile.parse_deps_state(settings, command.deps))
            state_file.save()
            result.append(state_file)

        return result
