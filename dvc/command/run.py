from dvc.command.common.base import CmdBase
from dvc.command.common.command_file import CommandFile
from dvc.exceptions import DvcException
from dvc.logger import Logger
from dvc.state_file import StateFile
from dvc.executor import Executor


class RunError(DvcException):
    def __init__(self, msg):
        DvcException.__init__(self, 'Run error: {}'.format(msg))


class CmdRun(CmdBase):
    def __init__(self, settings):
        super(CmdRun, self).__init__(settings)

    def run(self):
        cmd = ' '.join(self.parsed_args.command)
        try:
            command = CommandFile.load(cmd)
        except Exception as exc:
            Logger.debug("Failed to load {}: {}".format(cmd, str(exc)))
            command = CommandFile(cmd, self.parsed_args.out, self.parsed_args.out_git,
                                  self.parsed_args.deps, self.parsed_args.lock, None)

        self.run_command(self.settings, command)
        return self.commit_if_needed('DVC run: {}'.format(command.cmd))

    @staticmethod
    def run_command(settings, command):
        Executor.exec_cmd_only_success(command.cmd, shell=True)

        CmdRun.apply_to_files(command.out, command, CmdRun._create_cache_and_state_files, settings)
        CmdRun.apply_to_files(command.out_git, command, CmdRun._create_state_file, settings)

    @staticmethod
    def apply_to_files(files, command, func, settings):
        [func(i, command, settings) for i in settings.path_factory.to_data_items(files)[0]]

    @staticmethod
    def _create_cache_and_state_files(data_item, command, settings):
        Logger.debug('Move output file "{}" to cache dir "{}" and create a hardlink'.format(
                     data_item.data.relative, data_item.cache_dir_abs))
        data_item.move_data_to_cache()
        return CmdRun._create_state_file(data_item, command, settings)

    @staticmethod
    def _create_state_file(data_item, command, settings):
        Logger.debug('Create state file "{}"'.format(data_item.state.relative))
        state_file = StateFile(data_item,
                               settings,
                               command.fname if command.fname else command.dict,
                               StateFile.parse_deps_state(settings, command.deps))
        state_file.save()
        return state_file
