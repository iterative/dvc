import os

from dvc.command.common.base import CmdBase
from dvc.data_cloud import file_md5
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

        if os.path.isfile(self.parsed_args.file):
            Logger.error("Stage file {} already exists".format(self.parsed_args.file))
            return 1

        state = StateFile(fname=self.parsed_args.file,
                          cmd=cmd,
                          out=self.parsed_args.out,
                          out_git=self.parsed_args.out_git,
                          deps=self.parsed_args.deps,
                          locked=self.parsed_args.lock)

        self.run_command(self.settings, state)
        return self.commit_if_needed('DVC run: {}'.format(state.cmd))

    @staticmethod
    def run_command(settings, state):
        Executor.exec_cmd_only_success(state.cmd, cwd=state.cwd, shell=True)

        CmdRun.move_output_to_cache(settings, state)
        CmdRun.update_state_file(settings, state)

    @staticmethod
    def update_state_file(settings, state):
        Logger.debug('Update state file "{}"'.format(state.path))
        state.out = StateFile.parse_deps_state(settings, state.out)
        state.out_git = StateFile.parse_deps_state(settings, state.out_git)
        state.deps = StateFile.parse_deps_state(settings, state.deps)
        state.save()

    @staticmethod
    def move_output_to_cache(settings, state):
        items = settings.path_factory.to_data_items(state.out)[0]
        for item in items:
            Logger.debug('Move output file "{}" to cache dir "{}" and create a hardlink'.format(
                         item.data.relative, item.cache_dir_abs))
            item.move_data_to_cache()
