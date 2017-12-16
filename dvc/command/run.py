import os

from dvc.command.common.base import CmdBase
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

        stage_file = self.get_stage_file()
        if os.path.isfile(stage_file):
            Logger.error("Stage file {} already exists".format(stage_file))
            return 1

        state = StateFile(fname=os.path.join(self.parsed_args.cwd, stage_file),
                          cmd=cmd,
                          out=self.parsed_args.out,
                          out_git=self.parsed_args.out_git,
                          deps=self.parsed_args.deps,
                          locked=self.parsed_args.lock,
                          cwd=self.parsed_args.cwd)

        self.run_command(self.settings, state)
        return self.commit_if_needed('DVC run: {}'.format(state.cmd))

    def get_stage_file(self):
        if self.parsed_args.file:
            return self.parsed_args.file

        if self.parsed_args.out or self.parsed_args.out_git:
            result = self.parsed_args.out[0] if self.parsed_args.out else self.parsed_args.out_git[0]
            result = os.path.basename(result+StateFile.STATE_FILE_SUFFIX)
            Logger.info(u'Using \'{}\' as a stage file'.format(result))
            return result

        Logger.info(u'Using \'{}\' as stage file'.format(StateFile.DVCFILE_NAME))
        return StateFile.DVCFILE_NAME

    @staticmethod
    def run_command(settings, state):
        Executor.exec_cmd_only_success(state.cmd, cwd=state.cwd, shell=True)

        CmdRun.move_output_to_cache(settings, state)
        CmdRun.update_state_file(settings, state)

    @staticmethod
    def update_state_file(settings, state):
        Logger.debug('Update state file "{}"'.format(state.path))
        state.out = StateFile.parse_deps_state(settings, state.out, currdir=state.cwd)
        state.out_git = StateFile.parse_deps_state(settings, state.out_git, currdir=state.cwd)
        state.deps = StateFile.parse_deps_state(settings, state.deps, currdir=state.cwd)
        state.save()

    @staticmethod
    def move_output_to_cache(settings, state):
        items = settings.path_factory.to_data_items(state.out)[0]
        for item in items:
            Logger.debug('Move output file "{}" to cache dir "{}" and create a hardlink'.format(
                         item.data.relative, item.cache_dir_abs))
            item.move_data_to_cache()
